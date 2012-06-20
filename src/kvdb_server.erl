-module(kvdb_server).
-behaviour(gen_server).

-export([start_link/2,
	 start_session/2,
	 db/1,
	 close/1,
	 call/2,
	 cast/2]).

-export([begin_trans/1,
	 end_trans/2]).

-export([init/1,
	 handle_call/3,
	 handle_cast/2,
	 handle_info/2,
	 terminate/2,
	 code_change/3]).

-include("kvdb.hrl").
-include_lib("lager/include/log.hrl").
-import(kvdb_lib, [table_name/1]).

-record(st, {name, db, is_owner = false,
	     switch_pending = false,
	     transactions = [],
	     pending_transactions = []}).

start_link(Name, Backend) ->
    io:fwrite("starting ~p, ~p~n", [Name, Backend]),
    gen_server:start_link(?MODULE, {owner, Name, Backend}, []).

start_session(Name, Id) ->
    gen_server:start_link(?MODULE, session(Name, Id), []).

session(Name, Id) ->
    {Name, session, Id}.


db(Name) ->
    call(Name, db).

begin_trans(Name) ->
    call(Name, begin_trans).

end_trans(Name, Ref) ->
    cast(Name, {end_trans, self(), Ref}).

close(Name) ->
    call(Name, close).

cast(Name, Msg) ->
    gen_server:cast(gproc:where({n,l,{kvdb,Name}}), Msg).

call(Name, Req) ->
    Pid = case Name of
	      #kvdb_ref{name = N} ->
		  gproc:where({n, l, {kvdb, N}});
	      P when is_pid(P) ->
		  P;
	      _ ->
		  gproc:where({n,l,{kvdb,Name}})
	  end,
    case gen_server:call(Pid, Req) of
	badarg ->
	    ?KVDB_THROW(badarg);
	{badarg,_} = Err ->
	    ?KVDB_THROW(Err);
	Res ->
	    Res
    end.

%% @private
init(Alias) ->
    try init_(Alias)
    catch
	error:Reason ->
	    Trace = erlang:get_stacktrace(),
	    error_logger:error_report([{error_opening_kvdb_db, Alias},
				       {error, Reason},
				       {stacktrace, Trace}]),
	    error({Reason, Trace}, [Alias])
    end.

init_({Name, session, _Id} = Alias) ->
    Db = db(Name),
    gproc:reg({p, l, {kvdb, session}}, Alias),
    gproc:reg({n, l, {kvdb, Alias}}),
    {ok, #st{db = Db}};
init_({owner, Name, Opts}) ->
    Backend = proplists:get_value(backend, Opts, ets),
    gproc:reg({n, l, {kvdb,Name}}, Backend),
    DbMod = mod(Backend),
    F = name2file(Name),
    File = case proplists:get_value(file, Opts) of
	       undefined ->
		   {ok, CWD} = file:get_cwd(),
		   filename:join(CWD, F);
	       F1 ->
		   F1
	   end,
    ok = filelib:ensure_dir(File),
    NewOpts = lists:keystore(backend, 1,
			     lists:keystore(file, 1, Opts, {file, File}),
			     {backend, DbMod}),
    case do_open(Name, NewOpts) of
	{ok, Db} ->
	    create_tables_(Db, Opts),
	    {ok, #st{name = Name, db = Db, is_owner = true}};
	{error,_} = Error ->
	    io:fwrite("error opening kvdb database ~w:~n"
		      "Error: ~p~n"
		      "Opts = ~p~n", [Name, Error, NewOpts]),
	    Error
    end.

%% @private
handle_call(Req, From, St) ->
    try handle_call_(Req, From, St)
    catch
	error:badarg ->
	    {reply, {badarg, erlang:get_stacktrace()}, St};
	error:E ->
	    {reply, {badarg,[E, erlang:get_stacktrace()]}, St}
    end.

handle_call_({put, Tab, Obj}, _From, #st{db = Db} = St) ->
    {reply, kvdb_direct:put(Db, Tab, Obj), St};
handle_call_({update_counter, Table, Key, Incr}, _From, #st{db = Db} = St) ->
    {reply, kvdb_direct:update_counter(Db, Table, Key, Incr), St};
handle_call_({push, Tab, Q, Obj}, _From, #st{db = Db} = St) ->
    {reply, kvdb_direct:push(Db, Tab, Q, Obj), St};
handle_call_({pop, Tab, Q}, _From, #st{db = Db} = St) ->
    {reply, kvdb_direct:pop(Db, Tab, Q), St};
handle_call_({prel_pop, Tab, Q}, _From, #st{db = Db} = St) ->
    {reply, kvdb_direct:prel_pop(Db, Tab, Q), St};
handle_call_({extract, Tab, Key}, _From, #st{db = Db} = St) ->
    {reply, kvdb_direct:extract(Db, Tab, Key), St};
handle_call_({mark_queue_object, Table, Key, OSt}, _From, #st{db = Db} = St) ->
    {reply, kvdb_direct:mark_queue_object(Db, Table, Key, OSt), St};
handle_call_({delete, Tab, Key}, _From, #st{db = Db} = St) ->
    {reply, kvdb_direct:delete(Db, Tab, Key), St};
handle_call_({add_table, Table, Opts}, _From, #st{db = Db} = St) ->
    {reply, kvdb_direct:add_table(Db, Table, Opts), St};
handle_call_({delete_table, Table}, _From, #st{db = Db} = St) ->
    {reply, kvdb_direct:delete_table(Db, Table), St};
handle_call_(close, _From, #st{is_owner = true} = St) ->
    {stop, normal, ok, St};
handle_call_(begin_trans, {Pid,_}, #st{db = Db,
				       switch_pending = false,
				       transactions = Trans} = St) ->
    Ref = erlang:monitor(process, Pid),
    {reply, {Db, Ref}, St#st{transactions = [Ref | Trans]}};
handle_call_(begin_trans, From, #st{switch_pending = true,
				    pending_transactions = Pend} = St) ->
    {noreply, St#st{pending_transactions = [From|Pend]}};
handle_call_(db, _From, #st{db = Db} = St) ->
    {reply, Db, St}.

%% @private
handle_info({'DOWN',_,_,_,{?MODULE, new_log, Log}},
	    #st{db = Db, transactions = Trans} = St) ->
    %% Need to switch logs. We try to do this atomically. For individual
    %% updates, it's no problem, since they will call on us for the db ref.
    %% If there are ongoing transactions, we must wait for them to end,
    %% queueing new transaction requests in the meantime.
    case Trans of
	[] ->
	    NewDb = switch_logs(Db, Log),
	    {noreply, logs_switched(NewDb, St)};
	[_|_] ->
	    {noreply, St#st{switch_pending = {true, Log}}}
    end;
handle_info({'DOWN',Ref,_,_,_}, #st{db = OldDb, transactions = Ts} = St) ->
    Ts1 = lists:delete(Ref, Ts),
    case {St#st.switch_pending, Ts1} of
	{false, _} ->
	    {noreply, St#st{transactions = Ts1}};
	{{true,Log}, []} ->
	    NewDb = kvdb_direct:switch_logs(OldDb, Log),
	    {noreply, logs_switched(NewDb, St#st{transactions = Ts1})}
    end;
handle_info(_, St) ->
    {noreply, St}.

%% @private
handle_cast(log_threshold, #st{db = Db} = St) ->
    Me = self(),
    spawn_monitor(fun() ->
			  {ok, Log} = open_new_log(Db, Me),
			  exit({?MODULE, new_log, Log})
		  end),
    {noreply, St};
handle_cast(_, St) ->
    {noreply, St}.

%% @private
terminate(_Reason, #st{db = #kvdb_ref{mod = M, db = Db}}) ->
    M:close(Db),
    ok.

%% @private
code_change(_FromVsn, St, _Extra) ->
    {ok, St}.


switch_logs(#kvdb_ref{mod = M, db = Db} = Ref, Log) ->
    #db{} = NewDb = M:switch_logs(Db, Log),
    Ref#kvdb_ref{db = NewDb}.

logs_switched(NewDb, St) ->
    case St#st.pending_transactions of
	[] ->
	    St#st{db = NewDb};
	Pend ->
	    Ts = lists:foreach(
		   fun({Pid,_} = From) ->
			   Ref = erlang:monitor(process, Pid),
			   gen_server:reply(From, {NewDb, Ref}),
			   Ref
		   end, lists:reverse(Pend)),
	    St#st{
			db = NewDb,
			pending_transactions = [],
			transactions = Ts,
			switch_pending = false}
    end.


open_new_log(#kvdb_ref{name = Name, db = Db}, Pid) ->
    LogDir = kvdb_meta:read(Db, log_dir, undefined),
    kvdb_lib:open_log(Name, kvdb_lib:log_filename(LogDir), Pid).


do_open(Name, Options) when is_list(Options) ->
    DbMod = proplists:get_value(backend, Options, kvdb_sqlite3),
    case DbMod:open(Name,Options) of
	{ok, Db} ->
	    io:fwrite("opened ~p database: ~p~n", [DbMod, Options]),
	    Default = DbMod:get_schema_mod(Db, kvdb_schema),
	    Schema = proplists:get_value(schema, Options, Default),
	    {ok, #kvdb_ref{name = Name, mod = DbMod, db = Db, schema = Schema}};
	Error ->
	    io:fwrite("ERROR opening ~p database: ~p. Opts = ~p~n",
		      [DbMod, Error, Options]),
	    Error
    end.



mod(mnesia ) -> kvdb_mnesia;
mod(leveldb) -> kvdb_leveldb;
mod(sqlite3) -> kvdb_sqlite3;
mod(sqlite ) -> kvdb_sqlite3;
mod(ets    ) -> kvdb_ets;
mod(M) ->
    case is_behaviour(M) of
	true ->
	    M;
	false ->
	    error(illegal_backend_type)
    end.

name2file(X) ->
    kvdb_lib:good_string(X).



%% to_atom(A) when is_atom(A) ->
%%     A;
%% to_atom(S) when is_list(S) ->
%%     list_to_atom(S).


is_behaviour(_M) ->
    %% TODO: check that exported functions match those listed in
    %% behaviour_info(callbacks).
    true.

create_tables_(Db, Opts) ->
    case proplists:get_value(tables, Opts, []) of
	[] ->
	    ok;
	Ts ->
	    Tabs0 = lists:map(fun({T,Os}) ->
				      {table_name(T), Os};
				 (T) -> {table_name(T),[]}
			      end, Ts),
	    %% We don't warn if there are more tables than we've specified,
	    %% and we certainly don't remove them. Ok to do nothing?
	    Tables = internal_tables() ++ Tabs0,
	    Existing = kvdb:list_tables(Db),
	    New = lists:filter(fun({T,_}) ->
				       not lists:member(T, Existing) end,
			       Tables),
	    [kvdb_direct:add_table(Db, T, Os) || {T, Os} <- New]
    end.

internal_tables() ->
    [].
