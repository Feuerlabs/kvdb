%%%---- BEGIN COPYRIGHT -------------------------------------------------------
%%%
%%% Copyright (C) 2012 Feuerlabs, Inc. All rights reserved.
%%%
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at http://mozilla.org/MPL/2.0/.
%%%
%%%---- END COPYRIGHT ---------------------------------------------------------
%%% @author Ulf Wiger <ulf@feuerlabs.com>
%%% @hidden
%%% @doc
%%%   KVDB Database instance owner process
%%% @end
%%%
-module(kvdb_server).
-behaviour(gen_server).

-export([start_link/2,
	 start_session/2,
	 db/1, db/2,
	 await/1, await/2,
	 close/1,
	 call/2,
	 cast/2]).

-export([begin_trans/3,
	 start_commit/2,
	 end_trans/2]).

-export([init/1,
	 handle_call/3,
	 handle_cast/2,
	 handle_info/2,
	 terminate/2,
	 code_change/3]).

-include("kvdb.hrl").
-include("log.hrl").
-import(kvdb_lib, [table_name/1]).

-record(st, {name, db, is_owner = false,
	     switch_pending = false,
	     transactions = [],
	     commits = [],
	     pending_commits = []}).

-record(trans, {pid, mref, tref, role = master, db}).

start_link(Name, Backend) ->
    gen_server:start_link(?MODULE, {owner, Name, Backend}, []).

start_session(Name, Id) ->
    gen_server:start_link(?MODULE, session(Name, Id), []).

session(Name, Id) ->
    {Name, session, Id}.


db(Name) ->
    call(Name, db).

db(Name, TRef) ->
    call(Name, {db, TRef}).

begin_trans(Name, TRef, Db) ->
    call(Name, {begin_trans, TRef, Db}).

start_commit(Name, TRef) ->
    try call(Name, {start_commit, TRef}) of
	#kvdb_ref{} = Res ->
	    Res;
	Other ->
	    erlang:error(Other)
    catch
	exit:Err ->
	    io:fwrite(user, "kvdb_server status:~n~s~n",
		      [print_state(Name)]),
	    exit(Err)
    end.

print_state(Name) ->
    {status,_,_,[_,_,_,_,[_,_,{data,[{"State",S}]}]]} =
	sys:get_status(get_pid(Name)),
    Sz = tuple_size(S) -1,
    RF = fun(st, Size) when Size == Sz ->
		 record_info(fields, st);
	    (_, _) ->
		 no
	 end,
    io_lib_pretty:print(S, RF).

end_trans(Name, Ref) ->
    cast(Name, {end_trans, self(), Ref}).

close(Name) ->
    call(Name, close).

await(Name) ->
    await(Name, timer:minutes(1)).

await(Name, Timeout) ->
    gproc:await({n,l,{kvdb,Name}}, Timeout),
    ok.

cast(Name, Msg) ->
    gen_server:cast(gproc:where({n,l,{kvdb,Name}}), Msg).

call(Name, Req) ->
    call(Name, Req, 5000).

call(Name, Req, Timeout) ->
    Pid = get_pid(Name),
    case gen_server:call(Pid, Req, Timeout) of
	badarg ->
	    ?KVDB_THROW(badarg);
	{badarg,_} = Err ->
	    ?KVDB_THROW(Err);
	Res ->
	    Res
    end.

get_pid(Name) ->
    case Name of
	#kvdb_ref{name = N} ->
	    gproc:where({n, l, {kvdb, N}});
	P when is_pid(P) ->
	    P;
	_ ->
	    gproc:where({n,l,{kvdb,Name}})
    end.


%% @private
init(Alias) ->
    process_flag(trap_exit, true),
    try init_(Alias)
    catch
	error:Reason ->
	    Trace = erlang:get_stacktrace(),
	    error_logger:error_report([{error_opening_kvdb_db, Alias},
				       {error, Reason},
				       {stacktrace, Trace}]),
	    erlang:error({Reason, Trace}, [Alias])
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
    NewOpts = check_log_options(
		Name,
		lists:keystore(backend, 1,
			       %% lists:keystore(file, 1, Opts, {file, File}),
			       Opts,
			       {backend, DbMod})),
    case do_open(Name, NewOpts) of
	{ok, Db} ->
	    kvdb_cron:init_meta(Db),
	    create_tables_(Db, Opts),
	    case common_open(Db, NewOpts) of
		{ok, Db1} ->
		    {ok, #st{name = Name, db = Db1, is_owner = true}};
		{error,_} = CommonOpenErr ->
		    CommonOpenErr
	    end;
	{error,_} = Error ->
	    io:fwrite("error opening kvdb database ~w:~n"
		      "Error: ~p~n"
		      "Opts = ~p~n", [Name, Error, NewOpts]),
	    Error
    end.

check_log_options(Name, Opts) ->
    case {lists:keymember(log_dir, 1, Opts),
	  lists:keymember(log_threshold, 1, Opts)} of
	{false, true} ->
	    {File, Opts1} = ensure_file(Name, Opts),
	    [{log_dir, File ++ ".log"} | Opts1];
	_ ->
	    Opts
    end.

ensure_file(Name, Opts) ->
    case lists:keyfind(file, 1, Opts) of
	{_, F} ->
	    {F, Opts};
	false ->
	    F = kvdb_lib:db_file(Name),
	    {F, [{file, F}|Opts]}
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
handle_call_({begin_trans, Ref, DbT}, {Pid,_}, #st{transactions = Ts} = St) ->
    lager:debug("begin_trans, Ref = ~p, Pid = ~p", [Ref, Pid]),
    MRef = erlang:monitor(process, Pid),
    Ts1 = [#trans{pid = Pid, tref = Ref, mref = MRef, db = DbT}|Ts],
    {reply, ok, St#st{transactions = Ts1}};
handle_call_({start_commit, Ref}, {Pid,_} = From, #st{switch_pending = SwPend,
						      transactions = Ts,
						      db = Db} = St) ->
    case is_transaction_master(Pid, Ref, Ts) of
	yes ->
	    case SwPend of false ->
		    {reply, Db, St#st{commits = [Ref|St#st.commits]}};
		{true,_,_} ->
		    Pend = St#st.pending_commits,
		    {noreply, St#st{pending_commits = [From|Pend]}}
	    end;
	{no, Reason} ->
	    {reply, {error, Reason}, St}
    end;
handle_call_({new_log, Log}, From, #st{db = Db, commits = Commits} = St) ->
    %% Need to switch logs. We try to do this atomically. For individual
    %% updates, it's no problem, since they will call on us for the db ref.
    %% If there are ongoing transactions, we must wait for them to end,
    %% queueing new transaction requests in the meantime.
    case Commits of
	[] ->
	    NewDb = switch_logs(Db, Log),
	    {reply, {ok, NewDb}, logs_switched(NewDb, St)};
	[_|_] ->
	    %% io:fwrite("won't switch now; Ts = ~p~n", [Ts]),
	    {noreply, St#st{switch_pending = {true, From, Log}}}
    end;
handle_call_(db, _From, #st{db = Db} = St) ->
    {reply, Db, St}.

handle_info({'DOWN', Ref, _,_,_}, #st{transactions = Ts,
				      commits = Commits,
				      switch_pending = Pend} = St) ->
    case lists:keytake(Ref, #trans.mref, Ts) of
	{value, #trans{tref = TRef, role = master}, Ts1} ->
	    case Commits -- [TRef] of
		[] ->
		    case Pend of
			{true, _From, Log} ->
			    NewDb = switch_logs(St#st.db, Log),
			    {noreply, logs_switched(
					NewDb, St#st{transactions = Ts1,
						     commits = []})};
			false ->
			    {noreply, St#st{transactions = Ts1,
					    commits = []}}
		    end;
		Commits1 ->
		    {noreply, St#st{transactions = Ts1,
				    commits = Commits1}}
	    end;
	{value, _, Ts1} ->
	    {noreply, St#st{transactions = Ts1}};
	false ->
	    {noreply, St}
    end;
handle_info(_, St) ->
    {noreply, St}.

%% @private
handle_cast({end_trans, _Pid, Ref} = _M, #st{db = Db,
					     commits = Commits,
					     transactions = Ts} = St) ->
    lager:debug("~p", [_M]),
    Ts1 = lists:foldr(
	    fun(#trans{tref = R, mref = MRef}, Acc) when R =:= Ref ->
		    erlang:demonitor(MRef),
		    Acc;
	       (T, Acc) ->
		    [T|Acc]
	    end, [], Ts),
    case Commits1 = Commits -- [Ref] of
	[] ->
	    case St#st.switch_pending of
		false ->
		    {noreply, St#st{transactions = Ts1, commits = []}};
		{true, From, Log} ->
		    NewDb = switch_logs(Db, Log),
		    gen_server:reply(From, {ok, NewDb}),
		    {noreply, logs_switched(NewDb, St#st{transactions = Ts1,
							 commits = []})}
	    end;
	[_|_] = Commits1 ->
	    {noreply, St#st{transactions = Ts1,
			    commits = Commits1}}
    end;
handle_cast(log_threshold, #st{db = Db} = St) ->
    %% io:fwrite("threshold reached, ~p~n", [Db]),
    Me = self(),
    spawn_monitor(fun() ->
			  %% io:fwrite("~p spawned to switch logs~n", [self()]),
			  {ok, Log} = open_new_log(Db, Me),
			  %% io:fwrite("new log opened: ~p~n", [Log]),
			  TS = os:timestamp(),
			  {ok, NewDb} = call(Me, {new_log, Log}, 15000),
			  %% io:fwrite("NewDb = ~p~n", [NewDb]),
			  #kvdb_ref{db = D} = NewDb,
			  case kvdb_meta:read(D, last_dump, undefined) of
			      DumpTS when DumpTS > TS ->
				  kvdb_lib:purge_logs(D, DumpTS);
				  %% io:fwrite("logs purged~n", []);
			      _ ->
				  %% io:fwrite("nothing to purge~n", []),
				  ignore
			  end
		  end),
    {noreply, St};
handle_cast(_, St) ->
    {noreply, St}.

%% @private
terminate(_Reason, #st{db = #kvdb_ref{mod = M, db = Db}} = S) ->
    M:close(Db),
    close_log(S),
    ok.

%% @private
code_change(_FromVsn, St, _Extra) ->
    {ok, St}.

is_transaction_master(Pid, Ref, Ts) ->
    case [T || #trans{pid = P, tref = R} = T <- Ts,
	       P =:= Pid, R =:= Ref] of
	[#trans{role = master}] ->
	    yes;
	[_] ->
	    {no, wrong_pid};
	[] ->
	    {no, not_found}
    end.

switch_logs(#kvdb_ref{mod = M, db = #db{log = {OldLog,_}} = Db} = Ref, Log) ->
    #db{} = NewDb = M:switch_logs(Db, Log),
    disk_log:close(OldLog),
    Ref#kvdb_ref{db = NewDb}.

close_log(#st{db = #db{log = {Log, _}}}) ->
    disk_log:close(Log);
close_log(_) ->
    ok.

logs_switched(NewDb, St) ->
    St1 = case St#st.pending_commits of
	      [] ->
		  St#st{db = NewDb,
			switch_pending = false};
	      Pend ->
		  lists:foreach(fun(From) ->
					gen_server:reply(From, NewDb)
				end, Pend),
		  St#st{
		    db = NewDb,
		    pending_commits = [],
		    switch_pending = false}
	  end,
    %% io:fwrite("logs switched; new st= ~p~n", [St1]),
    St1.


open_new_log(#kvdb_ref{db = Db}, Pid) ->
    LogDir = kvdb_meta:read(Db, log_dir, undefined),
    kvdb_lib:open_log(kvdb_lib:log_filename(LogDir), Pid).


do_open(Name, Options) when is_list(Options) ->
    DbMod = proplists:get_value(backend, Options, kvdb_sqlite3),
    Fallback = check_for_fallback(Name, Options),
    case DbMod:open(Name,Options) of
	{ok, Db} ->
	    %% io:fwrite("opened ~p database: ~p~n", [DbMod, Options]),
	    Default = DbMod:get_schema_mod(Db, kvdb_schema),
	    Schema = proplists:get_value(schema, Options, Default),
	    kvdb_meta:write(Db, name, Name),
	    maybe_import(Fallback, DbMod, Db),
	    {ok, #kvdb_ref{name = Name, mod = DbMod, db = Db, schema = Schema}};
	Error ->
	    io:fwrite("ERROR opening ~p database: ~p. Opts = ~p~n",
		      [DbMod, Error, Options]),
	    Error
    end.

maybe_import(false, _, _) ->
    ok;
maybe_import(Fallback, Mod, Db) ->
    io:fwrite("Importing from ~p~n", [Fallback]),
    kvdb_export:import(Mod, Db, Fallback).


check_for_fallback(Name, Options) ->
    File = case lists:keyfind(file, 1, Options) of
	       {_, F} -> F;
	       false -> kvdb_lib:db_file(Name)
	   end,
    io:fwrite("File = ~p~n", [File]),
    Dir = filename:dirname(File),
    lager:debug("check_for_fallback: Dir = ~p~n", [Dir]),
    case filelib:wildcard(to_list(Name) ++ ".KBUP?", Dir) of
	[] ->
	    false;
	[_|_] = Files ->
	    Sorted = lists:sort(fun fcomp/2, Files),
	    lager:debug("Found backups: ~p~n", [Sorted]),
	    Pick = hd(Sorted),
	    move_file(Options),
	    filename:join(Dir, Pick)
    end.

to_list(A) when is_atom(A) -> atom_to_list(A);
to_list(L) when is_list(L) -> L;
to_list(B) when is_binary(B) -> binary_to_list(B).


move_file(Options) ->
    case lists:keyfind(file, 1, Options) of
	{_, F} ->
	    BakName = bak_name(F),
	    file:rename(F, BakName);
	false ->
	    ok
    end.

bak_name(F) ->
    bak_name(F, 0).

bak_name(F, N) ->
    case file:read_link_info(Bak = F ++ bak_ext(N)) of
	{ok,_}         -> bak_name(F, N+1);
	{error,enoent} -> Bak
    end.

bak_ext(0) -> ".bak";
bak_ext(N) -> ".bak." ++ integer_to_list(N).


fcomp(A, B) ->
    case {filename:extension(A), filename:extension(B)} of
	{".KBUPB", _} -> true;
	{_, ".KBUPB"} -> false;
	{_, _} -> true % just pick one
    end.

common_open(#kvdb_ref{mod = DbMod, db = Db} = DbRef, Options) ->
    case kvdb_lib:common_open(DbMod, Db, Options) of
	{ok, Db1} ->
	    {ok, DbRef#kvdb_ref{db = Db1}};
	Other ->
	    Other
    end.


mod(mnesia ) -> kvdb_mnesia;
mod(leveldb) -> kvdb_leveldb;
mod(sqlite3) -> kvdb_sqlite3;
mod(sqlite ) -> kvdb_sqlite3;
mod(ets    ) -> kvdb_ets;
mod(M) ->
    case is_behaviour(M) of
	true ->
	    M   % TODO: implement actual check
	%% false ->
	%%     error(illegal_backend_type)
    end.

%% name2file(X) ->
%%     kvdb_lib:good_string(X).



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
