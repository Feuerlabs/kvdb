%%% @author Tony Rogvall <tony@rogvall.se>
%%% @copyright (C) 2011, Tony Rogvall
%%% @doc
%%%    key value database frontend
%%% @end
%%% Created : 29 Dec 2011 by Tony Rogvall <tony@rogvall.se>

-module(kvdb).

-behaviour(gen_server).

-export([test/0]).

-export([start/0, open_db/2, info/2]).
-export([open/2, close/1, db/1]).
-export([add_table/2, delete_table/2, list_tables/1]).
-export([put/3, get/3, delete/3]).
-export([first/2, last/2, next/3, prev/3]).
-export([prefix_match/3, prefix_match/4]).
-export([select/3, select/4]).

%% direct API towards an active kvdb instance
-export([do_put/3,
	 do_get/3,
	 do_delete/3,
	 do_add_table/2,
	 do_delete_table/2,
	 do_first/2,
	 do_next/3,
	 do_prev/3,
	 do_last/2,
	 do_prefix_match/4,
	 do_select/4,
	 do_info/2]).

-export([behaviour_info/1]).

-export([start_link/2,
	 init/1,
	 handle_call/3,
	 handle_cast/2,
	 handle_info/2,
	 terminate/2,
	 code_change/3]).

-record(st, {name, db}).
-record(kvdb_ref, {db, mod}).

-define(KVDB_CATCH(Expr, Args),
	try Expr
	catch
	    throw:{kvdb_throw, __E} ->
		error(__E, Args)
	end).

-define(KVDB_THROW(E), throw({kvdb_throw, E})).

test() ->
    dbg:tracer(),
    [dbg:tpl(M, x) || M <- [kvdb, kvdb_sup, kvdb_sqlite3]],
    [dbg:tp(M, x) || M <- [gproc]],
    dbg:p(all, [c]),
    application:start(gproc),
    application:start(kvdb).

%% The plugin behaviour
behaviour_info(callbacks) ->
    [
     {open,2},
     {close,1},
     {add_table,2},
     {delete_table,2},
     {put,3},
     {get,3},
     {delete,3},
     {iterator,2},
     {iterator_close,2},
     {first,2},
     {last,2},
     {next,3},
     {prev,3}
    ];
behaviour_info(_Other) ->
    undefined.

-opaque db_ref()  :: #kvdb_ref{}.

-type key() :: any().
-type value() :: any().
-type attrs() :: [{atom(), any()}].
-type options() :: [{atom(), any()}].

-type object() :: {key(), value()} | {key(), attrs(), value()}.

start() ->
    application:start(gproc),
    application:start(kvdb).

-spec open_db(Name::any(), Options::options()) -> {ok, pid()} | {error, any()}.

%% @spec open_db(Name, Options) -> {ok, Pid} | {error, Reason}
%% @doc Opens a kvdb database instance.
%%
%% TODO: make sure that the database instance is able to remember relevant options and
%% verify that given options are compatible.
%% @end
%%
open_db(Name, Options) ->
    case gproc:where({n,l,{kvdb, Name}}) of
	undefined ->
	    Child = kvdb_sup:childspec({Name, Options}),
	    supervisor:start_child(kvdb_sup, Child);
	_ ->
	    {error, already_loaded}
    end.

info(Name, Item) ->
    ?KVDB_CATCH(do_info(db(Name), Item), [Name, Item]).

do_info(#kvdb_ref{mod = DbMod, db = Db}, Item) ->
    DbMod:info(Db, Item).

-spec open(Nam::atom(), Options::[{atom(),term()}]) ->
		  {ok,db_ref()} | {error,term()}.

open(Name, Options) when is_atom(Name), is_list(Options) ->
    DbMod = proplists:get_value(backend, Options, kvdb_sqlite3),
    case DbMod:open(Name,Options) of
	{ok, Db} ->
	    io:fwrite("opened ~p database: ~p~n", [DbMod, Options]),
	    {ok, #kvdb_ref{mod = DbMod, db = Db}};
	Error ->
	    io:fwrite("ERROR opening ~p database: ~p. Opts = ~p~n", [DbMod, Error, Options]),
	    Error
    end.

close(#kvdb_ref{mod = DbMod, db = Db}) ->
    DbMod:close(Db);
close(Name) ->
    ?KVDB_CATCH(call(Name, close), [Name]).

db(Name) ->
    call(Name, db).

-spec add_table(Db::db_ref(), Table::atom()) ->
		       ok | {error, any()}.

do_add_table(#kvdb_ref{mod = DbMod, db = Db}, Table) when is_atom(Table) ->
    DbMod:add_table(Db, Table).

add_table(Name, Table)
  when is_atom(Table) ->
    ?KVDB_CATCH(call(Name, {add_table, Table}), [Name, Table]).

-spec delete_table(Db::db_ref(), Table::atom()) ->
			  ok | {error, any()}.

do_delete_table(#kvdb_ref{mod = DbMod, db = Db}, Table)
  when is_atom(Table)->
    DbMod:delete_table(Db, Table).

delete_table(Name, Table)
  when is_atom(Table) ->
    ?KVDB_CATCH(call(Name, {delete_table, Table}), [Name, Table]).

list_tables(#kvdb_ref{mod = DbMod, db = Db}) ->
    DbMod:list_tables(Db);
list_tables(Name) ->
    ?KVDB_CATCH(list_tables(db(Name)), [Name]).



-spec put(Db::db_ref(), Table::atom(), Obj::object()) ->
		 ok | {error, any()}.

do_put(#kvdb_ref{mod = DbMod, db = Db}, Table, {_,_} = Obj) when is_atom(Table) ->
    DbMod:put(Db, Table, Obj);
do_put(#kvdb_ref{mod = DbMod, db = Db}, Table, {_,As,_} = Obj)
  when is_atom(Table), is_list(As) ->
    DbMod:put(Db, Table, Obj).

put(Name, Table, Obj)
  when is_atom(Table), is_tuple(Obj) ->
    ?KVDB_CATCH(call(Name, {put, Table, Obj}), [Name, Table, Obj]).

-spec get(Db::db_ref(), Table::atom(), Key::binary()) ->
		 {ok, binary()} | {error,any()}.

do_get(#kvdb_ref{mod = DbMod, db = Db}, Table, Key)
  when is_atom(Table) ->
    DbMod:get(Db, Table, Key).

get(Name, Table, Key) ->
    #kvdb_ref{} = Ref = call(Name, db),
    do_get(Ref, Table, Key).


-spec delete(Db::db_ref(), Table::atom(), Key::binary()) ->
		    ok | {error, any()}.

do_delete(#kvdb_ref{mod = DbMod, db = Db}, Table, Key)
  when is_atom(Table) ->
    DbMod:delete(Db, Table, Key).

delete(Name, Table, Key) when is_atom(Table) ->
    ?KVDB_CATCH(call(Name, {delete, Table, Key}), [Name, Table, Key]).

-spec first(Db::db_ref(), Table::atom()) ->
		   {ok,Key::binary()} |
		   {ok,Key::binary(),Value::binary()} |
		   done |
		   {error,any()}.

do_first(#kvdb_ref{mod = DbMod, db = Db}, Table) ->
    DbMod:first(Db, Table).

first(Name, Table) ->
    ?KVDB_CATCH(do_first(db(Name), Table), [Name, Table]).


-spec last(Db::db_ref(), Table::atom()) ->
		   {ok,Key::binary()} |
		   {ok,Key::binary(),Value::binary()} |
		   done |
		   {error,any()}.

do_last(#kvdb_ref{mod = DbMod, db = Db}, Table) ->
    DbMod:last(Db, Table).

last(Name, Table) ->
    ?KVDB_CATCH(do_last(db(Name), Table), [Name, Table]).

-spec next(Db::db_ref(), Table::atom(), FromKey::binary()) ->
		   {ok,Key::binary()} |
		   {ok,Key::binary(),Value::binary()} |
		   done |
		   {error,any()}.

do_next(#kvdb_ref{mod = DbMod, db = Db}, Table, Key) ->
    DbMod:next(Db, Table, Key).

next(Name, Table, Key) ->
    ?KVDB_CATCH(do_next(db(Name), Table, Key), [Name, Table, Key]).


-spec prev(Db::db_ref(), Table::atom(), FromKey::binary()) ->
		  {ok,Key::binary()} |
		  {ok,Key::binary(),Value::binary()} |
		  done |
		  {error,any()}.

do_prev(#kvdb_ref{mod = DbMod, db = Db}, Table, Key) ->
    DbMod:prev(Db, Table, Key).

prev(Name, Table, Key) ->
    ?KVDB_CATCH(do_prev(db(Name), Table, Key), [Name, Table, Key]).


prefix_match(Db, Table, Prefix) ->
    ?KVDB_CATCH(do_prefix_match(db(Db), Table, Prefix, default_limit()), [Db, Table, Prefix]).

prefix_match(Db, Table, Prefix, Limit)
  when Limit==infinity orelse (is_integer(Limit) andalso Limit >= 0) ->
    ?KVDB_CATCH(do_prefix_match(db(Db), Table, Prefix, Limit), [Db, Table, Prefix, Limit]).

do_prefix_match(#kvdb_ref{mod = DbMod, db = Db}, Table, Prefix, Limit)
  when Limit==infinity orelse (is_integer(Limit) andalso Limit >= 0) ->
    DbMod:prefix_match(Db, Table, Prefix, Limit).

default_limit() ->
    100.

%% @spec select(Db, Table, MatchSpec) -> {Objects, Cont} | done
%% @doc Similar to ets:select/3.
%%
%% This function builds on prefix_match/3, and applies a match specification on the results.
%% If keys are using `raw' encoding, a partial key can be given using string syntax,
%% e.g. <code>"abc" ++ '_'</code>. Note that this will necessitate some data conversion
%% back and forth on the found objects. If a prefix cannot be determined for the key, a
%% full traversal of the table will be performed. `sext'-encoded keys can be prefixed in the
%% same way as normal erlang terms in an ets:select().
%% @end
%%
select(Db, Table, MatchSpec) ->
    ?KVDB_CATCH(do_select(db(Db), Table, MatchSpec, default_limit()), [Db, Table, MatchSpec]).

select(Db, Table, MatchSpec, Limit) ->
    ?KVDB_CATCH(do_select(db(Db), Table, MatchSpec, Limit), [Db, Table, MatchSpec, Limit]).

do_select(#kvdb_ref{mod = DbMod, db = Db}, Table, MatchSpec, Limit) ->
    MSC = ets:match_spec_compile(MatchSpec),
    Encoding = DbMod:info(Db, encoding),
    {Prefix, Conv} = ms2pfx(MatchSpec, Encoding),
    do_select_(DbMod:prefix_match(Db,Table,Prefix,Limit), Conv, MSC, [], Limit, Limit).

%% We must create a prefix for the prefix_match().
%% This is a problem if we have raw encoding on the key, since you cannot have a wildcard
%% tail on a binary. You can do this on a list, however, so we allow the caller to provide
%% a string pattern on the key - enabling the declaration of a prefix like "foo" ++ '_'.
%%
%% Unfortunately, this conflicts with match_spec_run(): we must convert the results from
%% prefix_match(), changing from binaries to lists, then revert back to binaries on the
%% objects that match. This is wasteful, but presumably faster than setting the prefix to
%% <<>> (the empty binary), forcing select() to traverse the entire table.
%%
ms2pfx([{HeadPat,_,_}|_], Enc) when is_tuple(HeadPat) ->
    Key = element(1, HeadPat),
    case key_encoding(Enc) of
	sext -> {Key, none};
	raw ->
	    raw_prefix(Key, size(HeadPat))
    end;
ms2pfx(_, _) ->
    {<<>>, none}.

raw_prefix(A, _) when is_atom(A) -> {<<>>, none};
raw_prefix(B, _) when is_binary(B) -> {<<>>, none};
raw_prefix(L, Sz) when is_list(L) ->
    P = list_to_binary(raw_list_prefix(L)),
    case Sz of
	2 -> {P, {fun({K,V}) -> {binary_to_list(K), V} end,
		  fun({K,V}) -> {list_to_binary(K), V} end}};
	3 -> {P, {fun({K,A,V}) -> {binary_to_list(K), A, V} end,
		  fun({K,A,V}) -> {list_to_binary(K), A, V} end}}
    end.

raw_list_prefix([H|T]) when is_atom(T) andalso (0 =< H andalso H =< 255) ->
    %% e.g. [...|'_']
    [H];
raw_list_prefix([H|T]) when 0 =< H, H =< 255 ->
    [H|raw_list_prefix(T)].

convert(none, Objs) ->
    Objs;
convert({F,_}, Objs) ->
    [F(Obj) || Obj <- Objs].

revert(none, Objs) ->
    Objs;
revert({_,F}, Objs) ->
    [F(Obj) || Obj <- Objs].

key_encoding(E) when is_tuple(E) ->
    element(1, E);
key_encoding(E) when is_atom(E) ->
    E.


do_select_(done, _, _, Acc, _, _) ->
    {lists:concat(lists:reverse(Acc)), fun() -> done end};
do_select_({Objs, Cont}, Conv, MSC, Acc, Limit, Limit0) ->
    Matches = revert(Conv, ets:match_spec_run(convert(Conv, Objs), MSC)),
    N = length(Matches),
    NewAcc = [Matches | Acc],
    case decr(Limit, N) of
	NewLimit when NewLimit =< 0 ->
	    %% This can result in (> Limit) objects being returned to the caller.
	    {lists:concat(lists:reverse(NewAcc)),
	     fun() ->
		     do_select_(Cont(), Conv, MSC, NewAcc, Limit0, Limit0)
	     end};
	NewLimit when NewLimit > 0 ->
	    do_select_(Cont(), Conv, MSC, NewAcc, NewLimit, Limit0)
    end.

decr(infinity,_) ->
    infinity;
decr(Limit, N) when is_integer(Limit), is_integer(N) ->
    Limit - N.

%% server-related code

call(Name, Req) ->
    Pid = gproc:where({n,l,{kvdb,Name}}),
    case gen_server:call(Pid, Req) of
	badarg ->
	    ?KVDB_THROW(badarg);
	{badarg,_} = Err ->
	    ?KVDB_THROW(Err);
	Res ->
	    Res
    end.

start_link(Name, Backend) ->
    io:fwrite("starting ~p, ~p~n", [Name, Backend]),
    gen_server:start_link(?MODULE, {Name, Backend}, []).

init({Name, Opts}) ->
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
    case open(Name, NewOpts) of
	{ok, Db} ->
	    create_tables_(Db, Opts),
	    {ok, #st{name = Name, db = Db}};
	{error,_} = Error ->
	    io:fwrite("error opening kvdb database ~w:~n"
		      "Error: ~p~n"
		      "Opts = ~p~n", [Name, Error, NewOpts]),
	    Error
    end.

handle_call(Req, From, St) ->
    try handle_call_(Req, From, St)
    catch
	error:badarg ->
	    {reply, badarg, St};
	error:E ->
	    {reply, {badarg,E}, St}
    end.

handle_call_({put, Tab, Obj}, _From, #st{db = Db} = St) ->
    {reply, do_put(Db, Tab, Obj), St};
handle_call_({delete, Tab, Key}, _From, #st{db = Db} = St) ->
    {reply, do_delete(Db, Tab, Key), St};
handle_call_({add_table, Table}, _From, #st{db = Db} = St) ->
    io:fwrite("adding table ~p~n", [Table]),
    {reply, do_add_table(Db, Table), St};
handle_call_({delete_table, Table}, _From, #st{db = Db} = St) ->
    io:fwrite("deleting table ~p~n", [Table]),
    {reply, do_delete_table(Db, Table), St};
handle_call_(close, _From, #st{} = St) ->
    {stop, normal, ok, St};
handle_call_(db, _From, #st{db = Db} = St) ->
    {reply, Db, St}.

handle_info(_, St) ->
    {noreply, St}.

handle_cast(_, St) ->
    {noreply, St}.

terminate(_Reason, #st{db = Db}) ->
    close(Db),
    ok.

code_change(_FromVsn, St, _Extra) ->
    {ok, St}.

mod(mnesia) -> kvdb_mnesia;
mod(leveldb) -> kvdb_leveldb;
mod(sqlite3) -> kvdb_sqlite3;
mod(sqlite) -> kvdb_sqlite3;
mod(M) ->
    case is_behaviour(M) of
	true ->
	    M;
	false ->
	    error(illegal_backend_type)
    end.

name2file({App,Backend}) ->
    lists:flatten(io_lib:fwrite("~w-~w", [App,Backend]));
name2file(Name) when is_atom(Name) ->
    atom_to_list(Name);
name2file(Str) when is_list(Str) ->
    Str.

%% to_atom(A) when is_atom(A) ->
%%     A;
%% to_atom(S) when is_list(S) ->
%%     list_to_atom(S).


is_behaviour(_M) ->
    %% TODO: check that exported functions match those listed in behaviour_info(callbacks).
    true.

create_tables_(Db, Opts) ->
    case proplists:get_value(tables, Opts, []) of
	[] ->
	    ok;
	Ts ->
	    %% We don't warn if there are more tables than we've specified, and we certainly
	    %% don't remove them. Ok to do nothing?
	    Existing = list_tables(Db),
	    New = Ts -- Existing,
	    [do_add_table(Db, T) || T <- New]
    end.
