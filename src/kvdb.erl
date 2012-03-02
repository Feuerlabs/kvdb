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
-export([open/2, close/1, db/1, start_session/2]).
-export([add_table/2, add_table/3, delete_table/2, list_tables/1]).
-export([put/3, put_attr/5, put_attrs/4, get/3,
	 push/3, push/4, pop/2, pop/3, extract/3, list_queue/3, is_queue_empty/3,
	 first_queue/2, next_queue/3,
	 get_attr/4, get_attrs/3, delete/3]).
-export([first/2, last/2, next/3, prev/3]).
-export([prefix_match/3, prefix_match/4]).
-export([select/3, select/4]).

%% direct API towards an active kvdb instance
-export([do_put/3,
	 do_push/3,
	 do_push/4,
	 do_get/3,
	 do_pop/2,
	 do_pop/3,
	 do_extract/3,
	 do_list_queue/3,
	 do_is_queue_empty/3,
	 do_first_queue/2,
	 do_next_queue/3,
	 do_get_attr/4,
	 do_get_attrs/3,
	 do_put_attr/5,
	 do_put_attrs/4,
	 do_delete/3,
	 do_add_table/3,
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

%% -import(kvdb_schema, [validate/3, validate_attr/3,
%% 		      encode/3, encode_attr/3,
%% 		      decode/3, on_update/4]).
-import(kvdb_lib, [table_name/1]).

-include("kvdb.hrl").

-record(st, {name, db, is_owner = false}).

-define(KVDB_CATCH(Expr, Args),
	try Expr
	catch
	    throw:{kvdb_throw, __E} ->
		%% error(__E, Args)
		error(__E, erlang:get_stacktrace())
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
     {info, 2},
     {get_schema_mod, 2},
     {open,2},
     {close,1},
     {add_table,3},
     {delete_table,2},
     {put,3},
     {get,3},
     {push,4},
     {pop,3},
     {extract,3},
     {list_queue, 3},
     {is_queue_empty, 3},
     {first_queue, 2},
     {next_queue, 3},
     {pop,3},
     {delete,3},
     %% {iterator,2},          % may remove
     %% {iterator_close,2},    % may remove
     {first,2},
     {last,2},
     {next,3},
     {prev,3}
    ];
behaviour_info(_Other) ->
    undefined.

start() ->
    application:start(gproc),
    application:start(kvdb).

-spec open_db(name(), options()) -> {ok, pid()} | {error, any()}.

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

-spec info(name(), attr_name()) -> undefined | attr_value().
info(Name, Item) ->
    ?KVDB_CATCH(do_info(db(Name), Item), [Name, Item]).

-spec do_info(db_ref(), attr_name()) -> undefined | attr_value().
do_info(#kvdb_ref{mod = DbMod, db = Db}, Item) ->
    DbMod:info(Db, Item).

-spec open(name(), Options::[{atom(),term()}]) ->
		  {ok,db_ref()} | {error,term()}.

open(Name, Options) ->
    supervisor:start_child(kvdb_sup, kvdb_sup:childspec({Name, Options})).

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

close(#kvdb_ref{mod = DbMod, db = Db}) ->
    DbMod:close(Db);
close(Name) ->
    ?KVDB_CATCH(call(Name, close), [Name]).

-spec db(name() | db_ref()) -> db_ref().
db(#kvdb_ref{} = Db) ->
    Db;
db(Name) ->
    call(Name, db).

add_table(Name, Table) ->
    add_table(Name, Table, [{type, set}]).

add_table(Name, Table, Opts) when is_list(Opts) ->
    ?KVDB_CATCH(call(Name, {add_table, Table, Opts}), [Name, Table, Opts]).

-spec do_add_table(Db::db_ref(), Table::table(), Opts::list()) ->
			  ok | {error, any()}.

do_add_table(#kvdb_ref{mod = DbMod, db = Db}, Table0, Opts) ->
    Table = kvdb_lib:valid_table_name(Table0),
    DbMod:add_table(Db, Table, Opts).

-spec do_delete_table(Db::db_ref(), Table::table()) ->
			     ok | {error, any()}.

do_delete_table(#kvdb_ref{mod = DbMod, db = Db}, Table0) ->
    Table = table_name(Table0),
    DbMod:delete_table(Db, Table).

delete_table(Name, Table) ->
    ?KVDB_CATCH(call(Name, {delete_table, Table}), [Name, Table]).

list_tables(#kvdb_ref{mod = DbMod, db = Db}) ->
    DbMod:list_tables(Db);
list_tables(Name) ->
    ?KVDB_CATCH(list_tables(db(Name)), [Name]).



-spec do_put(Db::db_ref(), Table::table(), Obj::object()) ->
		    ok | {error, any()}.

do_put(#kvdb_ref{} = DbRef, Table0, {_,_} = Obj) ->
    Table = table_name(Table0),
    do_put_(DbRef, Table, Obj);
do_put(#kvdb_ref{} = DbRef, Table0, {K,As,V}) when is_list(As) ->
    Table = table_name(Table0),
    do_put_(DbRef, Table, {K, fix_attrs(As), V}).

do_put_(#kvdb_ref{mod = DbMod, db = Db, schema = Schema} = DbRef, Table, Obj) ->
    case DbMod:put(Db, Table,
		   Schema:validate(DbRef, put, Actual = Schema:encode(DbRef, obj, Obj))) of
	ok ->
	    Schema:on_update(put, DbRef, Table, Actual),
	    ok;
	Error ->
	    Error
    end.

-spec put(any(), Table::table(), Obj::object()) ->
		 ok | {error, any()}.
put(Name, Table, Obj) when is_tuple(Obj) ->
    ?KVDB_CATCH(call(Name, {put, Table, Obj}), [Name, Table, Obj]).


-spec do_put_attr(db_ref(), Table::table(), Key::key(), atom(), any()) ->
			 ok | {error, any()}.
do_put_attr(#kvdb_ref{mod = DbMod, db = Db, schema = Schema} = DbRef,
	    Table0, Key, AttrN, Value)
  when is_atom(AttrN) ->
    Table = table_name(Table0),
    Attr = Schema:validate_attr(DbRef, Key, Schema:encode_attr(Db, Key, {AttrN, Value})),
    case DbMod:put_attr(Db, Table, Key, Attr) of
	{ok, Actual} ->
	    Schema:on_update(put_attr, DbRef, Table, {Key, Attr}),
	    {ok, Actual};
	Error ->
	    Error
    end.

-spec put_attr(name(), Table::table(), Key::key(), atom(), any()) ->
		      ok | {error, any()}.
put_attr(Name, Table, Key, Attr, Value) when is_atom(Attr) ->
    ?KVDB_CATCH(call(Name, {put_attr, Table, Key, Attr, Value}),
		[Name, Table, Key, Attr, Value]).


do_put_attrs(#kvdb_ref{mod = DbMod, db = Db}, Table0, Key, As) ->
    Table = table_name(Table0),
    DbMod:put_attrs(Db, Table, Key, fix_attrs(As)).

-spec put_attrs(any(), Table::table(), Key::key(), Attrs::attrs()) ->
		       ok | {error, any()}.
put_attrs(Name, Table, Key, As) when is_list(As) ->
    ?KVDB_CATCH(call(Name, {put_attrs, Table, Key, As}), [Name, Table, Key, As]).


-spec do_get(Db::db_ref(), Table::table(), Key::binary()) ->
		    {ok, binary()} | {error,any()}.

do_get(#kvdb_ref{mod = DbMod, db = Db, schema = Schema}, Table0, Key) ->
    Table = table_name(Table0),
    case DbMod:get(Db, Table, Key) of
	{ok, Obj} ->
	    {ok, Schema:decode(Db, Table, Obj)};
	Other ->
	    Other
    end.

-spec get(name(), Table::table(), Key::binary()) ->
		 {ok, binary()} | {error,any()}.

get(Name, Table, Key) ->
    #kvdb_ref{} = Ref = call(Name, db),
    ?KVDB_CATCH(do_get(Ref, Table, Key), [Name, Table, Key]).

-spec do_push(Db::db_ref(), Table::table(), Obj::object()) ->
		     {ok, ActualKey::any()} | {error, any()}.
do_push(Db, Table, Obj) ->
    do_push(Db, Table, <<>>, Obj).

-spec do_push(Db::db_ref(), Table::table(), Q::any(), Obj::object()) ->
		     {ok, ActualKey::any()} | {error, any()}.

do_push(#kvdb_ref{} = DbRef, Table0, Q, {_,_} = Obj) ->
    Table = table_name(Table0),
    do_push_(DbRef, Table, Q, Obj);
do_push(#kvdb_ref{} = DbRef, Table0, Q, {K,As,V}) when is_list(As) ->
    Table = table_name(Table0),
    do_push_(DbRef, Table, Q, {K, fix_attrs(As), V}).

do_push_(#kvdb_ref{mod = DbMod, db = Db, schema = Schema} = DbRef, Table, Q, Obj) ->
    case DbMod:push(Db, Table, Q,
		    Schema:validate(DbRef, put, Actual = Schema:encode(DbRef, obj, Obj))) of
	{ok, ActualKey} ->
	    Schema:on_update({push,Q}, DbRef, Table, Actual),
	    {ok, ActualKey};
	Error ->
	    Error
    end.

-spec push(any(), Table::table(), Obj::object()) ->
		 {ok, ActualKey::any()} | {error, any()}.
push(Name, Table, Obj) when is_tuple(Obj) ->
    push(Name, Table, <<>>, Obj).

-spec push(any(), Table::table(), Q::any(), Obj::object()) ->
		 {ok, ActualKey::any()} | {error, any()}.
push(Name, Table, Q, Obj) when is_tuple(Obj) ->
    ?KVDB_CATCH(call(Name, {push, Table, Q, Obj}), [Name, Table, Q, Obj]).


-spec do_pop(Db::db_ref(), Table::table()) ->
		    {ok, object()} | done | {error,any()}.

do_pop(Db, Table) ->
    do_pop(Db, Table, <<>>).

-spec do_pop(Db::db_ref(), Table::table(), Q::any()) ->
		    {ok, object()} | done | {error,any()}.

do_pop(#kvdb_ref{mod = DbMod, db = Db, schema = Schema} = DbRef, Table0, Q) ->
    Table = table_name(Table0),
    case DbMod:pop(Db, Table, Q) of
	{ok, Obj, IsEmpty} ->
	    Schema:on_update({pop,Q,IsEmpty}, DbRef, Table, Obj),
	    {ok, Schema:decode(Db, Table, Obj)};
	done ->
	    done
    end.

-spec pop(name(), Table::table()) ->
		 {ok, object()} | done | {error,any()}.
pop(Name, Table) ->
    pop(Name, Table, <<>>).

-spec pop(name(), Table::table(), Q::any()) ->
		 {ok, object()} | done | {error,any()}.
pop(Name, Table, Q) ->
    ?KVDB_CATCH(call(Name, {pop, Table, Q}), [Name, Table, Q]).

-spec extract(name(), Table::table(), Key::binary()) ->
		 {ok, object()} | {error,any()}.

extract(Name, Table, Key) ->
    ?KVDB_CATCH(call(Name, {extract, Table, Key}), [Name, Table, Key]).

-spec do_extract(#kvdb_ref{}, Table::table(), Key::binary()) ->
			{ok, object()} | {error,any()}.
do_extract(#kvdb_ref{mod = DbMod, db = Db, schema = Schema} = DbRef, Table0, Key) ->
    Table = table_name(Table0),
    case DbMod:extract(Db, Table, Key) of
	{ok, Obj, Q, IsEmpty} ->
	    Schema:on_update({pop,Q,IsEmpty}, DbRef, Table, Obj),
	    {ok, Obj};
	Other ->
	    Other
    end.


-spec list_queue(name(), Table::table(), Q::any()) ->
			[object()] | {error,any()}.

list_queue(Name, Table, Q) ->
    #kvdb_ref{} = Ref = call(Name, db),
    ?KVDB_CATCH(do_list_queue(Ref, Table, Q), [Name, Table, Q]).

-spec do_list_queue(#kvdb_ref{}, Table::table(), Q::any()) ->
			   [object()] | {error,any()}.
do_list_queue(#kvdb_ref{mod = DbMod, db = Db}, Table0, Q) ->
    Table = table_name(Table0),
    DbMod:list_queue(Db, Table, Q).

-spec is_queue_empty(name(), table(), _Q::any()) -> boolean().

is_queue_empty(Name, Table, Q) ->
    #kvdb_ref{} = Ref = call(Name, db),
    ?KVDB_CATCH(do_is_queue_empty(Ref, Table, Q), [Name, Table, Q]).

-spec do_is_queue_empty(#kvdb_ref{}, table(), _Q::any()) -> boolean().
do_is_queue_empty(#kvdb_ref{mod = DbMod, db = Db}, Table0, Q) ->
    DbMod:is_queue_empty(Db, table_name(Table0), Q).

-spec first_queue(name(), table()) -> {ok, any()} | done.
first_queue(Name, Table) ->
    #kvdb_ref{} = Ref = call(Name, db),
    ?KVDB_CATCH(do_first_queue(Ref, Table), [Name, Table]).

-spec do_first_queue(#kvdb_ref{}, table()) -> {ok, any()} | done.
do_first_queue(#kvdb_ref{mod = DbMod, db = Db}, Table0) ->
    Table = table_name(Table0),
    DbMod:first_queue(Db, Table).

-spec next_queue(name(), table(), _Q::any()) -> {ok, any()} | done.
next_queue(Name, Table, Q) ->
    #kvdb_ref{} = Ref = call(Name, db),
    ?KVDB_CATCH(do_next_queue(Ref, Table, Q), [Name, Table, Q]).

-spec do_next_queue(#kvdb_ref{}, table(), _Q::any()) -> {ok, any()} | done.
do_next_queue(#kvdb_ref{mod = DbMod, db = Db}, Table0, Q) ->
    Table = table_name(Table0),
    DbMod:next_queue(Db, Table, Q).

do_get_attr(#kvdb_ref{mod = DbMod, db = Db}, Table0, Key, Attr) when is_atom(Attr) ->
    Table = table_name(Table0),
    DbMod:get_attr(Db, Table, Key, Attr).

get_attr(Name, Table, Key, Attr) when is_atom(Attr) ->
    ?KVDB_CATCH(do_get_attr(db(Name), Table, Key, Attr), [Name, Table, Key, Attr]).


do_get_attrs(#kvdb_ref{mod = DbMod, db = Db}, Table0, Key) ->
    Table = table_name(Table0),
    DbMod:get_attrs(Db, Table, Key).

get_attrs(Name, Table, Key) ->
    ?KVDB_CATCH(do_get_attrs(db(Name), Table, Key), [Name, Table, Key]).


-spec do_delete(Db::db_ref(), Table::table(), Key::binary()) ->
		       ok | {error, any()}.

do_delete(#kvdb_ref{mod = DbMod, db = Db}, Table0, Key) ->
    Table = table_name(Table0),
    DbMod:delete(Db, Table, Key).

delete(Name, Table, Key) ->
    ?KVDB_CATCH(call(Name, {delete, Table, Key}), [Name, Table, Key]).

-spec do_first(Db::db_ref(), Table::table()) ->
		      {ok,Key::binary()} |
		      {ok,Key::binary(),Value::binary()} |
		      done |
		      {error,any()}.

do_first(#kvdb_ref{mod = DbMod, db = Db}, Table0) ->
    Table = table_name(Table0),
    DbMod:first(Db, Table).

first(Name, Table) ->
    ?KVDB_CATCH(do_first(db(Name), Table), [Name, Table]).


-spec do_last(Db::db_ref(), Table::table()) ->
		     {ok,Key::binary()} |
		     {ok,Key::binary(),Value::binary()} |
		     done |
		     {error,any()}.

do_last(#kvdb_ref{mod = DbMod, db = Db}, Table0) ->
    Table = table_name(Table0),
    DbMod:last(Db, Table).

last(Name, Table) ->
    ?KVDB_CATCH(do_last(db(Name), Table), [Name, Table]).

-spec do_next(Db::db_ref(), Table::table(), FromKey::binary()) ->
		     {ok,Key::binary()} |
		     {ok,Key::binary(),Value::binary()} |
		     done |
		     {error,any()}.

do_next(#kvdb_ref{mod = DbMod, db = Db}, Table0, Key) ->
    Table = table_name(Table0),
    DbMod:next(Db, Table, Key).

next(Name, Table, Key) ->
    ?KVDB_CATCH(do_next(db(Name), Table, Key), [Name, Table, Key]).


-spec do_prev(Db::db_ref(), Table::table(), FromKey::binary()) ->
		     {ok,Key::binary()} |
		     {ok,Key::binary(),Value::binary()} |
		     done |
		     {error,any()}.

do_prev(#kvdb_ref{mod = DbMod, db = Db}, Table0, Key) ->
    Table = table_name(Table0),
    DbMod:prev(Db, Table, Key).

prev(Name, Table, Key) ->
    ?KVDB_CATCH(do_prev(db(Name), Table, Key), [Name, Table, Key]).


prefix_match(Db, Table, Prefix) ->
    ?KVDB_CATCH(do_prefix_match(db(Db), Table, Prefix, default_limit()), [Db, Table, Prefix]).

prefix_match(Db, Table, Prefix, Limit)
  when Limit==infinity orelse (is_integer(Limit) andalso Limit >= 0) ->
    ?KVDB_CATCH(do_prefix_match(db(Db), Table, Prefix, Limit), [Db, Table, Prefix, Limit]).

do_prefix_match(#kvdb_ref{mod = DbMod, db = Db}, Table0, Prefix, Limit)
  when Limit==infinity orelse (is_integer(Limit) andalso Limit >= 0) ->
    Table = table_name(Table0),
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

do_select(#kvdb_ref{mod = DbMod, db = Db}, Table0, MatchSpec, Limit) ->
    Table = table_name(Table0),
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

start_link(Name, Backend) ->
    io:fwrite("starting ~p, ~p~n", [Name, Backend]),
    gen_server:start_link(?MODULE, {owner, Name, Backend}, []).

start_session(Name, Id) ->
    gen_server:start_link(?MODULE, session(Name, Id), []).

session(Name, Id) ->
    {Name, session, Id}.

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
handle_call_({push, Tab, Q, Obj}, _From, #st{db = Db} = St) ->
    {reply, do_push(Db, Tab, Q, Obj), St};
handle_call_({pop, Tab, Q}, _From, #st{db = Db} = St) ->
    {reply, do_pop(Db, Tab, Q), St};
handle_call_({extract, Tab, Key}, _From, #st{db = Db} = St) ->
    {reply, do_extract(Db, Tab, Key), St};
handle_call_({put_attr, Table, Key, Attr, Value}, _From, #st{db = Db} = St) ->
    {reply, do_put_attr(Db, Table, Key, Attr, Value), St};
handle_call_({put_attrs, Tab, Key, As}, _From, #st{db = Db} = St) ->
    {reply, do_put_attrs(Db, Tab, Key, As), St};
handle_call_({delete, Tab, Key}, _From, #st{db = Db} = St) ->
    {reply, do_delete(Db, Tab, Key), St};
handle_call_({add_table, Table, Opts}, _From, #st{db = Db} = St) ->
    io:fwrite("adding table ~p~n", [Table]),
    {reply, do_add_table(Db, Table, Opts), St};
handle_call_({delete_table, Table}, _From, #st{db = Db} = St) ->
    io:fwrite("deleting table ~p~n", [Table]),
    {reply, do_delete_table(Db, Table), St};
handle_call_(close, _From, #st{is_owner = true} = St) ->
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
	    Tabs0 = lists:map(fun({T,Os}) ->
				      {table_name(T), Os};
				 (T) -> {table_name(T),[]}
			      end, Ts),
	    %% We don't warn if there are more tables than we've specified, and we certainly
	    %% don't remove them. Ok to do nothing?
	    Tables = internal_tables() ++ Tabs0,
	    Existing = list_tables(Db),
	    New = lists:filter(fun({T,_}) -> not lists:member(T, Existing) end, Tables),
	    [do_add_table(Db, T, Os) || {T, Os} <- New]
    end.

internal_tables() ->
    [].

fix_attrs(As) ->
    %% Treat the list of attributes as a proplist. This means there can be duplicates.
    %% Return an orddict, where values from the head of the list take priority over values
    %% from tail.
    lists:foldr(fun({K,V}, Acc) when is_atom(K) ->
			orddict:store(K, V, Acc)
		end, orddict:new(), As).

