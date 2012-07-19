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
%%% @author Tony Rogvall <tony@rogvall.se>
%%% @doc
%%% Key-value database frontend
%%%
%%% Kvdb is a key-value database library, supporting different backends
%%% (currently: sqlite3 and leveldb), and a number of different table types.
%%%
%%% Feature overview:
%%%
%%% - Multiple logical tables per database
%%%
%%% - Persistent ordered-set semantics
%%%
%%% - `{Key, Value}' or `{Key, Attributes, Value}' structure (per-table)
%%%
%%% - Table types: set (normal) or queue (FIFO, LIFO or keyed FIFO or LIFO)
%%%
%%% - Attributes can be indexed
%%%
%%% - Schema-based validation (per-database) with update triggers
%%%
%%% - Prefix matching
%%%
%%% - ETS-style select() operations
%%%
%%% - Configurable encoding schemes (raw, sext or term_to_binary)
%%%
%%% @end

-module(kvdb_direct).

%% direct API towards an active kvdb instance
-export([put/3,
	 push/3,
	 push/4,
	 get/3,
	 get_attrs/4,
	 index_get/4,
	 index_keys/4,
	 update_counter/4,
	 pop/2,
	 pop/3,
	 prel_pop/2,
	 prel_pop/3,
	 extract/3,
	 mark_queue_object/4,
	 list_queue/3,
	 list_queue/6,
	 list_tables/1,
	 is_queue_empty/3,
	 first_queue/2,
	 next_queue/3,
	 queue_insert/5,
	 queue_delete/3,
	 queue_read/3,
	 delete/3,
	 add_table/3,
	 delete_table/2,
	 first/2,
	 next/3,
	 prev/3,
	 last/2,
	 prefix_match/4,
	 prefix_match_rel/5,
	 select/4,
	 info/2,
	 dump_tables/1]).

%% -import(kvdb_schema, [validate/3, validate_attr/3, on_update/4]).
-import(kvdb_lib, [table_name/1, on_update/4]).

-include("kvdb.hrl").
-include_lib("lager/include/log.hrl").

-spec info(db_ref(), attr_name()) -> undefined | attr_value().
info(#kvdb_ref{mod = DbMod, db = Db}, Item) ->
    DbMod:info(Db, Item).

-spec dump_tables(db_ref()) -> list().
%% @doc Low-level equivalent to {@link dump_tables/1}
%% @end
dump_tables(#kvdb_ref{mod = DbMod, db = Db}) ->
    ?debug("dump_tables(#kvdb_ref{mod = ~p})~n", [DbMod]),
    DbMod:dump_tables(Db).

%% @doc Low-level equivalent to {@link add_table/3}
%% @end
add_table(#kvdb_ref{mod = DbMod, db = Db} = R, Table0, Opts) ->
    ?debug("add_table(#kvdb_ref{mod = ~p}, ~p, ~p)~n", [DbMod, Table0, Opts]),
    Table = kvdb_lib:valid_table_name(Table0),
    case DbMod:add_table(Db, Table, Opts) of
	ok ->
	    on_update(add_table, R, Table, Opts),
	    ok;
	Other ->
	    Other
    end.

-spec delete_table(Db::db_ref(), Table::table()) ->
			     ok | {error, any()}.
%% @doc low-level equivalent to {@link delete_table/2}
%% @end
delete_table(#kvdb_ref{mod = DbMod, db = Db} = R, Table0) ->
    ?debug("delete_table(#kvdb_ref{mod = ~p}, ~p)~n", [DbMod, Table0]),
    Table = table_name(Table0),
    case DbMod:delete_table(Db, Table) of
	ok ->
	    on_update(delete_table, R, Table, []),
	    ok;
	Other ->
	    Other
    end.

-spec list_tables(db_ref()) -> [binary()].
%% @doc Lists the tables defined in the database
%% @end
list_tables(#kvdb_ref{mod = DbMod, db = Db}) ->
    ?debug("list_tables(#kvdb_ref{mod = ~p})~n", [DbMod]),
    DbMod:list_tables(Db).

-spec put(Db::db_ref(), Table::table(), Obj::object()) ->
		    ok | {error, any()}.
%% @doc Low-level equivalent to {@link put/3}
%% @end
put(#kvdb_ref{mod = DbMod} = DbRef, Table0, {_,_} = Obj) ->
    ?debug("put(#kvdb_ref{mod = ~p}, ~p, ~p)~n", [DbMod, Table0, Obj]),
    Table = table_name(Table0),
    put_(DbRef, Table, Obj);
put(#kvdb_ref{mod = DbMod} = DbRef, Table0, {K,As,V} = _O) when is_list(As) ->
    ?debug("put(#kvdb_ref{mod = ~p}, ~p, ~p)~n", [DbMod, Table0, _O]),
    Table = table_name(Table0),
    put_(DbRef, Table, {K, fix_attrs(As), V}).

put_(#kvdb_ref{mod = DbMod, db = Db, schema = Schema} = DbRef, Table, Obj) ->
    if_table(DbMod, Db, Table,
	     fun() ->
		     case DbMod:put(Db, Table,
				    Actual =
					Schema:validate(DbRef, put, Obj)) of
			 ok ->
			     on_update(put, DbRef, Table, Actual),
			     ok;
			 Error ->
			     Error
		     end
	     end).

-spec get(Db::db_ref(), Table::table(), Key::any()) ->
		    {ok, object()} | {error,any()}.
%% @doc Low-level equivalent of {@link get/3}
%% @end
get(#kvdb_ref{mod = DbMod, db = Db}, Table0, Key) ->
    ?debug("get(#kvdb_ref{mod = ~p}, ~p, ~p)~n", [DbMod, Table0, Key]),
    Table = table_name(Table0),
    case DbMod:get(Db, Table, Key) of
	{ok, Obj} ->
	    {ok, Obj};
	{error, _} = Other ->
	    Other
    end.

get_attrs(#kvdb_ref{mod = DbMod, db = Db}, Table0, Key, As) ->
    ?debug("get_attrs(#kvdb_ref{mod = ~p}, ~p, ~p, ~p)~n", [DbMod, Table0,
							    Key, As]),
    Table = table_name(Table0),
    DbMod:get_attrs(Db, Table, Key, As).

-spec index_get(db_ref(), table(), _IxName::any(), _IxVal::any()) ->
			  [object()] | {error, any()}.
%% @doc Low-level equivalent of {@link index_get/4}
%% @end
index_get(#kvdb_ref{mod = DbMod, db = Db}, Table0, IxName, IxVal) ->
    ?debug("index_get(#kvdb_ref{mod = ~p}, ~p, ~p, ~p)~n",
	   [DbMod, Table0, IxName, IxVal]),
    Table = table_name(Table0),
    case DbMod:index_get(Db, Table, IxName, IxVal) of
	Res when is_list(Res) -> Res
    end.

%% @doc Low-level equivalent of {@link index_keys/4}
%% @end
index_keys(#kvdb_ref{mod = DbMod, db = Db}, Table0, IxName, IxVal) ->
    ?debug("index_keys(#kvdb_ref{mod = ~p}, ~p, ~p, ~p)~n",
	   [DbMod, Table0, IxName, IxVal]),
    Table = table_name(Table0),
    case DbMod:index_keys(Db, Table, IxName, IxVal) of
	Res when is_list(Res) -> Res
    end.

-spec update_counter(Db::db_ref(), Table::table(), Key::binary(),
		     Incr::increment()) -> integer() | binary().
update_counter(#kvdb_ref{mod = DbMod, db = Db} = DbRef, Table0, Key, Incr) ->
    ?debug("update_counter(#kvdb_ref{mod = ~p}, ~p, ~p, ~p)~n",
	   [DbMod, Table0, Key, Incr]),
    Table = table_name(Table0),
    if_table(DbMod, Db, Table,
	     fun() ->
		     Res = DbMod:update_counter(Db, Table, Key, Incr),
		     on_update(update_counter, DbRef, Table, {Key, Incr, Res}),
		     Res
	     end).

-spec push(Db::db_ref(), Table::table(), Obj::object()) ->
		     {ok, ActualKey::any()} | {error, any()}.
%% @equiv push(Db, Table, <<>>, Obj)
%%
push(Db, Table, Obj) ->
    push(Db, Table, <<>>, Obj).

-spec push(Db::db_ref(), Table::table(), Q::any(), Obj::object()) ->
		     {ok, ActualKey::any()} | {error, any()}.
%% @doc Low-level equivalent of {@link push/4}
%% @end
push(#kvdb_ref{mod = DbMod} = DbRef, Table0, Q, {_,_} = Obj) ->
    ?debug("push(#kvdb_ref{mod = ~p}, ~p, ~p, ~p)~n", [DbMod, Table0, Q, Obj]),
    Table = table_name(Table0),
    push_(DbRef, Table, Q, Obj);
push(#kvdb_ref{mod = DbMod} = DbRef, Table0, Q, {K,As,V} = _O)
  when is_list(As) ->
    ?debug("push(#kvdb_ref{mod = ~p}, ~p, ~p, ~p)~n", [DbMod, Table0, Q, _O]),
    Table = table_name(Table0),
    push_(DbRef, Table, Q, {K, fix_attrs(As), V}).

push_(#kvdb_ref{mod = DbMod, db = Db, schema = Schema} = DbRef,
	 Table, Q, Obj) ->
    if_table(
      DbMod, Db, Table,
      fun() ->
	      case DbMod:push(Db, Table, Q,
			      Actual = Schema:validate(DbRef, put, Obj)) of
		  {ok, ActualKey} ->
		      on_update({q_op,push,Q,false}, DbRef, Table,
				{Actual,Obj}),
		      {ok, ActualKey};
		  Error ->
		      Error
	      end
      end).

-spec pop(db_ref(), table()) ->
		    {ok, object()} | done | {error,any()}.
%% @equiv pop(Db, Table, <<>>)
%%
pop(Db, Table) ->
    pop(Db, Table, <<>>).

-spec pop(Db::db_ref(), Table::table(), queue_name()) ->
		    {ok, object()} |
		    done |
		    blocked |
		    {error,any()}.
%% @doc Low-level equivalent of {@link pop/3}
%% @end
pop(#kvdb_ref{mod = DbMod, db = Db} = DbRef, Table0, Q) ->
    ?debug("push(#kvdb_ref{mod = ~p}, ~p, ~p)~n", [DbMod, Table0, Q]),
    Table = table_name(Table0),
    if_table(
      DbMod, Db, Table,
      fun() ->
	      case DbMod:pop(Db, Table, Q) of
		  {ok, Obj, IsEmpty} ->
		      on_update({q_op,pop,Q,IsEmpty}, DbRef, Table, Obj),
		      {ok, Obj};
		  blocked -> blocked;
		  done    -> done
	      end
      end).

-spec prel_pop(Db::db_ref(), Table::table()) ->
			 {ok, object(), binary()} |
			 done |
			 blocked |
			 {error,any()}.

prel_pop(Db, Table) ->
    prel_pop(Db, Table, <<>>).

-spec prel_pop(Db::db_ref(), Table::table(), queue_name()) ->
			 {ok, object(), binary()} |
			 done |
			 blocked |
			 {error,any()}.

prel_pop(#kvdb_ref{mod = DbMod, db = Db} = DbRef,
	    Table0, Q) ->
    Table = table_name(Table0),
    if_table(
      DbMod, Db, Table,
      fun() ->
	      case DbMod:prel_pop(Db, Table, Q) of
		  {ok, Obj, RealKey, IsEmpty} ->
		      on_update({q_op,prel_pop,Q,IsEmpty}, DbRef, Table, Obj),
		      {ok, Obj, RealKey};
		  blocked -> blocked;
		  done    -> done
	      end
      end).

-spec extract(#kvdb_ref{}, Table::table(), Key::#q_key{}) ->
			{ok, object()} | {error,any()}.
extract(#kvdb_ref{mod = DbMod,
		  db = Db} = DbRef, Table0, #q_key{} = Key) ->
    Table = table_name(Table0),
    case DbMod:extract(Db, Table, Key) of
	{ok, Obj, Q, IsEmpty} ->
	    on_update({q_op,extract,Q,IsEmpty}, DbRef, Table, Key),
	    {ok, Obj};
	Other ->
	    Other
    end.

-spec mark_queue_object(#kvdb_ref{}, Table::table(), Key::#q_key{},
			St::active | blocking | inactive) ->
			       ok | {error, any()}.
mark_queue_object(#kvdb_ref{mod = DbMod,
			    db = Db} = DbRef, Table0, #q_key{} = Key, St) when
      St == active; St == blocking; St == inactive ->
    Table = table_name(Table0),
    case DbMod:mark_queue_object(Db, Table, Key, St) of
	{ok, Q, Obj} ->
	    IsEmpty = DbMod:is_empty(Db, Table, Q),
	    on_update({q_op,mark_obj,Q,IsEmpty}, DbRef, Table, {St,Key,Obj});
	Other ->
	    Other
    end.

-spec list_queue(#kvdb_ref{}, Table::table(), Q::queue_name()) ->
			   [object()] | {error,any()}.
list_queue(#kvdb_ref{mod = DbMod, db = Db}, Table0, Q) ->
    Table = table_name(Table0),
    DbMod:list_queue(Db, Table, Q).

-spec list_queue(#kvdb_ref{}, Table::table(), Q::queue_name(),
		    _Fltr :: fun((active|inactive, tuple()) ->
					keep | keep_raw | skip | tuple()),
		    _Inactive :: boolean(), _Limit :: integer() | infinity) ->
			   [object()] | {error,any()}.
list_queue(#kvdb_ref{mod = DbMod, db = Db}, Table0, Q,
	      Fltr, Inactive, Limit) ->
    Table = table_name(Table0),
    DbMod:list_queue(Db, Table, Q, Fltr, Inactive, Limit).

-spec is_queue_empty(#kvdb_ref{}, table(), _Q::queue_name()) -> boolean().
is_queue_empty(#kvdb_ref{mod = DbMod, db = Db}, Table0, Q) ->
    DbMod:is_queue_empty(Db, table_name(Table0), Q).

-spec first_queue(#kvdb_ref{}, table()) -> {ok, queue_name()} | done.
first_queue(#kvdb_ref{mod = DbMod, db = Db}, Table0) ->
    Table = table_name(Table0),
    DbMod:first_queue(Db, Table).

-spec next_queue(#kvdb_ref{}, table(), _Q::queue_name()) ->
			   {ok, any()} | done.
next_queue(#kvdb_ref{mod = DbMod, db = Db}, Table0, Q) ->
    Table = table_name(Table0),
    DbMod:next_queue(Db, Table, Q).

-spec queue_insert(#kvdb_ref{}, table(), #q_key{}, status(), object()) ->
			  ok.
queue_insert(#kvdb_ref{mod = DbMod, db = Db} = DbRef, Table0,
	     #q_key{queue = Q} = QKey, St, Obj) when
      St == active; St == inactive; St == blocking ->
    Table = table_name(Table0),
    if_table(DbMod, Db, Table,
	     fun() ->
		     ok = DbMod:queue_insert(Db, Table, QKey, St, Obj),
		     IsEmpty = is_queue_empty(DbRef, Table, Q),
		     on_update({q_op, push,Q,IsEmpty}, DbRef, Table,
			       {QKey, Obj}),
		     ok
	     end).

-spec queue_delete(#kvdb_ref{}, table(), #q_key{}) -> ok.
queue_delete(#kvdb_ref{mod = DbMod, db = Db}, Table0, #q_key{} = QKey) ->
    Table = table_name(Table0),
    if_table(DbMod, Db, Table,
	     fun() ->
		     DbMod:queue_delete(Db, Table, QKey)
	     end).

-spec queue_read(#kvdb_ref{}, table(), #q_key{}) ->
			{ok, status(), object()} | {error, any()}.
queue_read(#kvdb_ref{mod = DbMod, db = Db}, Table0, #q_key{} = QKey) ->
    Table = table_name(Table0),
    if_table(DbMod, Db, Table,
	     fun() ->
		     DbMod:queue_read(Db, Table, QKey)
	     end).

-spec delete(Db::db_ref(), Table::table(), Key::binary()) ->
		       ok | {error, any()}.

delete(#kvdb_ref{mod = DbMod, db = Db} = DbRef, Table0, Key) ->
    Table = table_name(Table0),
    if_table(DbMod, Db, Table,
	     fun() ->
		     DbMod:delete(Db, Table, Key),
		     case DbMod:info(Db, {Table,type}) of
			 T when T==set ->
			     on_update(delete, DbRef, Table, Key);
			 T ->
			     queue_delete_event(DbRef, Table, T, Key)
		     end
	     end).

-spec first(Db::db_ref(), Table::table()) ->
		      {ok,Key::binary()} |
		      {ok,Key::binary(),Value::binary()} |
		      done.

first(#kvdb_ref{mod = DbMod, db = Db}, Table0) ->
    Table = table_name(Table0),
    DbMod:first(Db, Table).

-spec last(Db::db_ref(), Table::table()) ->
		     {ok,Key::binary()} |
		     {ok,Key::binary(),Value::binary()} |
		     done.

last(#kvdb_ref{mod = DbMod, db = Db}, Table0) ->
    Table = table_name(Table0),
    DbMod:last(Db, Table).

-spec next(Db::db_ref(), Table::table(), FromKey::binary()) ->
		     {ok,Key::binary()} |
		     {ok,Key::binary(),Value::binary()} |
		     done.

next(#kvdb_ref{mod = DbMod, db = Db}, Table0, Key) ->
    Table = table_name(Table0),
    DbMod:next(Db, Table, Key).

-spec prev(Db::db_ref(), Table::table(), FromKey::binary()) ->
		     {ok,Key::binary()} |
		     {ok,Key::binary(),Value::binary()} |
		     done.

prev(#kvdb_ref{mod = DbMod, db = Db}, Table0, Key) ->
    Table = table_name(Table0),
    DbMod:prev(Db, Table, Key).


prefix_match(#kvdb_ref{mod = DbMod, db = Db}, Table0, Prefix, Limit)
  when Limit==infinity orelse (is_integer(Limit) andalso Limit >= 0) ->
    Table = table_name(Table0),
    DbMod:prefix_match(Db, Table, Prefix, Limit).

prefix_match_rel(#kvdb_ref{mod = DbMod, db = Db}, Table0, Prefix, Start, Limit)
  when Limit==infinity orelse (is_integer(Limit) andalso Limit >= 0) ->
    Table = table_name(Table0),
    DbMod:prefix_match_rel(Db, Table, Prefix, Start, Limit).

select(#kvdb_ref{mod = DbMod, db = Db}, Table0, MatchSpec, Limit) ->
    Table = table_name(Table0),
    MSC = ets:match_spec_compile(MatchSpec),
    Encoding = DbMod:info(Db, encoding),
    {Prefix, Conv} = ms2pfx(MatchSpec, Encoding),
    select_(DbMod:prefix_match(Db,Table,Prefix,Limit),
	       Conv, MSC, [], Limit, Limit).

%% queue_delete_event(#kvdb_ref{mod = DbMod, db = Db} = DbRef, Table, Key) ->
%%     queue_delete_event(DbRef, Table, DbMod:info(Db, {Table, type}), Key).

queue_delete_event(#kvdb_ref{mod = DbMod,
			     db = Db} = DbRef, Table, _Type,
		   #q_key{queue = Q} = QKey) ->
    %% Enc = DbMod:info(Db, {Table,encoding}),
    IsEmpty = DbMod:is_queue_empty(Db, Table, Q),
    on_update({q_op,extract,Q,IsEmpty}, DbRef, Table, QKey).


%% We must create a prefix for the prefix_match().
%% This is a problem if we have raw encoding on the key, since you cannot have
%% a wildcard tail on a binary. You can do this on a list, however, so we allow
%% the caller to provide a string pattern on the key - enabling the declaration
%% of a prefix like "foo" ++ '_'.
%%
%% Unfortunately, this conflicts with match_spec_run(): we must convert the
%% results from prefix_match(), changing from binaries to lists, then revert
%% back to binaries on the objects that match. This is wasteful, but presumably
%% faster than setting the prefix to <<>> (the empty binary), forcing select()
%% to traverse the entire table.
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


select_(done, _, _, Acc, _, _) ->
    {lists:concat(lists:reverse(Acc)), fun() -> done end};
select_({Objs, Cont}, Conv, MSC, Acc, Limit, Limit0) ->
    Matches = revert(Conv, ets:match_spec_run(convert(Conv, Objs), MSC)),
    N = length(Matches),
    NewAcc = [Matches | Acc],
    case decr(Limit, N) of
	NewLimit when NewLimit =< 0 ->
	    %% This can result in (> Limit) objects being returned to the caller
	    {lists:concat(lists:reverse(NewAcc)),
	     fun() ->
		     select_(Cont(), Conv, MSC, NewAcc, Limit0, Limit0)
	     end};
	NewLimit when NewLimit > 0 ->
	    select_(Cont(), Conv, MSC, NewAcc, NewLimit, Limit0)
    end.

decr(infinity,_) ->
    infinity;
decr(Limit, N) when is_integer(Limit), is_integer(N) ->
    Limit - N.

fix_attrs(As) ->
    %% Treat the list of attributes as a proplist. This means there can be
    %% duplicates. Return an orddict, where values from the head of the list
    %% take priority over values from tail.
    lists:foldr(fun({K,V}, Acc) when is_atom(K) ->
			orddict:store(K, V, Acc);
		   (A, _) ->
			error({bad_attribute, A})
		end, orddict:new(), As).


if_table(DbMod, Db, Table, F) ->
    case DbMod:is_table(Db, Table) of
	true ->
	    F();
	false ->
	    error({no_such_table, Table})
    end.


%% on_update(Event, #kvdb_ref{} = DbRef, Table, Info) ->
%%     kvdb_lib:log(DbRef, Event, Table, Info),
%%     kvdb_trans:on_update(Event, DbRef, Table, Info).
