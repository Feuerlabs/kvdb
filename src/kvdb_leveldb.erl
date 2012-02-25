%%% @author Tony Rogvall <tony@rogvall.se>
%%% @copyright (C) 2011, Tony Rogvall
%%% @doc
%%%    LevelDB backend to kvdb
%%% @end
%%% Created : 29 Dec 2011 by Tony Rogvall <tony@rogvall.se>

-module(kvdb_leveldb).

-behaviour(kvdb).

-export([open/2, close/1]).
-export([add_table/3, delete_table/2, list_tables/1]).
-export([put/3, push/4, get/3, pop/3, extract/3, delete/3, list_queue/3]).
-export([first/2, last/2, next/3, prev/3, prefix_match/3, prefix_match/4]).

-import(kvdb_lib, [dec/3, enc/3]).

-include("kvdb.hrl").

open(Db0, Options) ->
    Db = make_string(Db0),
    E = proplists:get_value(encoding, Options, sext),
    kvdb_lib:check_valid_encoding(E),
    DbOpts = proplists:get_value(db_opts, Options, [{create_if_missing,true}]),
    Res = case proplists:get_value(file, Options) of
	      undefined ->
		  eleveldb:open(atom_to_list(Db)++".db", DbOpts);
	      Name ->
		  eleveldb:open(Name, DbOpts)
	  end,
    case Res of
	{ok, Ref} ->
	    {ok, ensure_schema(#db{ref = Ref, encoding = E})};
	Error ->
	    Error
    end.

make_string(A) when is_atom(A) ->
    atom_to_list(A);
make_string(S) when is_list(S) ->
    try binary_to_list(iolist_to_binary([S]))
    catch
	error:_ ->
	    lists:flatten(io_lib:fwrite("~w",[S]))
    end;
make_string(B) when is_binary(B) ->
    binary_to_list(B);
make_string(X) ->
    lists:flatten(io_lib:fwrite("~w", [X])).




close(_Db) ->
    %% leveldb is garbage collected
    ok.

add_table(#db{encoding = Enc} = Db, Table, Opts) ->
    TabR = check_options(Opts, Db, #table{name = Table, encoding = Enc}),
    case schema_lookup(Db, {table, Table}, undefined) of
	T when T =/= undefined ->
	    ok;
	undefined ->
	    case do_add_table(Db, Table, Opts) of
		ok ->
		    schema_write(Db, {{table, Table}, TabR}),
		    schema_write(Db, {{Table, encoding}, TabR#table.encoding}),
		    schema_write(Db, {{Table, type}, TabR#table.type}),
		    ok;
		Error ->
		    Error
	    end
    end.

do_add_table(#db{ref = Db}, Table, _Opts) ->
    T = make_table_key(Table, <<>>),
%    eleveldb:put(Db, <<"table:", (atom_to_binary(Table,latin1))/binary>>, <<>>, []),
    eleveldb:put(Db, T, <<>>, []).

list_tables(#db{metadata = ETS}) ->
    ets:select(ETS, [{ {{table, '$1'}, '_'}, [{'=/=','$1',?SCHEMA_TABLE}], ['$1'] }]).

delete_table(#db{ref = Ref} = Db, Table) ->
    case schema_lookup(Db, {table, Table}, undefined) of
	undefined ->
	    ok;
	#table{} ->
	    T = make_table_key(Table, <<>>),
	    with_iterator(
	      Ref,
	      fun(I) ->
		      delete_table_(eleveldb:iterator_move(I, T), I, T, byte_size(T), Ref)
	      end),
	    schema_delete(Db, {table, Table}),
	    schema_delete(Db, {Table, encoding}),
	    ok
    end.

delete_table_({ok, K, _V}, I, T, Sz, Ref) ->
    case K of
	<<T:Sz/binary, _/binary>> ->
	    eleveldb:delete(Ref, K, []),
	    delete_table_(eleveldb:iterator_move(I, next), I, T, Sz, Ref);
	_ ->
	    ok
    end;
delete_table_({error,invalid_iterator}, _, _, _, _) ->
    ok.


put(#db{ref = Ref} = Db, Table, Obj) ->
    Enc = encoding(Db, Table),
    {Key, Attrs, Value} = encode_obj(Enc, Obj),
    put_attrs(Db, Table, Key, Attrs),
    case eleveldb:put(Ref, make_table_key(Table, Key), Value, []) of
	ok ->
	    ok;
	Other ->
	    Other
    end.

push(#db{ref = Ref} = Db, Table, Q, Obj) ->
    Type = type(Db, Table),
    if Type == fifo; Type == lifo ->
	    Enc = encoding(Db, Table),
	    ActualKey = kvdb_lib:actual_key(Enc, Q, element(1, Obj)),
	    {Key, Attrs, Value} = encode_obj(Enc, setelement(1, Obj, ActualKey)),
	    put_attrs(Db, Table, Key, Attrs),
	    case eleveldb:put(Ref, make_table_key(Table, Key), Value, []) of
		ok ->
		    {ok, ActualKey};
		Other ->
		    Other
	    end;
       true ->
	    error(illegal)
    end.


%% this functionality could be lifted up to kvdb.erl. There is no specific code, except
%% for type/2 and encoding/2, and they should be generalized to Mod:table_info/2.
pop(#db{} = Db, Table, Q) ->
    Type = type(Db, Table),
    Enc = encoding(Db, Table),
    case case Type of
	     fifo -> q_first(Db, Table, Q, Enc);
	     lifo -> q_last(Db, Table, Q, Enc)
	 end of
	{ok, Obj} ->
	    K = element(1, Obj),
	    delete(Db, Table, K),
	    {ok, fix_q_obj(Obj, Enc)};
	done ->
	    done;
	{error, _} = Error ->
	    Error
    end.

extract(#db{} = Db, Table, Key) ->
    Type = type(Db, Table),
    case get(Db, Table, Key) of
	{ok, Obj} ->
	    delete(Db, Table, Key),
	    case Type of
		_ when Type==fifo; Type==lifo ->
		    {ok, fix_q_obj(Obj, encoding(Db, Table))};
		set ->
		    {ok, Obj}
	    end;
	{error, _} = Error ->
	    Error
    end.

fix_q_obj(Obj, Enc) ->
    K = element(1, Obj),
    K1 = kvdb_lib:split_queue_key(Enc, K),
    setelement(1, Obj, K1).

q_first(#db{ref = Ref} = Db, Table, Q, Enc) ->
    with_iterator(Ref, fun(I) -> q_first_(I, Db, Table, Q, Enc) end).

q_first_(I, Db, Table, Q, Enc) ->
    QPfx = kvdb_lib:queue_prefix(Enc, Q, first),
    Prefix = make_table_key(Table, kvdb_lib:enc(key, QPfx, Enc)),
    QPrefix = table_queue_prefix(Table, Q, Enc),
    Sz = byte_size(QPrefix),
    TPrefix = make_table_key(Table),
    TPSz = byte_size(TPrefix),
    case eleveldb:iterator_move(I, Prefix) of
	{ok, <<QPrefix:Sz/binary, _/binary>> = K, V} ->
	    <<TPrefix:TPSz/binary, Key/binary>> = K,
	    case Key of
		<<>> -> done;
		_ ->
		    {ok, decode_obj(Db, Enc, Table, Key, V)}
	    end;
	_ ->
	    done
    end.

q_last(#db{ref = Ref} = Db, Table, Q, Enc) ->
    with_iterator(Ref, fun(I) -> q_last_(I, Db, Table, Q, Enc) end).

q_last_(I, Db, Table, Q, Enc) ->
    QPfx = kvdb_lib:queue_prefix(Enc, Q, last),
    Prefix = make_table_key(Table, kvdb_lib:enc(key, QPfx, Enc)),
    QPrefix = table_queue_prefix(Table, Q, Enc),
    Sz = byte_size(QPrefix),
    TPrefix = make_table_key(Table),
    TPSz = byte_size(TPrefix),
    case eleveldb:iterator_move(I, Prefix) of
	{ok, _K, _V} ->
	    case eleveldb:iterator_move(I, prev) of
		{ok, <<QPrefix:Sz/binary, _/binary>> = K1, V1} ->
		    <<TPrefix:TPSz/binary, Key/binary>> = K1,
		    case Key of
			<<>> -> done;
			_ ->
			    {ok, decode_obj(Db, Enc, Table, Key, V1)}
		    end;
		_Other ->
		    done
	    end;
	{error, invalid_iterator} ->
	    %% likely stepped outside entire leveldb database
	    case eleveldb:iterator_move(I, last) of
		{ok, <<QPrefix:Sz/binary, _/binary>> = K, V} ->
		    <<TPrefix:TPSz/binary, Key/binary>> = K,
		    case Key of
			<<>> -> done;
			_ ->
			    {ok, decode_obj(Db, Enc, Table, Key, V)}
		    end;
		_ ->
		    done
	    end
    end.

list_queue(#db{ref = Ref} = Db, Table, Q) ->
    Type = type(Db, Table),
    Enc = encoding(Db, Table),
    QPrefix = table_queue_prefix(Table, Q, Enc),
    TPrefix = make_table_key(Table),
    with_iterator(
      Ref,
      fun(I) ->
	      First = case Type of
			  fifo -> q_last_(I, Db, Table, Q, Enc);
			  lifo -> q_first_(I, Db, Table, Q, Enc)
		      end,
	      q_all_(First, I, Db, Table, q_all_dir(Type), Enc, QPrefix, TPrefix, [])
      end).

q_all_dir(fifo) -> prev;
q_all_dir(lifo) -> next.

q_all_({ok, Obj}, I, Db, Table, Dir, Enc, QPrefix, TPrefix, Acc) ->
    Acc1 = [fix_q_obj(Obj, Enc)|Acc],
    QSz = byte_size(QPrefix),
    TSz = byte_size(TPrefix),
    case eleveldb:iterator_move(I, Dir) of
	{ok, <<QPrefix:QSz/binary, _/binary>> = K, V} ->
	    <<TPrefix:TSz/binary, Key/binary>> = K,
	    q_all_({ok, decode_obj(Db, Enc, Table, Key, V)},
		   I, Db, Table, Dir, Enc, QPrefix, TPrefix, Acc1);
	_ ->
	    q_all_(done, I, Db, Table, Dir, Enc, QPrefix, TPrefix, Acc1)
    end;
q_all_(done, _, _, _, _, _, _, _, Acc) ->
    Acc.


table_queue_prefix(Table, Q, Enc) when Enc == raw; element(1, Enc) == raw ->
    make_table_key(Table, Q);
table_queue_prefix(Table, Q, Enc) when Enc == sext; element(1, Enc) == sext ->
    make_table_key(Table, sext:prefix({Q,'_','_'})).


get(#db{ref = Ref} = Db, Table, Key) ->
    Enc = encoding(Db, Table),
    EncKey = enc(key, Key, Enc),
    case eleveldb:get(Ref, make_table_key(Table, EncKey), []) of
	{ok, V} ->
	    {ok, decode_obj_v(Db, Enc, Table, Key, V)};
	not_found ->
	    {error, not_found};
	{error, _} = Error ->
	    Error
    end.

delete(#db{ref = Ref} = Db, Table, Key) ->
    Enc = encoding(Db, Table),
    EncKey = enc(key, Key, Enc),
    delete_attrs(Db, Table, EncKey),
    eleveldb:delete(Ref, make_table_key(Table, EncKey), []).

put_attrs(#db{ref = Ref} = Db, Table, EncKey, Attrs) when is_list(Attrs) ->
    case encoding(Db, Table) of
	{_, _, _} ->
	    [eleveldb:put(Ref, make_key(Table, $=, <<EncKey/binary,
						     (sext:encode(K))/binary>>),
			  term_to_binary(V), []) || {K, V} <- Attrs];
	_ ->
	    error(badarg)
    end;
put_attrs(_, _, _, none) ->
    ok.


get_attrs(#db{ref = Ref} = Db, Table, Key) ->
    case encoding(Db, Table) of
	{_, _, _} ->
	    EncKey = sext:encode(Key),
	    TableKey = make_key(Table, $=, EncKey),
	    with_iterator(
	      Ref,
	      fun(I) ->
		      get_attrs_(prefix_move(I, TableKey, TableKey), I, TableKey)
	      end);
	_ ->
	    error(badarg)
    end.

get_attrs_({ok, K, V}, I, Prefix) ->
    [{sext:decode(K), binary_to_term(V)}|
     get_attrs_(prefix_move(I, Prefix, next), I, Prefix)];
get_attrs_(done, _, _) ->
   [].

delete_attrs(#db{ref = Ref} = Db, Table, Key) ->
    case encoding(Db, Table) of
	{_, _, _} = Enc ->
	    EncKey = enc(key, Key, Enc),
	    TableKey = make_key(Table, $=, EncKey),
	    with_iterator(
	      Ref,
	      fun(I) ->
		      delete_attrs_(eleveldb:iterator_move(I, TableKey), I, TableKey, Ref)
	      end);
	_ ->
	    ok
    end.

delete_attrs_({ok, K, _V}, I, Prefix, Ref) ->
    Sz = byte_size(Prefix),
    case K of
	<<Prefix:Sz/binary, _/binary>> ->
	    eleveldb:delete(Ref, K),
	    delete_attrs_(eleveldb:iterator_move(I, Prefix, next), I, Prefix, Ref);
	_ ->
	    ok
    end;
delete_attrs_(done, _, _, _) ->
    ok.


prefix_match(Db, Table, Prefix) ->
    prefix_match(Db, Table, Prefix, 100).

prefix_match(#db{ref = Ref} = Db, Table, Prefix, Limit)
  when (is_integer(Limit) orelse Limit == infinity) ->
    Enc = encoding(Db, Table),
    EncPrefix = kvdb_lib:enc_prefix(key, Prefix, Enc),
    TablePrefix = make_table_key(Table),
    TabPfxSz = byte_size(TablePrefix),
    MatchKey = make_table_key(Table, EncPrefix),
    with_iterator(
      Ref,
      fun(I) ->
	      if EncPrefix == <<>> ->
		      case eleveldb:iterator_move(I, TablePrefix) of
			  {ok, <<TablePrefix:TabPfxSz/binary>>, _} ->
			      prefix_match_(I, next, Db, Table, MatchKey, TablePrefix,
					    Prefix, Enc, Limit, Limit, []);
			  {error, _} ->
			      done
		      end;
		 true ->
		      prefix_match_(I, MatchKey, Db, Table, MatchKey, TablePrefix,
				    Prefix, Enc, Limit, Limit, [])
	      end
      end).

prefix_match_(_I, Next, #db{ref = Ref} = Db, Table, MatchKey, TPfx, Pfx,
	      Enc, 0, Limit0, Acc) ->
    {lists:reverse(Acc),
     fun() ->
	     with_iterator(
	       Ref,
	       fun(I1) ->
		       prefix_match_(I1, Next, Db, Table, MatchKey, TPfx, Pfx,
				     Enc, Limit0, Limit0, [])
	       end)
     end};
prefix_match_(I, Next, Db, Table, MatchKey, TPfx, Pfx, Enc, Limit, Limit0, Acc) ->
    Sz = byte_size(MatchKey),
    case eleveldb:iterator_move(I, Next) of
	{ok, <<MatchKey:Sz/binary, _/binary>> = Key, Val} ->
	    PSz = byte_size(TPfx),
	    <<TPfx:PSz/binary, K/binary>> = Key,
	    case (K =/= <<>> andalso kvdb_lib:is_prefix(Pfx, K, Enc)) of
		true ->
		    prefix_match_(I, next, Db, Table, MatchKey, TPfx, Pfx,
				  Enc, decr(Limit), Limit0,
				  [decode_obj(Db, Enc, Table, K, Val) | Acc]);
		false ->
		    {lists:reverse(Acc), fun() -> done end}
	    end;
	_ ->
	    %% prefix doesn't match, or end of database
	    {lists:reverse(Acc), fun() ->
					 done
				 end}
    end.


decr(infinity) -> infinity;
decr(I) when is_integer(I) -> I-1.


first(#db{} = Db, Table) ->
    first(Db, encoding(Db, Table), Table).

first(#db{ref = Ref} = Db, Enc, Table) ->
    TableKey = make_table_key(Table),
    with_iterator(
      Ref,
      fun(I) ->
	      case prefix_move(I, TableKey, TableKey) of
		  {ok, <<>>, _} ->
		      case prefix_move(I, TableKey, next) of
			  {ok, K, V} ->
			      {ok, decode_obj(Db, Enc, Table, K, V)};
			  done ->
			      done
		      end;
		  _ ->
		      done
	      end
      end).

prefix_move(I, Prefix, Dir) ->
    Sz = byte_size(Prefix),
    case eleveldb:iterator_move(I, Dir) of
	{ok, <<Prefix:Sz/binary, K/binary>>, Value} ->
	    {ok, K, Value};
	_ ->
	    done
    end.

last(#db{} = Db, Table) ->
    last(Db, encoding(Db, Table), Table).

last(#db{ref = Ref} = Db, Enc, Table) ->
    FirstKey = make_table_key(Table),
    FirstSize = byte_size(FirstKey),
    LastKey = make_table_last_key(Table), % isn't actually stored in the database
    with_iterator(
      Ref,
      fun(I) ->
	      case eleveldb:iterator_move(I, LastKey) of
		  {ok,_AfterKey,_} ->
		      case eleveldb:iterator_move(I, prev) of
			  {ok, FirstKey, _} ->
			      % table is empty
			      done;
			  {ok, << FirstKey:FirstSize/binary, K/binary>>, Value} ->
			      {ok, decode_obj(Db, Enc, Table, K, Value)};
			  _ ->
			      done
		      end;
		  {error,invalid_iterator} ->
		      %% the last object of this table is likely the very last in the db
		      case eleveldb:iterator_move(I, last) of
			  {ok, FirstKey, _} ->
			      %% table is empty
			      done;
			  {ok, << FirstKey:FirstSize/binary, K/binary >>, Value} ->
			      {ok, decode_obj(Db, Enc, Table, K, Value)}
		      end;
		  _ ->
		      done
	      end
      end).

next(#db{} = Db, Table, Rel) ->
    next(Db, encoding(Db, Table), Table, Rel).

next(Db, Enc, Table, Rel) ->
    iterator_move(Db, Enc, Table, Rel, fun(A,B) -> A > B end, next).

prev(#db{} = Db, Table, Rel) ->
    prev(Db, encoding(Db, Table), Table, Rel).

prev(Db, Enc, Table, Rel) ->
    try iterator_move(Db, Enc, Table, Rel, fun(A,B) -> A < B end, prev) of
	{ok, {<<>>, _}} ->
	    done;
	Other ->
	    Other
    catch
	error:Err ->
	    io:fwrite("CRASH: ~p, ~p~n", [Err, erlang:get_stacktrace()]),
	    error(Err)
    end.

iterator_move(#db{ref = Ref} = Db, Enc, Table, Rel, Comp, Dir) ->
    TableKey = make_table_key(Table),
    KeySize = byte_size(TableKey),
    EncRel = enc(key, Rel, Enc),
    RelKey = make_table_key(Table, EncRel),
    with_iterator(
      Ref,
      fun(I) ->
	      case eleveldb:iterator_move(I, RelKey) of
		  {ok, <<TableKey:KeySize/binary, Key/binary>>, Value} ->
		      case Key =/= <<>> andalso Comp(Key, EncRel) of
			  true ->
			      {ok, decode_obj(Db, Enc, Table, Key, Value)};
			  false ->
			      case eleveldb:iterator_move(I, Dir) of
				  {ok, <<TableKey:KeySize/binary, Key2/binary>>, Value2} ->
				      case Key2 of
					  <<>> -> done;
					  _ ->
					      {ok, decode_obj(Db, Enc, Table, Key2, Value2)}
				      end;
				  _ ->
				      done
			      end
		      end;
		  _ ->
		      done
	      end
      end).

%% create key
make_table_last_key(Table) ->
    make_key(Table, $;, <<>>).

make_table_key(Table) ->
    make_key(Table, $:, <<>>).

make_table_key(Table, Key) ->
    make_key(Table, $:, Key).

make_key(Table, Sep, Key) when is_binary(Table) ->
    <<Table/binary,Sep,Key/binary>>.

encode_obj({_,_,_} = Enc, {Key, Attrs, Value}) ->
    {enc(key, Key, Enc), Attrs, enc(value, Value, Enc)};
encode_obj(Enc, {Key, Value}) ->
    {enc(key, Key, Enc), none, enc(value, Value, Enc)}.


decode_obj(Db, Enc, Table, K, V) ->
    Key = dec(key, K, Enc),
    decode_obj_v(Db, Enc, Table, Key, V).

decode_obj_v(Db, Enc, Table, Key, V) ->
    Value = dec(value, V, Enc),
    case Enc of
	{_, _, _} ->
	    Attrs = get_attrs(Db, Table, Key),
	    {Key, Attrs, Value};
	_ ->
	    {Key, Value}
    end.


with_iterator(Db, F) ->
    {ok, I} = eleveldb:iterator(Db, []),
    try F(I)
    after
        eleveldb:iterator_close(I)
    end.


type(Db, Table) ->
    schema_lookup(Db, {Table, type}, set).

encoding(#db{encoding = Enc} = Db, Table) ->
    schema_lookup(Db, {Table, encoding}, Enc).


check_options([{type, T}|Tl], Db, Rec) when T==set; T==fifo; T==lifo ->
    check_options(Tl, Db, Rec#table{type = T});
check_options([{encoding, E}|Tl], Db, Rec) ->
    Rec1 = Rec#table{encoding = E},
    kvdb_lib:check_valid_encoding(E),
    check_options(Tl, Db, Rec1);
check_options([], _, Rec) ->
    Rec.

ensure_schema(#db{ref = Ref} = Db) ->
    ETS = ets:new(kvdb_schema, [ordered_set]),
    Db1 = Db#db{metadata = ETS},
    case eleveldb:get(Ref, make_table_key(?SCHEMA_TABLE, <<>>), []) of
	{ok, _} ->
	    [ets:insert(ETS, X) || X <- whole_table(Db1, sext, ?SCHEMA_TABLE)],
	    Db1;
	_ ->
	    ok = do_add_table(Db1, ?SCHEMA_TABLE, []),
	    Tab = #table{name = ?SCHEMA_TABLE, encoding = sext, columns = [key,value]},
	    schema_write(Db1, {{table, ?SCHEMA_TABLE}, Tab}),
	    schema_write(Db1, {{?SCHEMA_TABLE, encoding}, sext}),
	    schema_write(Db1, {{?SCHEMA_TABLE, type}, set}),
	    Db1
    end.

whole_table(Db, Enc, Table) ->
    whole_table(first(Db, Enc, Table), Db, Enc, Table).

whole_table({ok, {K, V}}, Db, Enc, Table) ->
    [{K,V} | whole_table(next(Db, Enc, Table, K), Db, Enc, Table)];
whole_table(done, _Db, _Enc, _Table) ->
    [].

schema_write(#db{metadata = ETS} = Db, Item) ->
    ets:insert(ETS, Item),
    put(Db, ?SCHEMA_TABLE, Item).

schema_lookup(_, {?SCHEMA_TABLE, Attr}, Default) ->
    case Attr of
	type -> set;
	encoding -> sext;
	_ -> Default
    end;
schema_lookup(#db{metadata = ETS}, Key, Default) ->
    case ets:lookup(ETS, Key) of
	[{_, Value}] ->
	    Value;
	[] ->
	    Default
    end.

schema_delete(#db{metadata = ETS} = Db, Key) ->
    ets:delete(ETS, Key),
    delete(Db, ?SCHEMA_TABLE, Key).
