%%%---- BEGIN COPYRIGHT -------------------------------------------------------
%%%
%%% Copyright (C) 2012 Feuerlabs, Inc. All rights reserved.
%%%
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at http://mozilla.org/MPL/2.0/.
%%%
%%%---- END COPYRIGHT ---------------------------------------------------------
%%% @author Tony Rogvall <tony@rogvall.se>
%%% @doc
%%%    LevelDB backend to kvdb
%%% @end

-module(kvdb_leveldb).

-behaviour(kvdb).

-export([open/2, close/1]).
-export([add_table/3, delete_table/2, list_tables/1]).
-export([put/3, push/4, get/3, get_attrs/4, index_get/4, index_keys/4,
	 update_counter/4, pop/3, prel_pop/3, extract/3, delete/3,
	 list_queue/3, list_queue/6, is_queue_empty/3,
	 queue_read/3, queue_insert/5, queue_delete/3, mark_queue_object/4]).
-export([first_queue/2, next_queue/3]).
-export([first/2, last/2, next/3, prev/3,
	 prefix_match/3, prefix_match/4, prefix_match_rel/5]).
-export([get_schema_mod/2]).
-export([info/2, is_table/2]).

-export([dump_tables/1]).

-import(kvdb_lib, [dec/3, enc/3]).

-include("kvdb.hrl").

-define(if_table(Db, Tab, Expr), if_table(Db, Tab, fun() -> Expr end)).

info(#db{} = Db, What) ->
    case What of
	tables   -> list_tables(Db);
	encoding -> Db#db.encoding;
	ref      -> Db#db.ref;
	{Tab,encoding} -> ?if_table(Db, Tab, encoding(Db, Tab));
	{Tab,index   } -> ?if_table(Db, Tab, index(Db, Tab));
	{Tab,type    } -> ?if_table(Db, Tab, type(Db, Tab));
	{Tab,schema  } -> ?if_table(Db, Tab, schema(Db, Tab));
	{Tab,tabrec  } -> schema_lookup(Db, {table, Tab}, undefined);
	_ -> undefined
    end.

is_table(#db{metadata = ETS}, Tab) ->
    ets:member(ETS, {table, Tab}).

if_table(Db, Tab, F) ->
    case is_table(Db, Tab) of
	true -> F();
	false -> undefined
    end.


dump_tables(#db{ref = Ref} = Db) ->
    with_iterator(
      Ref,
      fun(I) ->
	      dump_tables_(eleveldb:iterator_move(I, first), I, Db)
      end).

dump_tables_({ok, K, V}, I, Db) ->
    Type = bin_match([{$:, obj}, {$=, attr}, {$?, index}], K),
    io:fwrite("Type = ~p: K=~p; V=~p~n", [Type, K, V]),
    Obj = case Type of
	      obj ->
		  [T, Key] = binary:split(K, <<":">>),
		  case Key of
		      <<>> ->
			  {tab_obj, T, kvdb_lib:try_decode(V)};
		      _ ->
			  Enc = encoding(Db, T),
			  case type(Db, T) of
			      set ->
				  {obj, T, kvdb_lib:try_decode(Key),
				   kvdb_lib:try_decode(V)};
			      TType when TType == fifo; TType == lifo;
					 element(2, TType) == fifo;
					 element(2, TType) == lifo ->
				  %% io:fwrite("split_queue_key(~p, ~p, ~p)~n",
				  %% 	    [Enc, TType, Key]),
				  #q_key{queue = Q,
					 key = Ko} = kvdb_lib:split_queue_key(
					      Enc, TType, Key),
				  <<F:8, Val/binary>> = V,
				  St = case F of
					   $* -> blocking;
					   $+ -> active;
					   $- -> inactive
				       end,
				  Kr = dec(key, Key, Enc),
				  {q_obj, T, Q, Kr,
				   {Ko, kvdb_lib:try_decode(Val)}, St}
			  end
		  end;
	      attr ->
		  [T, AKey] = binary:split(K, <<"=">>),
		  %% io:fwrite("attr: T=~p; AKey=~p~n", [T, AKey]),
		  %% {OKey, Rest} = sext:decode_next(AKey),
		  %% Attr = sext:decode(Rest),
		  {OKey, Attr} = decode_attr_key(Db, T, AKey),
		  {attr, T, OKey, Attr, binary_to_term(V)};
	      index ->
		  [T, IKey] = binary:split(K, <<"?">>),
		  {Ix, Rest} = sext:decode_next(IKey),
		  %% io:fwrite("index: T=~p; Ix =~p; Rest =~p~n", [T,Ix,Rest]),
		  {index, T, Ix, sext:decode(Rest), V};
	      unknown ->
		  {unknown, K, V}
	  end,
    [Obj | dump_tables_(eleveldb:iterator_move(I, next), I, Db)];
dump_tables_({error, _}, _, _) ->
    [];
dump_tables_(Other, _, _) ->
    io:fwrite("dump_tables_(~p, _, _)~n", [Other]).

%% <<Key/binary, (sext:encode(AttrName))/binary>>
decode_attr_key(Db, Tab, K) ->
    case key_encoding(Db, Tab) of
	sext ->
	    [Kdec, Rest] = sext:decode_next(K),
	    {Kdec, sext:decode(Rest)};
	raw ->
	    %% This is the hard part
	    %% since the first part is an arbitrary binary, we must guess where
	    %% the sext-encoded attribute name starts. We know what the start
	    %% codes of a sext-encoded term can be. Try each, in order.
	    {match, Ps} =
		re:run(K,<<"[",8,9,10,11,12,13,14,15,16,17,18,19,"]">>,
		       [global]),
	    {P, AName} = try_sext_decode(lists:flatten(Ps), K),
	    <<Key:P/binary, _/binary>> = K,
	    {Key, AName}
    end.

key_encoding(Db, T) ->
    case encoding(Db, T) of
	E when is_atom(E) -> E;
	E when is_tuple(E) -> element(1, E)
    end.

try_sext_decode([{P,_}|Ps], K) ->
    <<_:P/binary, Rest/binary>> = K,
    try   A = sext:decode(Rest),
	  {P, A}
    catch
	error:_ ->
	    try_sext_decode(Ps, K)
    end.



bin_match(Ts, B) ->
    Cs = << << "\\", C:8 >> || {C,_} <- Ts >>,
    Pat = << "[", Cs/binary, "]" >>,
    case re:run(B, Pat, [{capture, first, list}]) of
	{match, [[C]]} ->
	    {_,T} = lists:keyfind(C,1,Ts),
	    T;
	_ ->
	    unknown
    end.

get_schema_mod(Db, Default) ->
    case schema_lookup(Db, schema_mod, undefined) of
	undefined ->
	    schema_write(Db, {schema_mod, Default}),
	    Default;
	M ->
	    M
    end.

open(DbName, Options) ->
    E = proplists:get_value(encoding, Options, sext),
    kvdb_lib:check_valid_encoding(E),
    DbOpts = proplists:get_value(db_opts, Options, [{create_if_missing,true}]),
    Res = case proplists:get_value(file, Options) of
	      undefined ->
		  NameStr = kvdb_lib:good_string(DbName),
		  File = NameStr ++ ".db",
		  filelib:ensure_dir(File),
		  eleveldb:open(File, DbOpts);
	      Name ->
		  filelib:ensure_dir(Name),
		  eleveldb:open(Name, DbOpts)
	  end,
    case Res of
	{ok, Ref} ->
	    {ok, ensure_schema(#db{ref = Ref, encoding = E}, Options)};
	Error ->
	    Error
    end.

%% make_string(A) when is_atom(A) ->
%%     atom_to_list(A);
%% make_string(S) when is_list(S) ->
%%     try binary_to_list(iolist_to_binary([S]))
%%     catch
%% 	error:_ ->
%% 	    lists:flatten(io_lib:fwrite("~w",[S]))
%%     end;
%% make_string(B) when is_binary(B) ->
%%     binary_to_list(B);
%% make_string(X) ->
%%     lists:flatten(io_lib:fwrite("~w", [X])).




close(_Db) ->
    %% leveldb is garbage collected
    ok.

add_table(#db{encoding = Enc} = Db, Table, Opts) when is_list(Opts) ->
    TabR = check_options(Opts, Db, #table{name = Table, encoding = Enc}),
    add_table(Db, Table, TabR);
add_table(Db, Table, #table{} = TabR) ->
    case schema_lookup(Db, {table, Table}, undefined) of
	T when T =/= undefined ->
	    ok;
	undefined ->
	    case do_add_table(Db, Table) of
		ok ->
		    schema_write(Db, {{table, Table}, TabR}),
		    schema_write(Db, {{a, Table, encoding}, TabR#table.encoding}),
		    schema_write(Db, {{a, Table, type}, TabR#table.type}),
		    schema_write(Db, {{a, Table, index}, TabR#table.index}),
		    ok;
		Error ->
		    Error
	    end
    end.

do_add_table(#db{ref = Db}, Table) ->
    T = make_table_key(Table, <<>>),
    eleveldb:put(Db, T, <<>>, []).

list_tables(#db{metadata = ETS}) ->
    ets:select(ETS, [{ {{table, '$1'}, '_'},
		       [{'=/=','$1',?META_TABLE}], ['$1'] }]).

delete_table(#db{ref = Ref} = Db, Table) ->
    case schema_lookup(Db, {table, Table}, undefined) of
	undefined ->
	    ok;
	#table{} ->
	    Kt = make_table_key(Table, <<>>),
	    Ka = make_key(Table, $=, <<>>),
	    Ki = make_key(Table, $?, <<>>),
	    with_iterator(
	      Ref,
	      fun(I) ->
		      delete_table_(eleveldb:iterator_move(I, Kt),
				    I, Kt,Ka,Ki, byte_size(Kt), Ref)
	      end),
	    schema_delete(Db, {table, Table}),
	    schema_delete(Db, {Table, encoding}),
	    ok
    end.

delete_table_({ok, K, _V}, I, Kt,Ka,Ki, Sz, Ref) ->
    case K of
	<<X:Sz/binary, _/binary>> when X==Kt; X==Ka; X==Ki ->
	    eleveldb:delete(Ref, K, []),
	    delete_table_(eleveldb:iterator_move(I, next), I, Kt,Ka,Ki, Sz, Ref);
	_ ->
	    ok
    end;
delete_table_({error,invalid_iterator}, _, _, _, _, _, _) ->
    ok.

put(Db, Table,Obj) ->
    case type(Db, Table) of
	set ->
	    put_(Db, Table, Obj, put);
	_ ->
	    {error, illegal}
    end.

put_(#db{ref = Ref} = Db, Table, {K,V}, Op) ->
    %% Frequently used case, therefore optimized. No indexing on {K,V} tuples
    Enc = encoding(Db, Table),
    Type = type(Db, Table),
    Key = encode_elem(key, K, Type, Enc, Op),
    Val = encode_elem(value, V, Type, Enc, Op),
    eleveldb:put(Ref, make_table_key(Table, Key), Val, []);
put_(#db{ref = Ref} = Db, Table, {K, Attrs, V}, Op) ->
    Enc = encoding(Db, Table),
    Ix = index(Db, Table),
    Type = type(Db, Table),
    Key = encode_elem(key, K, Type, Enc, Op),
    Val = encode_elem(value, V, Type, Enc, Op),
    OldAttrs = get_attrs_(Db, Table, Key, all),
    IxOps = case Ix of
		[_|_] ->
		    OldIxVals = kvdb_lib:index_vals(
				  Ix, K, OldAttrs,
				  fun() ->
					  get_value(Db, Table, K)
				  end),
		    NewIxVals = kvdb_lib:index_vals(Ix, K, Attrs,
						    fun() -> V end),
		    [{delete, ix_key(Table, I, K)} ||
			I <- OldIxVals -- NewIxVals]
			++ [{put, ix_key(Table, I, K), <<>>} ||
			       I <- NewIxVals -- OldIxVals];
		_ ->
		    []
	    end,
    DelAttrs = attrs_to_delete(
		 Table, Key,
		 [{A,Va} ||
		     {A,Va} <- OldAttrs,
		     not lists:keymember(A, 1, Attrs)]),
    PutAttrs = attrs_to_put(Table, Key, Attrs),
    case eleveldb:write(Ref, [{put, make_table_key(Table, Key), Val}|
			      DelAttrs ++ PutAttrs ++ IxOps], []) of
	ok ->
	    ok;
	Other ->
	    Other
    end.

encode_elem(_Elem, V, _T, _Enc, push) when is_binary(V) ->
    %% This should really be cleaned up, but for now, when put_(...) is called
    %% from push(...), the key and value parts are already coded as binary.
    V;
encode_elem(Elem, V, _, Enc, _) ->
    enc(Elem, V, Enc).


ix_key(Table, I, K) ->
    make_key(Table, $?, <<(sext:encode(I))/binary, (sext:encode(K))/binary>>).

update_counter(#db{ref = Ref} = Db, Table, K, Incr) when is_integer(Incr) ->
    case type(Db, Table) of
	set ->
	    Enc = encoding(Db, Table),
	    Key = enc(key, K, Enc),
	    case eleveldb:get(Ref, TabKey = make_table_key(Table, Key),
			      [{fill_cache, true}]) of
		{ok, V} ->
		    NewV =
			case dec(value, V, Enc) of
			    I when is_integer(I) ->
				NewI = I + Incr,
				enc(value, NewI, Enc);
			    B when is_binary(B) ->
				Sz = bit_size(B),
				<<I:Sz/integer>> = B,
				NewI = I + Incr,
				enc(value, <<NewI:Sz/integer>>, Enc);
			    _ ->
				error(illegal)
			end,
		    ok = eleveldb:put(Ref, TabKey, NewV, []),
		    dec(value, NewV, Enc);
		_ ->
		    error(not_found)
	    end;
	_ ->
	    error(illegal)
    end.


push(#db{} = Db, Table, Q, Obj) ->
    Type = type(Db, Table),
    if Type == fifo; Type == lifo; element(1,Type) == keyed ->
	    Enc = encoding(Db, Table),
	    {ActualKey, QKey} =
		kvdb_lib:actual_key(Enc, Type, Q, element(1, Obj)),
	    {Key, Attrs, Value} = encode_queue_obj(
				    Enc, setelement(1, Obj, ActualKey)),
	    case put_(Db, Table, {Key, Attrs, Value}, push) of
		ok ->
		    {ok, QKey};
		Other ->
		    Other
	    end;
	    %% PutAttrs = attrs_to_put(Table, Key, Attrs),
	    %% Put = {put, make_table_key(Table, Key), Value},
	    %% case eleveldb:write(Ref, [Put|PutAttrs], []) of
	    %% 	ok ->
	    %% 	    {ok, QKey};
	    %% 	Other ->
	    %% 	    Other
	    %% end;
       true ->
	    error(illegal)
    end.

queue_insert(#db{} = Db, Table, #q_key{} = QKey, St, Obj) when
      St==blocking; St==active; St==inactive ->
    Enc = encoding(Db, Table),
    Type = type(Db, Table),
    Key = kvdb_lib:q_key_to_actual(QKey, Enc, Type),
    Obj1 = setelement(1, Obj, Key),
    %% put_(Db, Table, Obj1, put).
    {EncKey, Attrs, Value} = encode_queue_obj(Enc, Obj1, St),
    put_(Db, Table, {EncKey, Attrs, Value}, push).
    %% PutAttrs = attrs_to_put(Table, EncKey, Attrs),
    %% Put = {put, make_table_key(Table, EncKey), Value},
    %% case eleveldb:write(Ref, [Put|PutAttrs], []) of
    %% 	ok -> ok;
    %% 	{error, Error} ->
    %% 	    error(Error)
    %% end.

queue_delete(Db, Table, #q_key{} = QKey) ->
    _ = extract(Db, Table, QKey),
    ok.

mark_queue_object(#db{} = Db, Table, #q_key{queue = Q} = QK, St) when
      St==blocking; St==active; St==inactive ->
    case queue_read(Db, Table, QK) of
	{ok, _OldSt, Obj} ->
	    mark_queue_obj(Db, Table, encoding(Db,Table), QK, Obj, St),
	    {ok, Q, Obj};
	{error,_} = E ->
	    E
    end.

mark_queue_obj(#db{ref = Ref} = Db, Table, Enc, QK, Obj, St) when
      St==blocking; St==active; St==inactive ->
    Type = type(Db, Table),
    AKey = kvdb_lib:q_key_to_actual(QK, Enc, Type),
    {Key, _Attrs, Value} = encode_queue_obj(Enc, setelement(1,Obj,AKey), St),
    eleveldb:put(Ref, make_table_key(Table, Key), Value, []).

pop(#db{} = Db, Table, Q) ->
    case type(Db, Table) of
	set -> error(illegal);
	T ->
	    Remove = fun(QKey, _Obj, _) ->
			     delete(Db, Table, QKey)
		     end,
	    do_pop(Db, Table, T, Q, Remove, false)
    end.

prel_pop(Db, Table, Q) ->
    case type(Db, Table) of
	set -> error(illegal);
	T ->
	    Remove = fun(QKey, Obj, Enc) ->
			     mark_queue_obj(Db, Table, Enc, QKey, Obj, blocking)
		     end,
	    do_pop(Db, Table, T, Q, Remove, true)
    end.

do_pop(Db, Table, _Type, Q, Remove, ReturnKey) ->
    Enc = encoding(Db, Table),
    case list_queue(Db, Table, Q, fun(inactive, _, _) ->
					  skip;
				     (_St,K,O) ->
					  {keep, {K,O}}
				  end, _HeedBlock = true, 2) of
	{[{QKey,Obj}|More], _} ->
	    %% Obj1 = fix_q_obj(Obj, Enc, Type),
	    Empty = More == [],
	    Remove(QKey, Obj, Enc),
	    if ReturnKey ->
		    {ok, Obj, QKey, Empty};
	       true ->
		    {ok, Obj, Empty}
	    end;
	Stop when Stop == done; Stop == blocked ->
	    Stop
    end.



extract(#db{ref = Ref} = Db, Table, #q_key{queue = Q, key = Key} = QKey) ->
    case type(Db, Table) of
	set -> error(illegal);
	Type ->
	    Enc = encoding(Db, Table),
	    AKey = kvdb_lib:q_key_to_actual(QKey, Enc, Type),
	    EncKey = enc(key, AKey, Enc),
	    RawKey = make_table_key(Table, EncKey),
	    case eleveldb:get(Ref, RawKey, []) of
		{ok, <<St:8, V/binary>>} when St==$*; St==$-; St==$+ ->
		    Obj = decode_obj_v(Db, Enc, Table, EncKey, Key, V),
		    eleveldb:delete(Ref, RawKey, []),
		    IsEmpty =
			case list_queue(Db, Table, Q,
					fun(_,_,O) ->
						{keep,O}
					end,
					_HeedBlock=true, 1) of
			    {[_], _} -> false;
			    _ -> true
			end,
		    {ok, setelement(1, Obj, Key), Q, IsEmpty};
		not_found ->
		    {error, not_found};
		{error,_} = Err ->
		    Err
	    end
    end.


is_queue_empty(#db{ref = Ref} = Db, Table, Q) ->
    Enc = encoding(Db, Table),
    QPfx = kvdb_lib:queue_prefix(Enc, Q, first),
    Prefix = make_table_key(Table, kvdb_lib:enc(key, QPfx, Enc)),
    QPrefix = table_queue_prefix(Table, Q, Enc),
    Sz = byte_size(QPrefix),
    TPrefix = make_table_key(Table),
    TPSz = byte_size(TPrefix),
    with_iterator(
      Ref,
      fun(I) ->
	      case eleveldb:iterator_move(I, Prefix) of
		  {ok, <<QPrefix:Sz/binary, _/binary>>,
		   <<"*", _/binary>>} ->
		      %% blocking
		      false;
		  {ok, <<QPrefix:Sz/binary, _/binary>> = K, _} ->
		      <<TPrefix:TPSz/binary, Key/binary>> = K,
		      case Key of
			  <<>> -> true;
			  _ ->
			      false
		      end;
		  _ ->
		      true
	      end
      end).

first_queue(#db{ref = Ref} = Db, Table) ->
    Type = type(Db, Table),
    case Type of
	set -> error(illegal);
	_ ->
	    TPrefix = make_table_key(Table),
	    TPSz = byte_size(TPrefix),
	    with_iterator(
	      Ref,
	      fun(I) ->
		      first_queue_(
			eleveldb:iterator_move(I, TPrefix), I, Db, Table,
			TPrefix, TPSz)
	      end)
    end.

first_queue_(Res, I, Db, Table, TPrefix, TPSz) ->
    case Res of
	{ok, <<TPrefix:TPSz/binary>>, _} ->
	    first_queue_(eleveldb:iterator_move(I, next), I, Db,
			 Table, TPrefix, TPSz);
	{ok, <<TPrefix:TPSz/binary, K/binary>>, _} ->
	    Enc = encoding(Db, Table),
	    #q_key{queue = Q} = kvdb_lib:split_queue_key(Enc, dec(key,K,Enc)),
	    {ok, Q};
	_ ->
	    done
    end.

next_queue(#db{ref = Ref} = Db, Table, Q) ->
    Type = type(Db, Table),
    case Type of
	set -> error(illegal);
	_ ->
	    Enc = encoding(Db, Table),
	    QPfx = kvdb_lib:queue_prefix(Enc, Q, last),
	    Prefix = make_table_key(Table, kvdb_lib:enc(key, QPfx, Enc)),
	    QPrefix = table_queue_prefix(Table, Q, Enc),
	    Sz = byte_size(QPrefix),
	    TPrefix = make_table_key(Table),
	    TPSz = byte_size(TPrefix),
	    with_iterator(
	      Ref,
	      fun(I) ->
		      next_queue_(eleveldb:iterator_move(I, Prefix), I,
				  Db, Table, QPrefix, Sz, TPrefix, TPSz, Enc)
	      end)
    end.

next_queue_(Res, I, Db, Table, QPrefix, Sz, TPrefix, TPSz, Enc) ->
    case Res of
	{ok, <<QPrefix:Sz/binary, _/binary>>, _} ->
	    next_queue_(eleveldb:iterator_move(I, next), I, Db, Table,
		       QPrefix, Sz, TPrefix, TPSz, Enc);
	{ok, <<TPrefix:TPSz/binary, K/binary>>, _} ->
	    #q_key{queue = Q} = kvdb_lib:split_queue_key(Enc, dec(key,K,Enc)),
	    {ok, Q};
	_ ->
	    done
    end.

%% fix_q_obj(Obj, Enc, Type) ->
%%     K = element(1, Obj),
%%     #q_key{key = K1} = kvdb_lib:split_queue_key(Enc, Type, K),
%%     setelement(1, Obj, K1).


q_first_(I, Db, Table, Q, Enc, HeedBlock) ->
    QPfx = kvdb_lib:queue_prefix(Enc, Q, first),
    Prefix = make_table_key(Table, kvdb_lib:enc(key, QPfx, Enc)),
    QPrefix = table_queue_prefix(Table, Q, Enc),
    Sz = byte_size(QPrefix),
    TPrefix = make_table_key(Table),
    TPSz = byte_size(TPrefix),
    q_first_move(eleveldb:iterator_move(I, Prefix),
		 I, Db, Table, Enc, QPrefix, Sz, TPrefix, TPSz, HeedBlock).

q_first_move(Res, I, Db, Table, Enc, QPrefix, Sz, TPrefix, TPSz, HeedBlock) ->
    case Res of
	{ok, <<QPrefix:Sz/binary, _/binary>>, <<"-", _/binary>>} ->
	    q_first_move(eleveldb:iterator_move(I, next),
			 I, Db, Table, Enc, QPrefix, Sz, TPrefix,
			 TPSz, HeedBlock);
	{ok, <<QPrefix:Sz/binary, _/binary>> = K, <<F:8, V/binary>>} ->
	    if F == $*, HeedBlock ->
		    blocked;
	       true ->
		    <<TPrefix:TPSz/binary, Key/binary>> = K,
		    case Key of
			<<>> -> q_first_move(eleveldb:iterator_move(I, next),
					     I, Db, Table, Enc, QPrefix, Sz,
					     TPrefix, TPSz, HeedBlock);
			_ ->
			    Status = case F of
					 $* -> blocking;
					 $+ -> active;
					 $- -> inactive
				     end,
			    {Status, decode_obj(Db, Enc, Table, Key, V)}
		    end
	    end;
	_ ->
	    done
    end.



%% q_last(#db{ref = Ref} = Db, Table, Q, Enc) ->
%%     with_iterator(Ref, fun(I) -> q_last_(I, Db, Table, Q, Enc) end).

q_last_(I, Db, Table, Q, Enc, HeedBlock) ->
    QPfx = kvdb_lib:queue_prefix(Enc, Q, last),
    Prefix = make_table_key(Table, kvdb_lib:enc(key, QPfx, Enc)),
    QPrefix = table_queue_prefix(Table, Q, Enc),
    Sz = byte_size(QPrefix),
    TPrefix = make_table_key(Table),
    TPSz = byte_size(TPrefix),
    case eleveldb:iterator_move(I, Prefix) of
	{ok, _K, _V} ->
	    q_last_move_(eleveldb:iterator_move(I, prev), Db, Table, Enc,
			 QPrefix, Sz, TPrefix, TPSz, HeedBlock);
	{error, invalid_iterator} ->
	    q_last_move_(eleveldb:iterator_move(I, last), Db, Table, Enc,
			 QPrefix, Sz, TPrefix, TPSz, HeedBlock)
    end.

q_last_move_(Res, Db, Table, Enc, QPrefix, Sz, TPrefix, TPSz, HeedBlock) ->
    case Res of
	{ok, <<QPrefix:Sz/binary, _/binary>> = K1, <<F:8, V1/binary>>} ->
	    if F == $*, HeedBlock ->
		    blocked;
	       true ->
		    <<TPrefix:TPSz/binary, Key/binary>> = K1,
		    case Key of
			<<>> -> done;
			_ ->
			    Status = case F of
					 $* -> blocking;
					 $+ -> active;
					 $- -> inactive
				     end,
			    {Status, decode_obj(Db, Enc, Table, Key, V1)}
		    end
	    end;
	_Other ->
	    done
    end.

list_queue(Db, Table, Q) ->
    list_queue(Db, Table, Q, fun(_,_,O) -> {keep,O} end, false, infinity).

list_queue(#db{ref = Ref} = Db, Table, Q, Filter, HeedBlock, Limit)
  when Limit > 0 ->  % includes 'infinity'
    Type = type(Db, Table),
    Enc = encoding(Db, Table),
    QPrefix = table_queue_prefix(Table, Q, Enc),
    TPrefix = make_table_key(Table),
    with_iterator(
      Ref,
      fun(I) ->
	      First =
		  case Type of
		      fifo -> q_first_(I, Db, Table, Q, Enc, HeedBlock);
		      lifo -> q_last_(I, Db, Table, Q, Enc, HeedBlock);
		      {keyed,fifo} -> q_first_(I,Db,Table,Q,Enc, HeedBlock);
		      {keyed,lifo} -> q_last_(I,Db,Table,Q,Enc, HeedBlock)
		  end,
	      q_all_(First, Limit, Limit, Filter, I, Db, Table,
		     q_all_dir(Type), Enc, Type, QPrefix, TPrefix,
		     HeedBlock, [])
      end);
list_queue(_, _, _, _, _, 0) ->
    {[], fun() -> done end}.

q_all_dir(fifo)     -> next;
q_all_dir(lifo)     -> prev;
q_all_dir({keyed,fifo}) -> next;
q_all_dir({keyed,lifo}) -> prev.

q_all_({St, Obj}, Limit, Limit0, Filter, I, Db, Table, Dir, Enc,
       Type, QPrefix, TPrefix, HeedBlock, Acc)
  when Limit > 0 ->
    #q_key{key = Key} = QKey =
	kvdb_lib:split_queue_key(Enc,Type,element(1,Obj)),
    {Cont,Acc1} = case Filter(St, QKey, setelement(1, Obj, Key)) of
		      skip     -> {true, Acc};
		      stop     -> {false, Acc};
		      {stop,X} -> {false, [X|Acc]};
		      {keep,X} -> {true, [X|Acc]}
	   end,
    case {Cont, decr(Limit)} of
	{true, Limit1} when Limit1 > 0 ->
	    q_all_cont(Limit1, Limit0, Filter, I, Db, Table, Dir, Enc,
		       Type, QPrefix, TPrefix, HeedBlock, Acc1);
	_ when Acc1 == [] ->
	    done;
	{true, _} ->
	    TabKey = make_table_key(Table, enc(key, element(1, Obj), Enc)),
	    {lists:reverse(Acc1),
	     fun() ->
		     with_iterator(
		       Db#db.ref,
		       fun(I1) ->
			       eleveldb:iterator_move(I1, TabKey),
			       q_all_cont(Limit0, Limit0, Filter, I1,
					  Db, Table, Dir, Enc,
					  Type, QPrefix, TPrefix, HeedBlock, [])
		       end)
	     end};
	{false, _}->
	    {lists:reverse(Acc1), fun() -> done end}
    end;
q_all_(Stop, _, _, _, _, _, _, _, _, _, _, _, _, Acc)
  when Stop == done; Stop == blocked ->
    if Acc == [] -> Stop;
       true ->
	    {lists:reverse(Acc), fun() -> Stop end}
    end.

q_all_cont(Limit, Limit0, Filter, I, Db, Table, Dir, Enc, Type,
	   QPrefix, TPrefix, HeedBlock, Acc) ->
    QSz = byte_size(QPrefix),
    TSz = byte_size(TPrefix),
    case eleveldb:iterator_move(I, Dir) of
	{ok, <<QPrefix:QSz/binary, _/binary>> = K, <<F:8, V/binary>>} ->
	    if F == $*, HeedBlock ->
		    q_all_(blocked, Limit, Limit0, Filter, I, Db,
			   Table, Dir, Enc, Type, QPrefix, TPrefix,
			   HeedBlock, Acc);
	       true ->
		    Status = case F of
				 $* -> blocking;
				 $+ -> active;
				 $- -> inactive
			     end,
		    <<TPrefix:TSz/binary, Key/binary>> = K,
		    q_all_({Status, decode_obj(Db, Enc, Table, Key, V)},
			   Limit, Limit0, Filter, I, Db, Table,
			   Dir, Enc, Type, QPrefix, TPrefix, HeedBlock, Acc)
	    end;
	_ ->
	    q_all_(done, Limit, Limit0, Filter, I, Db, Table, Dir,
		   Enc, Type, QPrefix, TPrefix, HeedBlock, Acc)
    end.

table_queue_prefix(Table, Q, Enc) when Enc == raw; element(1, Enc) == raw ->
    make_table_key(Table, <<Q/binary, "-">>);
table_queue_prefix(Table, Q, Enc) when Enc == sext; element(1, Enc) == sext ->
    make_table_key(Table, sext:prefix({Q,'_','_'})).


get(Db, Table, Key) ->
    lager:debug("get: Db = ~p, Table = ~p, Key = ~p ~n",  [Db, Table, Key]),
    case type(Db, Table) of
	set ->
	    get(Db, Table, Key, encoding(Db, Table), set);
	_ ->
	    error(illegal)
    end.

%% get(#db{} = Db, Table, #q_key{} = QKey, Enc, Type) ->
%%     Actual = kvdb_lib:q_key_to_actual(QKey, Enc, Type),
%%     EncKey = enc(key, Actual, Enc),
%%     get_(Db, Table, QKey#q_key.key, EncKey, Enc, Type);
get(#db{} = Db, Table, Key, Enc, Type) ->
    EncKey = enc(key, Key, Enc),
    get_(Db, Table, Key, EncKey, Enc, Type).

get_(#db{ref = Ref} = Db, Table, Key, EncKey, Enc, Type) ->
    case {Type, eleveldb:get(Ref, make_table_key(Table, EncKey), [])} of
	{set, {ok, V}} ->
	    {ok, decode_obj_v(Db, Enc, Table, EncKey, Key, V)};
	{_, {ok, <<F:8, V/binary>>}} when F==$*; F==$+; F==$- ->
	    {ok, decode_obj_v(Db, Enc, Table, EncKey, Key, V)};
	{_, not_found} ->
	    {error, not_found};
	{_, {error, _}} = Error ->
	    Error
    end.

%% used during indexing (only if index function requires the value)
get_value(Db, Table, K) ->
    case get(Db, Table, K) of
	{ok, {_, _, V}} ->
	    V;
	{ok, {_, V}} ->
	    V;
	{error, not_found} ->
	    throw(no_value)
    end.

index_get(#db{ref = Ref} = Db, Table, IxName, IxVal) ->
    Enc = encoding(Db, Table),
    Type = type(Db, Table),
    IxPat = make_key(Table, $?, sext:encode({IxName, IxVal})),
    with_iterator(
      Ref,
      fun(I) ->
	      get_by_ix_(prefix_move(I, IxPat, IxPat), I, IxPat, Db,
			 Table, Enc, Type, obj)
      end).

index_keys(#db{ref = Ref} = Db, Table, IxName, IxVal) ->
    Enc = encoding(Db, Table),
    Type = type(Db, Table),
    IxPat = make_key(Table, $?, sext:encode({IxName, IxVal})),
    with_iterator(
      Ref,
      fun(I) ->
	      get_by_ix_(prefix_move(I, IxPat, IxPat), I, IxPat, Db,
			 Table, Enc, Type, key)
      end).

get_by_ix_({ok, K, _}, I, Prefix, Db, Table, Enc, Type, Acc) ->
    case get(Db, Table, sext:decode(K), Enc, Type) of
	{ok, Obj} ->
	    Keep = case Acc of
		       obj -> Obj;
		       key -> element(1, Obj)
		   end,
	    [Keep | get_by_ix_(prefix_move(I, Prefix, next), I,
			       Prefix, Db, Table, Enc, Type, Acc)];
	{error,_} ->
	    get_by_ix_(prefix_move(I, Prefix, next), I, Prefix,
		       Db, Table, Enc, Type, Acc)
    end;
get_by_ix_(done, _, _, _, _, _, _, _) ->
    [].


queue_read(#db{ref = Ref} = Db, Table, #q_key{key = K} = QKey) ->
    Enc = encoding(Db, Table),
    Type = type(Db, Table),
    Key = kvdb_lib:q_key_to_actual(QKey, Enc, Type),
    EncKey = enc(key, Key, Enc),
    case eleveldb:get(Ref, make_table_key(Table, EncKey), []) of
	{ok, <<St:8, V/binary>>} when St==$*; St==$-; St==$+ ->
	    Obj = decode_obj_v(Db, Enc, Table, EncKey, Key, V),
	    Status = dec_queue_obj_status(St),
	    {ok, Status, setelement(1, Obj, K)};
	not_found ->
	    {error, not_found};
	{error, _} = Error ->
	    Error
    end.

delete(#db{} = Db, Table, #q_key{} = QKey) ->
    Enc = encoding(Db, Table),
    Type = type(Db, Table),
    Key = kvdb_lib:q_key_to_actual(QKey, Enc, Type),
    EncKey = enc(key, Key, Enc),
    delete_(Db, Table, Enc, Key, EncKey);
delete(#db{} = Db, Table, Key) ->
    Enc = encoding(Db, Table),
    EncKey = enc(key, Key, Enc),
    delete_(Db, Table, Enc, Key, EncKey).

delete_(#db{ref = Ref} = Db, Table, Enc, Key, EncKey) ->
    Ix = case Enc of
	     {_,_,_} -> index(Db, Table);
	     _ -> []
	 end,
    {IxOps, As} =
	case Enc of
	    {_, _, _} ->
		Attrs = get_attrs_(Db, Table, EncKey, all),
		IxOps_ = case Ix of
			     [_|_] -> [{delete, ix_key(Table, I, Key)} ||
					  I <- kvdb_lib:index_vals(
						 Ix, Key, Attrs,
						 fun() ->
							 get_value(Db, Table,
								   Key)
						 end)];
			     _ -> []
			 end,
		{IxOps_, attrs_to_delete(Table, EncKey, Attrs)};
	    _ ->
		{[], []}
	end,
    eleveldb:write(Ref, IxOps ++ [{delete, make_table_key(Table, EncKey)} | As], []).


attrs_to_put(_, _, []) -> [];
attrs_to_put(_, _, none) -> [];  % still needed?
attrs_to_put(Table, Key, Attrs) when is_list(Attrs), is_binary(Key) ->
    %% EncKey = sext:encode(Key),
    [{put, make_key(Table, $=, <<Key/binary,
				 (sext:encode(K))/binary>>),
      term_to_binary(V)} || {K, V} <- Attrs].

attrs_to_delete(_, _, []) -> [];
attrs_to_delete(Table, Key, Attrs) when is_list(Attrs), is_binary(Key) ->
    %% EncKey = sext:encode(Key),
    [{delete, make_key(Table, $=,
		       <<Key/binary,
			 (sext:encode(A))/binary>>)} || {A,_} <- Attrs].


get_attrs(#db{ref = Ref} = Db, Table, Key, As) ->
    case encoding(Db, Table) of
	{_, _, _} = Enc ->
	    EncKey = enc(key, Key, Enc),
	    case eleveldb:get(Ref, make_table_key(Table, EncKey), []) of
		{ok, _} ->
		    {ok, get_attrs_(Db, Table, EncKey, As)};
		_ ->
		    {error, not_found}
	    end;
	_ ->
	    error(badarg)
    end.

get_attrs_(#db{ref = Ref}, Table, EncKey, As) ->
    TableKey = make_key(Table, $=, EncKey),
    with_iterator(
      Ref,
      fun(I) ->
	      get_attrs_iter_(prefix_move(I, TableKey, TableKey),
			      I, TableKey, As)
      end).

get_attrs_iter_({ok, K, V}, I, Prefix, As) ->
    Key = sext:decode(K),
    case As == all orelse lists:member(Key, As) of
	true ->
	    [{Key, binary_to_term(V)}|
	     get_attrs_iter_(prefix_move(I, Prefix, next), I, Prefix, As)];
	false ->
	    get_attrs_iter_(prefix_move(I, Prefix, next), I, Prefix, As)
    end;
get_attrs_iter_(done, _, _, _) ->
   [].


prefix_match(Db, Table, Prefix) ->
    prefix_match(Db, Table, Prefix, 100).

prefix_match(#db{} = Db, Table, Prefix, Limit)
  when (is_integer(Limit) orelse Limit == infinity) ->
    prefix_match(Db, Table, Prefix, false, Limit).

prefix_match_rel(#db{} = Db, Table, Prefix, StartPoint, Limit) ->
    prefix_match(Db, Table, Prefix, {true, StartPoint}, Limit).

prefix_match(#db{ref = Ref} = Db, Table, Prefix, Rel, Limit)
  when (is_integer(Limit) orelse Limit == infinity) ->
    Enc = encoding(Db, Table),
    EncPrefix = kvdb_lib:enc_prefix(key, Prefix, Enc),
    EncStart = case Rel of
		   false ->
		       EncPrefix;
		   {true, StartPoint} ->
		       enc(key, StartPoint, Enc)
	       end,
    TablePrefix = make_table_key(Table),
    TabPfxSz = byte_size(TablePrefix),
    MatchKey = make_table_key(Table, EncPrefix),
    StartKey = make_table_key(Table, EncStart),
    with_iterator(
      Ref,
      fun(I) ->
	      if Rel==false, EncStart == <<>> ->
		      case eleveldb:iterator_move(I, TablePrefix) of
			  {ok, <<TablePrefix:TabPfxSz/binary>>, _} ->
			      prefix_match_(I, next, Db, Table, MatchKey, TablePrefix,
					    Prefix, Enc, Limit, Limit, []);
			  _ ->
			      done
		      end;
		 Rel=/=false ->
		      case eleveldb:iterator_move(I, StartKey) of
			  {ok, StartKey, _} ->
			      prefix_match_(I, next, Db, Table, MatchKey,
					    TablePrefix, Prefix, Enc,
					    Limit, Limit, []);
			  {ok, _, _} ->
			      prefix_match_(I, StartKey, Db, Table, MatchKey,
					    TablePrefix, Prefix, Enc,
					    Limit, Limit, [])
		      end;
		 true ->
		      prefix_match_(I, StartKey, Db, Table, MatchKey,
				    TablePrefix, Prefix, Enc, Limit, Limit, [])
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
	      iterator_move_(I, Db, Table, Enc, TableKey, KeySize,
			     EncRel, RelKey, Comp, Dir)
      end).

iterator_move_(I, Db, Table, Enc, TableKey, KeySize, EncRel,
	       RelKey, Comp, Dir) ->
    case eleveldb:iterator_move(I, RelKey) of
	{ok, <<TableKey:KeySize/binary, Key/binary>>, Value} ->
	    case Key =/= <<>> andalso Comp(Key, EncRel) of
		true ->
		    {ok, decode_obj(Db, Enc, Table, Key, Value)};
		false ->
		    case eleveldb:iterator_move(I, Dir) of
			{ok, <<TableKey:KeySize/binary,
			       Key2/binary>>, Value2} ->
			    case Key2 of
				<<>> -> done;
				_ ->
				    {ok, decode_obj(Db, Enc, Table,
						    Key2, Value2)}
			    end;
			_ ->
			    done
		    end
	    end;
	{ok, OtherKey, _} when Dir == prev ->
	    if byte_size(OtherKey) >= KeySize ->
		    <<OtherTabPat:KeySize/binary, _/binary>> = OtherKey,
		    if OtherTabPat > TableKey ->
			    %% try stepping back
			    iterator_move_(I, Db, Table, Enc, TableKey,
					   KeySize, EncRel, prev, Comp, Dir);
		       true ->
			    done
		    end;
	       true ->
		    done
	    end;
	_ ->
	    done
    end.

%% create key
make_table_last_key(Table) ->
    make_key(Table, $;, <<>>).

make_table_key(Table) ->
    make_key(Table, $:, <<>>).

make_table_key(Table, Key) ->
    make_key(Table, $:, Key).

make_key(Table, Sep, Key) when is_binary(Table) ->
    <<Table/binary,Sep,Key/binary>>.

%% encode_obj({_,_,_} = Enc, {Key, Attrs, Value}) ->
%%     {enc(key, Key, Enc), Attrs, enc(value, Value, Enc)};
%% encode_obj(Enc, {Key, Value}) ->
%%     {enc(key, Key, Enc), none, enc(value, Value, Enc)}.

encode_queue_obj(Enc, Obj) ->
    encode_queue_obj(Enc, Obj, active).

encode_queue_obj({_,_,_} = Enc, {Key, Attrs, Value}, Status) ->
    St = enc_queue_obj_status(Status),
    {enc(key, Key, Enc), Attrs, <<St:8, (enc(value, Value, Enc))/binary>>};
encode_queue_obj(Enc, {Key, Value}, Status) ->
    St = enc_queue_obj_status(Status),
    {enc(key, Key, Enc), none, <<St:8, (enc(value, Value, Enc))/binary>>}.

enc_queue_obj_status(blocking) -> $*;
enc_queue_obj_status(active  ) -> $+;
enc_queue_obj_status(inactive) -> $-.

dec_queue_obj_status($*) -> blocking;
dec_queue_obj_status($+) -> active;
dec_queue_obj_status($-) -> inactive.

decode_obj(Db, Enc, Table, K, V) ->
    Key = dec(key, K, Enc),
    decode_obj_v(Db, Enc, Table, K, Key, V).

decode_obj_v(Db, Enc, Table, EncKey, Key, V) ->
    Value = dec(value, V, Enc),
    case Enc of
	{_, _, _} ->
	    Attrs = get_attrs_(Db, Table, EncKey, all),
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
    schema_lookup(Db, {a, Table, type}, set).

encoding(#db{encoding = Enc} = Db, Table) ->
    schema_lookup(Db, {a, Table, encoding}, Enc).

index(#db{} = Db, Table) ->
    schema_lookup(Db, {a, Table, index}, []).

schema(#db{} = Db, Table) ->
    schema_lookup(Db, {a, Table, schema}, []).

check_options([{type, T}|Tl], Db, Rec)
  when T==set; T==fifo; T==lifo; T=={keyed,fifo}; T=={keyed,lifo} ->
    check_options(Tl, Db, Rec#table{type = T});
check_options([{schema, S}|Tl], Db, Rec) when is_atom(S) ->
    check_options(Tl, Db, Rec#table{schema = S});
check_options([{encoding, E}|Tl], Db, Rec) ->
    Rec1 = Rec#table{encoding = E},
    kvdb_lib:check_valid_encoding(E),
    check_options(Tl, Db, Rec1);
check_options([{index, Ix}|Tl], Db, Rec) ->
    case kvdb_lib:valid_indexes(Ix) of
	ok -> check_options(Tl, Db, Rec#table{index = Ix});
	{error, Bad} ->
	    error({invalid_index, Bad})
    end;
check_options([], _, Rec) ->
    Rec.

ensure_schema(#db{ref = Ref} = Db, Opts) ->
    ETS = ets:new(kvdb_schema, [ordered_set, public]),
    Db1 = Db#db{metadata = ETS},
    case eleveldb:get(Ref, make_table_key(?META_TABLE, <<>>), []) of
	{ok, _} ->
	    [ets:insert(ETS, X) || X <- whole_table(Db1, sext, ?META_TABLE)],
	    Db1;
	_ ->
	    ok = do_add_table(Db1, ?META_TABLE),
	    Tab = #table{name = ?META_TABLE, encoding = sext,
			 columns = [key,value]},
	    schema_write(Db1, {{table, ?META_TABLE}, Tab}),
	    schema_write(Db1, {{a, ?META_TABLE, encoding}, sext}),
	    schema_write(Db1, {{a, ?META_TABLE, type}, set}),
	    schema_write(Db1, {schema_mod, proplists:get_value(schema, Opts, kvdb_schema)}),
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
    put(Db, ?META_TABLE, Item).

schema_lookup(_, {a, ?META_TABLE, Attr}, Default) ->
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
    delete(Db, ?META_TABLE, Key).
