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
%%% @hidden
%%% @doc
%%%    SQLITE3 backend to kvdb
%%% @end

-module(kvdb_sqlite3).

-behaviour(kvdb).

-export([open/2, close/1]).
-export([add_table/3, delete_table/2, list_tables/1,
	 get_attrs/4,
	 delete/3]).
-export([put/3, push/4, update_counter/4,
	 %% put_attrs/4,
	 get/3, index_get/4, index_keys/4,
	 pop/3, prel_pop/3, extract/3,
	 list_queue/3, list_queue/6, list_queue/7, is_queue_empty/3,
	 first_queue/2, next_queue/3,
	 mark_queue_object/4,
	 queue_insert/5, queue_delete/3, queue_read/3,
         queue_head_read/3, queue_head_write/4, queue_head_delete/3]).
-export([first/2, last/2, next/3, prev/3]).
-export([prefix_match/3, prefix_match/4, prefix_match_rel/5]).
-export([info/2, get_schema_mod/2, dump_tables/1]).
-export([is_table/2]).

%% for testing
%% -export([prefix_match/5]).

-import(kvdb_lib, [enc/3, dec/3, enc_prefix/3]).
-include("kvdb.hrl").
%-record(sqlite3_iter, {table, ref, db}).

get_schema_mod(_, M) ->
    M.

info(#db{ref = Db}, ref) -> Db;
info(#db{} = Db, tables) -> list_tables(Db);
info(#db{encoding = Enc}, encoding) -> Enc;
info(#db{} = Db, {Tab, What}) ->
    case is_table(Db, Tab) of
	true ->
	    case What of
		encoding -> encoding(Db, Tab);
		index    -> index   (Db, Tab);
		type     -> type    (Db, Tab);
		schema   -> schema  (Db, Tab);
		tabrec   -> schema_lookup(Db, {table, Tab}, undefined)
	    end;
	false ->
	    undefined
    end;
info(#db{}, _) -> undefined.

dump_tables(#db{ref = Ref} = Db) ->
    Tabs = sqlite3:list_tables(Ref),
    lists:flatmap(
      fun(T) ->
	      dump_table(Ref, T, Db)
      end, Tabs).

dump_table(Ref, T, Db) ->
    case sqlite3:sql_exec(Ref, ["SELECT * FROM [", T, "];"]) of
	[{columns, Cols},
	 {rows, Rows}] ->
	    case type(Db, T) of
		set ->
		    [format_row(T, Cols, R) || R <- Rows];
		TType when TType==fifo; TType==lifo;
			   element(1, TType) == fifo;
			   element(1, TType) == lifo ->
		    [format_q_row(T, TType, encoding(Db, T), Cols, R) ||
			R <- Rows]
	    end;
	Other ->
	    [{error, Other}]
    end.

format_row(T, ["key","value"], R) ->
    {obj, T, decode_r(R)};
format_row(T, ["key", "attrs", "value"], R) ->
    {obj, T, decode_r(R)};
format_row(T, ["ix", "key"], {{blob,Bi}, {blob,Bk}}) ->
    {Ix, Rest} = sext:decode_next(Bi),
    Kix = sext:decode(Rest),
    {index, T, Ix, Kix, kvdb_lib:try_decode(Bk)}.

format_q_row(T, Type, Enc, ["key","value","active"], {{blob,Bk},
						      {blob,Bv},A}) ->
    #q_key{queue = Q,key = Ko} = kvdb_lib:split_queue_key(Enc, Type, Bk),
    {q_obj, T, Q, Bk, {Ko, dec(value, Bv, Enc)}, active_to_status(A)};
format_q_row(T, Type, Enc, ["key","attrs","value","active"], {{blob,Bk},
							      {blob,Ba},
							      {blob,Bv},A}) ->
    #q_key{queue = Q, key= Ko} = kvdb_lib:split_queue_key(Enc, Type, Bk),
    {q_obj, T, Q, Bk, {Ko, dec(attrs, Ba, Enc), dec(value, Bv, Enc)},
     active_to_status(A)}.

active_to_status(0) -> inactive;
active_to_status(1) -> active;
active_to_status(2) -> blocking.

decode_r(R) ->
    list_to_tuple(lists:map(fun({blob, X}) -> kvdb_lib:try_decode(X) end,
			    tuple_to_list(R))).

open(DbName, Options) ->
    IntName = make_atom_name(kvdb_lib:good_string(DbName)),
    E = proplists:get_value(encoding, Options, sext),
    kvdb_lib:check_valid_encoding(E),
    DbOptions = proplists:get_value(db_opts, Options, []),
    Res = case {proplists:get_value(file, Options),
		proplists:get_value(file, DbOptions)} of
	      {undefined,undefined} ->
		  sqlite3:open(IntName, [{file,atom_to_list(IntName)++".db"}|
					 DbOptions]);
	      {F, undefined} ->
		  sqlite3:open(IntName, [{file, F}|DbOptions]);
	      _ ->
		  sqlite3:open(IntName, DbOptions)
	  end,
    case Res of
	{ok, _Pid} ->
	    {ok, ensure_schema(#db{ref = IntName, encoding = E})};
	{error,_} = Error ->
	    Error
    end.

%% make_atom_name(B) when is_binary(B) ->
%%     binary_to_atom(B, latin1);
make_atom_name(X) ->
    list_to_atom(lists:flatten(io_lib:fwrite("~s", [X]))).




close(#db{ref = Db, metadata = ETS}) ->
    ets:delete(ETS),  % will crash if called several times
    sqlite3:close(Db).

add_table(#db{encoding = Enc} = Db,Table,Opts) when is_list(Opts) ->
    TabR = check_options(Opts, Db, #table{name = Table, encoding = Enc}),
    add_table(Db, Table, TabR);
add_table(#db{ref = Ref} = Db, Table, #table{} = TabR) ->
    case schema_lookup(Db, {table, Table}, undefined) of
	T when T =/= undefined ->
	    ok;
	undefined ->
	    Columns0 = case TabR#table.encoding of
			   {_, _, _} ->
			       [{key, blob, primary_key},
				{attrs, blob},
				{value, blob}];
			   _ ->
			       [{key, blob, primary_key},
				{value, blob}]
		       end,
	    Columns = case TabR#table.type of
			  Type when Type==fifo; Type==lifo;
				    element(1,Type) == keyed ->
			      Columns0 ++ [{active, integer, [{default,1}]}];
			  _ ->
			      Columns0
		      end,
	    case sqlite3:create_table(Ref, Table, Columns) of
		ok ->
		    TabR1 = maybe_create_index_table(Ref, Table, TabR),
		    schema_write(Db, {{table, Table}, TabR1}),
		    schema_write(Db, {{a,Table, type}, TabR1#table.type}),
		    schema_write(Db, {{a,Table, index}, TabR1#table.index}),
		    schema_write(Db, {{a,Table, encoding}, TabR1#table.encoding}),
		    ok;
		Error ->
		    Error
	    end
    end.

maybe_create_index_table(_Ref, _Table, #table{index = []} = TabR) ->
    TabR;
maybe_create_index_table(Ref, Table, #table{index = [_|_] = Ix} = TabR) ->
    IxTab = <<"[", Table/binary, "-index]">>,
    sqlite3:create_table(Ref, IxTab, [{ix, blob, primary_key}, {key, blob}]),
    TabR#table{index = {IxTab, Ix}}.


list_tables(#db{metadata = ETS}) ->
    ets:select(ETS, [{ {{table, '$1'}, '_'}, [{'=/=','$1',?META_TABLE}], ['$1'] }]).


delete_table(#db{ref = Ref} = Db, Table) ->
    case schema_lookup(Db, {table, Table}, undefined) of
	undefined ->
	    ok;
	#table{index = Index} ->
	    sqlite3:drop_table(Ref, Table),
	    schema_delete(Db, {table, Table}),
	    schema_delete(Db, {Table, encoding}),
	    case Index of
		{IxTab, _} ->
		    sqlite3:drop_table(Ref, IxTab);
		_ ->
		    ok
	    end,
	    ok
    end.

put(Db, Table, Obj) ->
    case type(Db, Table) of
	set -> put_(Db, Table, Obj);
	_ -> {error, illegal}
    end.

put_(#db{ref = Ref} = Db, Table, {Key, Value}) ->
    Enc = encoding(Db, Table),
    case insert_or_replace(Ref, Table,
			   [{key, {blob, enc(key, Key, Enc)}},
			    {value, {blob, enc(value, Value, Enc)}}]) of
	ok ->
	    ok;
	{error, _} = Error ->
	    Error
    end;
put_(#db{ref = Ref} = Db, Table, {Key, Attrs, Value}) ->
    Enc = encoding(Db, Table),
    EncKey = enc(key, Key, Enc),
    InsertSQLData = [{key, {blob, EncKey}},
		     {attrs, {blob, enc(attrs, Attrs, Enc)}},
		     {value, {blob, enc(value, Value, Enc)}}],
    case index(Db, Table) of
	[] ->
	    case insert_or_replace(Ref, Table, InsertSQLData) of
		ok ->
		    ok;
		{error,_} = Error ->
		    Error
	    end;
	{IxTable, Ix} ->
	    OldAttrs = case get(Db, Table, Key) of
			   {ok, {_, OldAs, _}} -> OldAs;
			   {error, _} -> []
		       end,
	    OldIxVals = kvdb_lib:index_vals(
			  Ix, Key, OldAttrs,
			  fun() -> get_value(Db, Table, Key) end),
	    NewIxVals = kvdb_lib:index_vals(Ix, Key, Attrs, fun() -> Value end),
	    DelIxVals = [{I, Key} || I <- OldIxVals -- NewIxVals],
	    PutIxVals = [{I, Key} || I <- NewIxVals -- OldIxVals],
	    KeySext = sext:encode(Key),
	    Results =
		sqlite3:sql_exec_script(
		  Ref,
		  ["BEGIN;",
		   [sqlite3_lib:delete_sql(IxTable, "ix",
					   {blob, <<(sext:encode(I))/binary,
						    KeySext/binary>>}) ||
		       I <- DelIxVals],
		   [insert_or_replace_sql(IxTable,
					  [{ix, {blob, <<(sext:encode(I))/binary,
							 KeySext/binary>>}},
					   {key, {blob, EncKey}}]) ||
		       I <- PutIxVals],
		   insert_or_replace_sql(Table, InsertSQLData),
		   "COMMIT;"]),
	    Bad = fun({rowid,_}) -> false;
		     (ok) -> false;
		     (_) -> true
		  end,
	    [] = [X || X <- Results, Bad(X)],
	    ok
    end.

update_counter(#db{} = Db, Table, K, Incr) ->
    %% Ouch! This is problematic. Since we support different encodings that
    %% sqlite doesn't know about, it's hard to do this atomically in SQL.
    case type(Db, Table) of
	set ->
	    case get(Db, Table, K) of
		{ok, Obj} ->
		    Sz = size(Obj),
		    V = element(Sz, Obj),
		    NewV = if is_binary(V) ->
				   VSz = bit_size(V),
				   <<I:VSz/integer>> = V,
				   <<(I+Incr):VSz/integer>>;
			      is_integer(V) ->
				   V+Incr;
			      true ->
				   erlang:error(illegal)
			   end,
		    NewObj = setelement(Sz, Obj, NewV),
		    ok = put(Db, Table, NewObj),
		    NewV;
		E ->
		    E
	    end;
	_ ->
	    erlang:error(illegal)
    end.

push(#db{ref = Ref} = Db, Table, Q, {Key, Value}) ->
    Type = type(Db, Table),
    if Type == fifo; Type == lifo; element(1, Type) == keyed ->
	    Enc = encoding(Db, Table),
	    {ActualKey, QKey} = kvdb_lib:actual_key(Enc, Type, Q, Key),
	    case insert_or_replace(
		   Ref, Table, [{key, {blob, enc(key, ActualKey, Enc)}},
				{value, {blob, enc(value, Value, Enc)}}]) of
		ok ->
		    {ok, QKey};
		{error, _} = Error ->
		    Error
	    end;
       true ->
	    {error, badarg}
    end;
push(#db{ref = Ref} = Db, Table, Q, {Key, Attrs, Value}) ->
    Type = type(Db, Table),
    if Type == fifo; Type == lifo; element(1, Type) == keyed ->
	    Enc = encoding(Db, Table),
	    {ActualKey, QKey} = kvdb_lib:actual_key(Enc, Type, Q, Key),
	    case insert_or_replace(
		   Ref, Table, [{key, {blob, enc(key, ActualKey, Enc)}},
				{attrs, {blob, enc(attrs, Attrs, Enc)}},
				{value, {blob, enc(value, Value, Enc)}}]) of
		ok ->
		    {ok, QKey};
		{error,_} = Error ->
		    Error
	    end;
       true ->
	    {error, badarg}
    end.

index(#db{} = Db, Table) ->
    schema_lookup(Db, {a, Table, index}, []).

encoding(#db{encoding = Enc} = Db, Table) ->
    schema_lookup(Db, {a, Table, encoding}, Enc).

key_encoding(E) when E==sext; element(1, E) == sext -> sext;
key_encoding(E) when E==raw ; element(1, E) == raw  -> raw.


type(Db, Table) ->
    schema_lookup(Db, {a, Table, type}, set).

schema(Db, Table) ->
    schema_lookup(Db, {a, Table, schema}, []).

insert_or_replace(Db, Table, Data) ->
    SQL = insert_or_replace_sql(Table, Data),
    case sqlite3:sql_exec(Db, SQL) of
	{rowid,_} ->
	    ok;
	[{columns,_},{rows,[{_,{blob,_OldValue}}]}] ->
	    ok;
	Error ->
	    Error
    end.

insert_or_replace_sql(Table, Data) ->
    {Cols, Values} = lists:unzip(Data),
    ["INSERT OR REPLACE INTO ", Table, " (",
     sqlite3_lib:write_col_sql(Cols),
     ") values (",
     sqlite3_lib:write_value_sql(Values), ");"
    ].

index_get(#db{ref = Ref} = Db, Table, IxName, IxVal) ->
    case index(Db, Table) of
	{IxTable, Ix} ->
	    Enc = encoding(Db, Table),
	    case lists:member(IxName, Ix) orelse
		lists:keymember(IxName, 1, Ix) of
		true ->
		    Pfx = sext:prefix({{IxName, IxVal}, '_'}),
		    Sz = byte_size(Pfx) -1,
		    <<P:Sz/binary, Last>> = Pfx,
		    PfxB = <<P/binary, (Last+1):8>>,
		    PrefixA = sqlite3_lib:value_to_sql({blob, Pfx}),
		    PrefixB = sqlite3_lib:value_to_sql({blob, PfxB}),
		    SQL = ["SELECT t.key, t.attrs, t.value FROM ",
			   Table, " AS t, ", IxTable, " AS i WHERE ",
			   "i.ix BETWEEN ", PrefixA, " AND ", PrefixB,
			   " AND t.key == i.key;"],
		    case sqlite3:sql_exec(Ref, SQL) of
			[{columns, ["key","attrs","value"]},
			 {rows, Rows}] ->
			    lists:map(
			      fun({{blob,Bk}, {blob,Ba}, {blob,Bv}}) ->
				      {dec(key, Bk, Enc),
				       dec(attrs, Ba, Enc),
				       dec(value, Bv, Enc)}
			      end, Rows);
			_ ->
			    []
		    end;
		false ->
		    {error, invalid_index}
	    end;
	_ ->
	    {error, no_index}
    end.

%% Copy-pasted from index_get() and slightly changed. Should be refactored
%%
index_keys(#db{ref = Ref} = Db, Table, IxName, IxVal) ->
    case index(Db, Table) of
	{IxTable, Ix} ->
	    Enc = encoding(Db, Table),
	    case lists:member(IxName, Ix) orelse
		lists:keymember(IxName, 1, Ix) of
		true ->
		    Pfx = sext:prefix({{IxName, IxVal}, '_'}),
		    Sz = byte_size(Pfx) -1,
		    <<P:Sz/binary, Last>> = Pfx,
		    PfxB = <<P/binary, (Last+1):8>>,
		    PrefixA = sqlite3_lib:value_to_sql({blob, Pfx}),
		    PrefixB = sqlite3_lib:value_to_sql({blob, PfxB}),
		    SQL = ["SELECT t.key FROM ",
			   Table, " AS t, ", IxTable, " AS i WHERE ",
			   "i.ix BETWEEN ", PrefixA, " AND ", PrefixB,
			   " AND t.key == i.key;"],
		    case sqlite3:sql_exec(Ref, SQL) of
			[{columns, ["key"]},
			 {rows, Rows}] ->
			    lists:map(
			      fun({{blob,Bk}}) ->
				      dec(key, Bk, Enc)
			      end, Rows);
			_ ->
			    []
		    end;
		false ->
		    {error, invalid_index}
	    end;
	_ ->
	    {error, no_index}
    end.

get(Db, Table, Key) ->
    case type(Db, Table) of
	set ->
	    get_(Db, Table, Key, false);
	_ ->
	    erlang:error(illegal)
    end.

get_(#db{ref = Ref} = Db, Table, Key, IncludeActive) when
      is_boolean(IncludeActive) ->
    Enc = encoding(Db, Table),
    get_(Db, Table, Key, enc(key, Key, Enc), Enc, IncludeActive).

get_(#db{ref = Ref} = Db, Table, Key, EncKey, Enc, IncludeActive) ->
    case sqlite3:read(Ref, Table, {key, {blob, EncKey}}) of
	[{columns,["key","value","active"]},
	 {rows,[{_,{blob,Value},_} = Data]}] ->
	    if IncludeActive ->
		    {ok, {obj_status(Data), {Key, dec(value, Value, Enc)}}};
	       true ->
		    {ok, {Key, dec(value, Value, Enc)}}
	    end;
	[{columns,_},{rows,[{_,{blob,Value}}]}] ->
	    {ok, {Key, dec(value, Value, Enc)}};
	[{columns,["key","attrs","value","active"]},
	 {rows,[{_,{blob,Attrs},{blob,Value},_} = Data]}] ->
	    if IncludeActive ->
		    {ok,
		     {obj_status(Data),
		      {Key, dec(attrs, Attrs, Enc), dec(value, Value, Enc)}}};
	       true ->
		    {ok, {Key, dec(attrs, Attrs, Enc), dec(value, Value, Enc)}}
	    end;
	[{columns,["key","attrs","value"]},
	 {rows,[{_,{blob,Attrs},{blob,Value}}]}] ->
	    {ok, {Key, dec(attrs, Attrs, Enc), dec(value, Value, Enc)}};
	_ ->
	    {error,not_found}
    end.

%% Used by the indexing function (only if it requires the value part)
get_value(Db, Table, Key) ->
    case get(Db, Table, Key) of
	{ok, {_, _, V}} ->
	    V;
	{ok, {_, V}} ->
	    V;
	{error, _} ->
	    throw(no_value)
    end.

pop(Db, Table, Q) ->
    case type(Db, Table) of
	set -> erlang:error(illegal);
	T ->
	    Remove = fun(Obj, _) ->
			     delete(Db, Table, element(1, Obj))
		     end,
	    do_pop(Db, Table, T, Q, Remove, false)
    end.

prel_pop(Db, Table, Q) ->
    case type(Db, Table) of
	set ->
	    erlang:error(illegal);
	T ->
	    Remove = fun(Obj, Enc) ->
			     mark_queue_object(Db, Table, Enc, Obj, blocking)
		     end,
	    do_pop(Db, Table, T, Q, Remove, true)
    end.

mark_queue_object(#db{} = Db, Table, #q_key{} = QKey, St) when
      St==active; St==blocking; St==inactive ->
    case queue_read(Db, Table, QKey) of
	{ok, _, Obj} ->
	    Enc = encoding(Db, Table),
	    Type = type(Db, Table),
	    K = element(1, Obj),
	    #q_key{queue = Q, key = K1} =
		kvdb_lib:split_queue_key(Enc, Type, K),
	    mark_queue_object(Db, Table, Enc, Obj, St),
	    {ok, Q, setelement(1, Obj, K1)};
	Error ->
	    Error
    end.

mark_queue_object(#db{ref = Ref}, Table, Enc, Obj, St) when St==inactive;
							    St==blocking;
							    St==active ->
    ActiveCol = case St of
		    inactive -> {active, 0};
		    active   -> {active, 1};
		    blocking -> {active, 2}
		end,
    insert_or_replace(Ref, Table, mark_cols(Obj, Enc) ++ [ActiveCol]).

mark_cols({K,As,V}, Enc) ->
    [{key, {blob, enc(key, K, Enc)}},
     {attrs, {blob, enc(attrs, As, Enc)}},
     {value, {blob, enc(value, V, Enc)}}];
mark_cols({K,V}, Enc) ->
    [{key, {blob, enc(key, K, Enc)}},
     {value, {blob, enc(value, V, Enc)}}].


queue_insert(#db{ref = Ref} = Db, Table, #q_key{} = QKey, St, Obj) when
      St==inactive; St==active; St==blocking ->
    Type = type(Db, Table),
    if Type == fifo; Type == lifo; element(1, Type) == keyed ->
	    Enc = encoding(Db, Table),
	    Key = kvdb_lib:q_key_to_actual(QKey, Enc, Type),
	    ActiveCol = case St of
			    inactive -> {active, 0};
			    active   -> {active, 1};
			    blocking -> {active, 2}
			end,
	    case insert_or_replace(
		   Ref, Table,
		   case Obj of
		       {_, Attrs, Value} ->
			   [{key, {blob, Key}},
			    {attrs, {blob, enc(attrs, Attrs, Enc)}},
			    {value, {blob, enc(value, Value, Enc)}},
			    ActiveCol];
		       {_, Value} ->
			   [{key, {blob, Key}},
			    {value, {blob, enc(value, Value, Enc)}},
			    ActiveCol]
		   end) of
		ok ->
		    ok;
		{error,Error} ->
		    erlang:error(Error)
	    end;
       true ->
	    erlang:error(badarg)
    end.

queue_delete(Db, Table, #q_key{} = QKey) ->
    _ = extract(Db, Table, QKey),
    ok.


do_pop(#db{} = Db, Table, Type, Q, Remove, ReturnKey) ->
    Enc = encoding(Db, Table),
    QPfx = kvdb_lib:queue_prefix(Enc, Q),
    EncQPfx = kvdb_lib:enc_prefix(key, QPfx, Enc),
    EncFirst = kvdb_lib:enc(key, queue_meta(Enc, Q, order(Type)), Enc),
    Rel = {true, EncFirst},
    Fltr = fun(inactive, _, _) -> skip;
	      (_, Kr, O) ->
		   {keep, {Kr,O}}
	   end,
    case case Type of
	     _ when Type==fifo; element(2, Type) == fifo ->
		 prefix_match_(Db, Table, EncQPfx, Rel, QPfx, Enc,
			       Fltr, true, 2, asc);
	     _ when Type==lifo; element(2, Type) == lifo ->
		 prefix_match_(Db, Table, EncQPfx, Rel, QPfx, Enc,
			       Fltr, true, 2, desc);
	     _ -> erlang:error(illegal)
	 end of
	{[{RawKey,Obj}|More], _} ->
	    Remove(setelement(1, Obj, RawKey), Enc),
	    if ReturnKey ->
		    QKey = kvdb_lib:split_queue_key(Enc,Type,RawKey),
		    {ok, Obj, QKey, More == []};
	       true ->
		    {ok, Obj, More == []}
	    end;
	{[], _} ->
	    done;
	blocked ->
	    blocked
    end.

extract(#db{ref = Ref} = Db, Table, #q_key{queue = Q} = QKey) ->
    Type = type(Db, Table),
    Enc = encoding(Db, Table),
    if Type == fifo; Type == lifo; element(1, Type) == keyed ->
	    case queue_read(Db, Table, QKey) of
		{ok, _, Obj} ->
		    Key = kvdb_lib:q_key_to_actual(QKey, Enc, Type),
                    sqlite3:delete(Ref, Table, {key,{blob, Key}}),
		    IsEmpty = is_queue_empty(Db, Table, Q),
		    {ok, Obj, Q, IsEmpty};
		{error, _} = Error ->
		    Error
	    end;
       true ->
	    erlang:error(illegal)
    end.

queue_read(#db{} = Db, Table, #q_key{key = K} = QKey) ->
    Type = type(Db, Table),
    Enc = encoding(Db, Table),
    if Type == fifo; Type == lifo; element(1, Type) == keyed ->
	    EncKey = kvdb_lib:q_key_to_actual(QKey, Enc, Type),
	    case get_(Db, Table, QKey, EncKey, Enc, _IncludeActive = true) of
		{ok, {St, Obj}} ->
		    {ok, St, setelement(1, Obj, K)};
		{error, _} = Error ->
		    Error
	    end;
       true ->
	    erlang:error(illegal)
    end.

queue_head_read(Db, Table, Queue) ->
    Type = type(Db, Table),
    HeadKey = kvdb_lib:q_head_key(Queue, Type),
    case queue_read(Db, Table, HeadKey) of
        {ok, _St, Obj} ->
            {ok, Obj};
        Other ->
            Other
    end.

queue_head_write(Db, Table, Queue, Obj) ->
    Type = type(Db, Table),
    Enc = encoding(Db, Table),
    case kvdb_lib:matches_encoding(Enc, Obj) of
        true ->
            queue_insert(
              Db, Table, kvdb_lib:q_head_key(Queue, Type), active, Obj);
        false ->
            erlang:error({encoding_mismatch, [Enc, Obj]})
    end.

queue_head_delete(Db, Table, Queue) ->
    Type = type(Db, Table),
    HeadKey = kvdb_lib:q_head_key(Queue, Type),
    queue_delete(Db, Table, HeadKey).
    

is_queue_empty(Db, Table, Q) ->
    case list_queue(Db, Table, Q, fun(inactive,_,_) -> skip;
				     (_, _, O) -> {keep,O}
				  end, true, 1) of
	{[_], _} ->
	    false;
	blocked ->
	    false;
	_ ->
	    true
    end.

list_queue(Db, Table, Q) ->
    list_queue(Db, Table, Q, infinity).

list_queue(Db, Table, Q, Limit) ->
    %% HeedBlock set to false by default. Note that we don't pass on the
    %% status flag by default. Is this a useful combination?
    list_queue(Db, Table, Q, fun(_,_,O) -> {keep,O} end, false, Limit).

list_queue(Db, Table, Q, Fltr, HeedBlock, Limit) ->
    list_queue(Db, Table, Q, Fltr, HeedBlock, Limit, false).

list_queue(Db, Table, Q, Fltr, HeedBlock, Limit, Reverse)
  when Limit > 0, is_boolean(Reverse) ->
    Type = type(Db, Table),
    Dir = kvdb_lib:queue_list_direction(Type, Reverse),
    list_queue_(Db, Table, Q, Fltr, HeedBlock, order(Dir), Limit).

list_queue_(Db, Table, Q, Fltr, HeedBlock, Order, Limit)
  when Order==asc; Order==desc ->
    Enc = encoding(Db, Table),
    QPfx = kvdb_lib:queue_prefix(Enc, Q),
    EncQPfx = kvdb_lib:enc_prefix(key, QPfx, Enc),
    EncFirst = kvdb_lib:enc(key, queue_meta(Enc, Q, Order), Enc),
    prefix_match_(Db, Table, EncQPfx, {true, EncFirst}, QPfx, Enc, Fltr,
		  HeedBlock, Limit, Order);
list_queue_(_, _, _, _, _, _, 0) ->
    [].

queue_meta(Enc, Q, asc ) -> kvdb_lib:queue_prefix(Enc, Q, first);
queue_meta(Enc, Q, desc) -> kvdb_lib:queue_prefix(Enc, Q, last).

order(fifo) -> asc;
order(lifo) -> desc;
order({_,fifo}) -> asc;
order({_,lifo}) -> desc;
order(_) -> error(illegal).


first_queue(#db{} = Db, Table) ->
    Type = type(Db, Table),
    case Type of
	set -> erlang:error(illegal);
	_ ->
	    Enc = encoding(Db, Table),
	    case first(Db, Table, Enc) of
		{ok, Obj} ->
		    Key = element(1, Obj),
		    #q_key{queue = Q} =
			kvdb_lib:split_queue_key(Enc, Type, Key),
		    {ok, Q};
		done ->
		    done
	    end
    end.

next_queue(Db, Table, Q) ->
    Enc = encoding(Db, Table),
    KEnc = key_encoding(Enc),
    Type = type(Db, Table),
    Pfx = if Type == fifo; Type == lifo ->
		  kvdb_lib:queue_prefix(KEnc, Q, last);
	     element(1, Type) == keyed, KEnc == raw ->
		  kvdb_lib:raw_queue_prefix(Q, last);
	     element(1, Type) == keyed, KEnc == sext ->
		  kvdb_lib:queue_prefix(KEnc, Q);
	     true ->
		  error(illegal)
	  end,
    EncPfx = enc_queue_prefix(Pfx, Enc),
    next_queue_(Db, Table, Q, EncPfx, Type, Enc).

next_queue_(Db, Table, Q, Prev, Type, Enc) ->
    case next_(Db, Table, Prev, Enc) of
	{ok, Obj} ->
	    Key = element(1, Obj),
	    case kvdb_lib:split_queue_key(Enc, Type, Key) of
		#q_key{queue = Q} ->
		    next_queue_(Db, Table, Q, Key, Type, Enc);
		#q_key{queue = NextQ} ->
		    {ok, NextQ}
	    end;
	done ->
	    done
    end.

enc_queue_prefix(Pfx, sext) when is_tuple(Pfx) ->
    EncPfx = sext:prefix(Pfx),
    %% add a byte that's guaranteed to be higher than the next sext-encoded
    %% element.
    <<EncPfx/binary, 127>>;
enc_queue_prefix(Pfx, raw) ->
    Pfx.

get_attrs(#db{ref = Ref} = Db, Table, Key, As) ->
    Enc = encoding(Db, Table),
    SQL = ["SELECT (attrs) FROM ", Table,
	   " WHERE key == ?"],
    case sqlite3:sql_exec(Ref, SQL, [{blob, enc(key, Key, Enc)}]) of
	[{columns,_},{rows,[{{blob,Attrs}}]}] ->
	    {ok, select_attrs(As, dec(attrs, Attrs, Enc))};
	_Other ->
	    {error, not_found}
    end.

select_attrs(all, Attrs) ->
    Attrs;
select_attrs(As, Attrs) ->
    [{K,V} || {K,V} <- Attrs,
	      lists:member(K, As)].

%% FIXME!! Must clear out indexes.
delete(#db{ref = Ref} = Db, Table, Key) ->
    Enc = encoding(Db, Table),
    case index(Db, Table) of
	{IxTable, Ix} ->
	    case get(Db, Table, Key) of
		{ok, {_, As, V}} ->
		    OldIxVals = kvdb_lib:index_vals(
				  Ix, Key, As,
				  fun() -> V end),
		    DelIxVals = [{I,Key} || I <- OldIxVals],
		    EncKey = enc(key, Key, Enc),
		    Results =
			sqlite3:sql_exec_script(
			  Ref,
			  ["BEGIN;",
			   [sqlite3_lib:delete_sql(
			      IxTable, "ix",
			      {blob, <<(sext:encode(I))/binary,
				       (sext:encode(Key))/binary>>}) ||
			       I <- DelIxVals],
			   sqlite3_lib:delete_sql(Table, "key",
						  {blob, EncKey}),
			   "COMMIT;"]),
		    [] = [X || X <- Results, X =/= ok],
		    ok;
		_ ->
		    sqlite3:delete(Ref, Table,
				   {key,{blob, enc(key, Key, Enc)}})
	    end;
	[] ->
	    sqlite3:delete(Ref, Table, {key,{blob, enc(key, Key, Enc)}})
    end.


prefix_match(Db, Table, Prefix) ->
    prefix_match(Db, Table, Prefix, 100).

prefix_match(Db, Table, Prefix, Limit) ->
    prefix_match_(Db, Table, Prefix, false, Limit, asc).

prefix_match_rel(Db, Table, Prefix, StartPoint, Limit) ->
    prefix_match_(Db, Table, Prefix, {true, StartPoint}, Limit, asc).

prefix_match_(Db, Table, Prefix, Rel, Limit, Dir) ->
    Enc = encoding(Db, Table),
    EncPrefix = enc_prefix(key, Prefix, Enc),
    EncStart = case Rel of
		   false -> false;
		   {true,StartPoint} ->
		       {true, enc(key, StartPoint, Enc)}
	       end,
    prefix_match_(Db, Table, EncPrefix, EncStart, Prefix, Enc,
		  fun(_,_,O) -> {keep,O} end, false, Limit, Dir).

prefix_match_(#db{ref = Ref} = Db, Table, EncPrefix, Rel, Prefix,
	      Enc, Fltr, HeedBlock, Limit, Dir)
  when (is_integer(Limit) orelse Limit==infinity) ->
    DirS = case Dir of
	       asc  -> "ASC";
	       desc -> "DESC"
	   end,
    AndStart = case Rel of
		   false -> "";
		   {true, _} when Dir==asc ->
		       " AND key > ?";
		   {true, _} when Dir==desc ->
		       " AND key < ?"
	       end,
    AndActive = case {HeedBlock, type(Db, Table)} of
		    {_, set} -> "";
		    {true,_} -> " AND active >= 1";
		    _ -> ""
		end,
    Type = type(Db, Table),
    SQL = ["SELECT ", sel_cols(Enc,Type), " FROM ", Table,
	   " WHERE key >= ?", AndStart, AndActive,
	   " ORDER BY key ", DirS],
    {ok, Handle} = sqlite3:prepare(Ref, SQL),
    Args = case Rel of
	       false ->
		   [{blob, EncPrefix}];
	       {true, StartPoint} ->
		   [{blob, EncPrefix}, {blob, StartPoint}]
	   end,
    ok = sqlite3:bind(Ref, Handle, Args),
    SH = track_resource(Ref, Handle),
    prefix_match_([], Dir, Ref, SH, Table, EncPrefix, Prefix, AndActive,
		  Enc, Fltr, HeedBlock, Type, Limit, Limit, []).

track_resource(Ref,Handle) ->
    %% spawn a resource monitor, since a lingering prepare statement may lock the
    %% database (I think...)
    Pid = spawn(fun() -> receive finalize ->
				 sqlite3:finalize(Ref, Handle)
			 end
		end),
    N = resource:notify_when_destroyed(Pid, finalize),
    {Pid, N, Handle}.

finalize(Ref, {Pid, _Trk, Handle}) ->
    exit(Pid, kill),
    sqlite3:finalize(Ref, Handle).

prefix_match_(Prev, Dir, Ref, Handle, Table, Pfx, Pfx0,
	      AndActive, Enc, Fltr, HeedBlock, Type, 0, Limit0, Acc) ->
    finalize(Ref, Handle),
    Cont = if Prev == []; Limit0 == 0 -> fun() -> done end;
	      true ->
		   SH = case Dir of
			    asc ->
				{ok, NewHandle} =
				    sqlite3:prepare(
				      Ref,
				      ["SELECT ",
				       sel_cols(Enc,Type),
				       " FROM ", Table,
				       " WHERE key > ?", AndActive,
				       " ORDER BY key ASC"]),
				ok = sqlite3:bind(Ref, NewHandle,
						  [{blob, Prev}]),
				track_resource(Ref, NewHandle);
			    desc ->
				{ok, NewHandle} =
				    sqlite3:prepare(
				      Ref,
				      ["SELECT ",
				       sel_cols(Enc,Type),
				       " FROM ", Table,
				       " WHERE key >= ? AND key < ?", AndActive,
				       " ORDER BY key DESC"]),
				ok = sqlite3:bind(Ref, NewHandle,
						  [{blob, Pfx}, {blob, Prev}]),
				track_resource(Ref, NewHandle)
			end,
		   fun() ->
			   prefix_match_(
			     Prev, Dir, Ref, SH, Table, Pfx, Pfx0, AndActive,
			     Enc, Fltr, HeedBlock, Type, Limit0, Limit0, [])
		   end
	   end,
    {lists:reverse(Acc), Cont};
    %% {lists:reverse(Acc), fun() ->
    %% 				 prefix_match_(Ref, Handle, Pfx, Pfx0, Enc,
    %% 					       Limit0, Limit0, [])
    %% 			 end};
prefix_match_(_Prev, Dir, Ref, {_, _, Handle} = SH, Table,
	      Pfx, Pfx0, AndActive, Enc, Fltr, HeedBlock, Type,
	      Limit, Limit0, Acc) ->
    case sqlite3:next(Ref, Handle) of
	done ->
	    finalize(Ref, SH),
	    {lists:reverse(Acc), fun() -> done end};
	Other ->
	    {blob,K} = element(1, Other),
	    case is_prefix(Pfx, K, Pfx0, Enc) of
		true ->
		    Status = obj_status(Other),
		    if HeedBlock, Status == blocking ->
			    if Acc == [] ->
				    blocked;
			       true ->
				    {lists:reverse(Acc),
				     fun() -> blocked end}
			    end;
		       true ->
			    Obj = decode_obj(Other, Enc),
			    AbsKey = element(1, Obj),
			    Obj1 = case Type of
				       set -> Obj;
				       _ ->
					   #q_key{key = Kx} =
					       kvdb_lib:split_queue_key(
						 Enc, Type, AbsKey),
					   setelement(1,Obj, Kx)
				   end,
			    {Cont,Acc1} = case Fltr(Status, AbsKey, Obj1) of
					      {keep, X} -> {true, [X|Acc]};
					      {stop, X} -> {false, [X|Acc]};
					      stop      -> {false, Acc};
					      skip      -> {true, Acc}
					  end,
			    case Cont of
				true ->
				    prefix_match_(
				      K, Dir, Ref, SH, Table,
				      Pfx, Pfx0, AndActive, Enc, Fltr,
				      HeedBlock, Type, decr(Limit),
				      Limit0, Acc1);
				false ->
				    finalize(Ref, SH),
				    {lists:reverse(Acc1), fun() -> done end}
			    end
		    end;
		false ->
		    if Dir == desc, K > Pfx ->
			    prefix_match_(K, Dir, Ref, SH, Table,
					  Pfx, Pfx0, AndActive, Enc,
					  Fltr, HeedBlock, Type, Limit,
					  Limit0, Acc);
		       true ->
			    finalize(Ref, SH),
			    {lists:reverse(Acc), fun() -> done end}
		    end
	    end
    end.

is_prefix(Pfx, Key, Prefix0, Enc) ->
    Sz = byte_size(Pfx),
    case Key of
	<< Pfx:Sz/binary, _/binary >> ->
	    kvdb_lib:is_prefix(Prefix0, Key, Enc);
	_ ->
	    false
    end.

decode_obj({{blob,K},{blob,V}, A}, Enc) when A==0; A==1; A==2 ->
    {dec(key,K,Enc), dec(value,V,Enc)};
decode_obj({{blob,K},{blob,V}}, Enc) ->
    {dec(key,K,Enc), dec(value,V,Enc)};
decode_obj({{blob,K},{blob,As},{blob,V}, A}, Enc) when A==0; A==1; A==2 ->
    {dec(key,K,Enc), dec(attrs, As, Enc), dec(value,V,Enc)};
decode_obj({{blob,K},{blob,As},{blob,V}}, Enc) ->
    {dec(key,K,Enc), dec(attrs, As, Enc), dec(value,V,Enc)}.

obj_status({{blob,_}, {blob,_}, 0}) -> inactive;
obj_status({{blob,_}, {blob,_}, 1}) -> active;
obj_status({{blob,_}, {blob,_}, 2}) -> blocking;
obj_status({{blob,_}, {blob,_}, {blob,_}, 0}) -> inactive;
obj_status({{blob,_}, {blob,_}, {blob,_}, 1}) -> active;
obj_status({{blob,_}, {blob,_}, {blob,_}, 2}) -> blocking;
obj_status(_) -> active.

decr(infinity) ->
    infinity;
decr(N) when is_integer(N), N > 0 ->
    N - 1.

first(Db, Table) ->
    first(Db, Table, encoding(Db, Table)).

first(#db{ref = Ref} = Db, Table, Enc) ->
    Where = case type(Db, Table) of
		set -> "";
		_ -> " WHERE active == 1"
	    end,
    select_one(Ref, Enc, ["SELECT ", sel_cols(Enc), " FROM ", Table, Where,
			 " ORDER BY key ASC LIMIT 1"]).

last(#db{ref = Ref} = Db, Table) ->
    Enc = encoding(Db, Table),
    Where = case type(Db, Table)==set of
		true -> "";
		false -> " WHERE active == 1"
	    end,
    select_one(Ref, Enc, ["SELECT ", sel_cols(Enc), " FROM ", Table, Where,
			 " ORDER BY key DESC LIMIT 1"]).

next(#db{} = Db, Table, Key) ->
    Enc = encoding(Db, Table),
    next_(Db, Table, enc(key, Key, Enc), Enc).

next_(#db{ref = Ref} = Db, Table, EncKey, Enc) ->
    IsActive = case type(Db, Table) == set of
		   true -> "";
		   false -> " AND active == 1"
	       end,
    select_one(Ref, Enc, ["SELECT ", sel_cols(Enc), " FROM ", Table,
			  " WHERE key > ?", IsActive,
			  " ORDER BY key ASC LIMIT 1"], [{blob, EncKey}]).



prev(#db{ref = Ref} = Db, Table, Key) ->
    Enc = encoding(Db, Table),
    IsActive = case type(Db, Table) == set of
		   true -> "";
		   false -> " AND active == 1"
	       end,
    select_one(Ref, Enc, ["SELECT ", sel_cols(Enc), " FROM ", Table,
			 " WHERE key < ?", IsActive,
			 " ORDER BY key DESC LIMIT 1"], [{blob, enc(key, Key, Enc)}]).

sel_cols({_,_,_}) ->
    "key, attrs, value";
sel_cols(_) ->
    "key, value".

sel_cols({_,_,_},set) -> "key, attrs, value";
sel_cols({_,_,_},  _) -> "key, attrs, value, active";
sel_cols(_,      set) -> "key, value";
sel_cols(_,        _) -> "key, value, active".





select_one(Ref, Enc, SQL) ->
    select_one(Ref, Enc, SQL, []).

select_one(Ref, Enc, SQL, Params) ->
    case sqlite3:sql_exec(Ref, SQL, Params) of
	[{columns, _},
	 {rows, []}] ->
	    done;
	[{columns, [_,_]},
	 {rows, [{{blob,Key},{blob,Value}}]}]->
	    {ok, {dec(key, Key, Enc), dec(value, Value, Enc)}};
	[{columns, [_,_,_]},
	 {rows, [{{blob,Key},{blob,Attrs},{blob,Value}}]}]->
	    {ok, {dec(key, Key, Enc), dec(attrs, Attrs, Enc), dec(value, Value, Enc)}};
	[{columns, [_]},
	 {rows, [{{blob,Key}}]}]->
	    {ok, dec(key, Key, Enc)}
    end.


check_options([{type, T}|Tl], Db, Rec)
  when T==set; T==lifo; T==fifo; T=={keyed,fifo}; T=={keyed,lifo} ->
    check_options(Tl, Db, Rec#table{type = T});
check_options([{encoding, E}|Tl], Db, Rec) ->
    Rec1 = Rec#table{encoding = E},
    kvdb_lib:check_valid_encoding(E),
    check_options(Tl, Db, Rec1);
check_options([{index, Ix}|Tl], Db, Rec) ->
    case kvdb_lib:valid_indexes(Ix) of
	ok -> check_options(Tl, Db, Rec#table{index = Ix});
	{error, Bad} ->
	    erlang:error({invalid_index, Bad})
    end;
check_options([], _, Rec) ->
    Rec.

ensure_schema(#db{ref = Ref} = Db) ->
    ETS = ets:new(kvdb_schema, [ordered_set, public]),
    Db1 = Db#db{metadata = ETS},
    case lists:member(
	   ?META_TABLE, [to_bin(T) || T <- sqlite3:list_tables(Ref)]) of
	false ->
	    Columns = [{key, blob, primary_key}, {value, blob}],
	    sqlite3:create_table(Ref, ?META_TABLE, Columns),
	    Tab = #table{name = ?META_TABLE,
			 encoding = sext, columns = [key,value]},
	    schema_write(Db1, {{table, ?META_TABLE}, Tab}),
	    schema_write(Db1, {{a, ?META_TABLE, encoding}, sext}),
	    Db1;
	true ->
	    [ets:insert(ETS, X) || X <- whole_table(Ref, ?META_TABLE, sext)],
	    Db1
    end.

to_bin(A) when is_atom(A) ->
    atom_to_binary(A, latin1);
to_bin(B) when is_binary(B) ->
    B;
to_bin(L) when is_list(L) ->
    list_to_binary(L).



whole_table(Ref, Table, Enc) ->
    SQL = ["SELECT ", sel_cols(Enc), " FROM ", Table],
    case sqlite3:sql_exec(Ref, SQL) of
	[{columns, [_,_,_]},
	 {rows, Rows}] ->
	    [{dec(key,K,Enc), dec(attrs,A,Enc), dec(value,V,Enc)} || {{blob,K},
								      {blob,A},
								      {blob,V}} <- Rows];
	[{columns, [_,_]},
	 {rows, Rows}] ->
	    [{dec(key,K,Enc), dec(value,V,Enc)} || {{blob,K},
						    {blob,V}} <- Rows]
    end.

schema_write(#db{metadata = ETS} = Db, Item) ->
    ets:insert(ETS, Item),
    put(Db, ?META_TABLE, Item).

is_table(#db{metadata = ETS}, Table) ->
    ets:member(ETS, {table, Table}).

schema_lookup(_, {a, ?META_TABLE,Attr}, Default) ->
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


%% Need to convert error result
%% sqlite_call(M,F,Args) ->
%%     case M:F(Args) of
%% 	{error, Int, String} -> {error, {Int, String}};
%% 	Other -> Other
%%     end.
