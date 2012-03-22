%%% @author Tony Rogvall <tony@rogvall.se>
%%% @copyright (C) 2011, Tony Rogvall
%%% @doc
%%%    SQLITE3 backend to kvdb
%%% @end
%%% Created : 29 Dec 2011 by Tony Rogvall <tony@rogvall.se>
%%% Hacked further by Ulf Wiger <ulf@feuerlabs.com>

-module(kvdb_sqlite3).

-behaviour(kvdb).

-export([open/2, close/1]).
-export([add_table/3, delete_table/2, list_tables/1,
	 get_attr/4, get_attrs/3, delete/3]).
-export([put/3, push/4, put_attr/5, put_attrs/4, get/3, pop/3, prel_pop/3, extract/3,
	 list_queue/3, list_queue/6, is_queue_empty/3,
	 first_queue/2, next_queue/3]).
-export([first/2, last/2, next/3, prev/3]).
-export([prefix_match/3, prefix_match/4]).
-export([info/2, get_schema_mod/2]).

%% for testing
-export([prefix_match/5]).

-import(kvdb_lib, [enc/3, dec/3, enc_prefix/3]).
-include("kvdb.hrl").
%-record(sqlite3_iter, {table, ref, db}).

get_schema_mod(_, M) ->
    M.

info(#db{ref = Db}, ref) -> Db;
info(#db{encoding = Enc}, encoding) -> Enc;
info(#db{}, _) -> undefined.

open(Db0, Options) ->
    Db = make_atom_name(Db0),
    E = proplists:get_value(encoding, Options, sext),
    kvdb_lib:check_valid_encoding(E),
    DbOptions = proplists:get_value(db_opts, Options, []),
    Res = case {proplists:get_value(file, Options),
		proplists:get_value(file, DbOptions)} of
	      {undefined,undefined} ->
		  sqlite3:open(Db, [{file,atom_to_list(Db)++".db"}|DbOptions]);
	      {F, undefined} ->
		  sqlite3:open(Db, [{file, F}|DbOptions]);
	      _ ->
		  sqlite3:open(Db, DbOptions)
	  end,
    case Res of
	{ok, Instance} ->
	    {ok, ensure_schema(#db{ref = Instance, encoding = E})};
	{error,_} = Error ->
	    Error
    end.

make_atom_name(A) when is_atom(A) ->
    A;
make_atom_name(B) when is_binary(B) ->
    binary_to_atom(B, latin1);
make_atom_name(X) ->
    list_to_atom(lists:flatten(io_lib:fwrite("~w", [X]))).




close(#db{ref = Db, metadata = ETS}) ->
    ets:delete(ETS),  % will crash if called several times
    sqlite3:close(Db).

add_table(#db{ref = Ref, encoding = Enc} = Db, Table, Opts) ->
    TabR = check_options(Opts, Db, #table{name = Table, encoding = Enc}),
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
		    schema_write(Db, {{table, Table}, TabR}),
		    schema_write(Db, {{Table, type}, TabR#table.type}),
		    schema_write(Db, {{Table, encoding}, TabR#table.encoding}),
		    ok;
		Error ->
		    Error
	    end
    end.


list_tables(#db{metadata = ETS}) ->
    ets:select(ETS, [{ {{table, '$1'}, '_'}, [{'=/=','$1',?SCHEMA_TABLE}], ['$1'] }]).


delete_table(#db{ref = Ref} = Db, Table) ->
    case schema_lookup(Db, {table, Table}, undefined) of
	undefined ->
	    ok;
	#table{} ->
	    sqlite3:drop_table(Ref, Table),
	    schema_delete(Db, {table, Table}),
	    schema_delete(Db, {Table, encoding}),
	    ok
    end.

put(#db{ref = Ref} = Db, Table, {Key, Value}) ->
    Enc = encoding(Db, Table),
    case insert_or_replace(Ref, Table, [{key, {blob, enc(key, Key, Enc)}},
					{value, {blob, enc(value, Value, Enc)}}]) of
	ok ->
	    ok;
	{error, _} = Error ->
	    Error
    end;
put(#db{ref = Ref} = Db, Table, {Key, Attrs, Value}) ->
    Enc = encoding(Db, Table),
    case insert_or_replace(Ref, Table, [{key, {blob, enc(key, Key, Enc)}},
					{attrs, {blob, enc(attrs, Attrs, Enc)}},
					{value, {blob, enc(value, Value, Enc)}}]) of
	ok ->
	    ok;
	{error,_} = Error ->
	    Error
    end.

push(#db{ref = Ref} = Db, Table, Q, {Key, Value}) ->
    Type = type(Db, Table),
    if Type == fifo; Type == lifo; element(1, Type) == keyed ->
	    Enc = encoding(Db, Table),
	    ActualKey = kvdb_lib:actual_key(Enc, Type, Q, Key),
	    case insert_or_replace(
		   Ref, Table, [{key, {blob, enc(key, ActualKey, Enc)}},
				{value, {blob, enc(value, Value, Enc)}}]) of
		ok ->
		    {ok, ActualKey};
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
	    ActualKey = kvdb_lib:actual_key(Enc, Type, Q, Key),
	    case insert_or_replace(
		   Ref, Table, [{key, {blob, enc(key, ActualKey, Enc)}},
				{attrs, {blob, enc(attrs, Attrs, Enc)}},
				{value, {blob, enc(value, Value, Enc)}}]) of
		ok ->
		    {ok, ActualKey};
		{error,_} = Error ->
		    Error
	    end;
       true ->
	    {error, badarg}
    end.

encoding(#db{encoding = Enc} = Db, Table) ->
    schema_lookup(Db, {Table, encoding}, Enc).

type(Db, Table) ->
    schema_lookup(Db, {Table, type}, set).

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


get(#db{ref = Ref} = Db, Table, Key) ->
    Enc = encoding(Db, Table),
    case sqlite3:read(Ref, Table, {key, {blob, enc(key, Key, Enc)}}) of
	[{columns,["key","value","active"]},{rows,[{_,{blob,Value},_}]}] ->
	    {ok, {Key, dec(value, Value, Enc)}};
	[{columns,_},{rows,[{_,{blob,Value}}]}] ->
	    {ok, {Key, dec(value, Value, Enc)}};
	[{columns,["key","attrs","value","active"]},
	 {rows,[{_,{blob,Attrs},{blob,Value},_}]}] ->
	    {ok, {Key, dec(attrs, Attrs, Enc), dec(value, Value, Enc)}};
	[{columns,["key","attrs","value"]},
	 {rows,[{_,{blob,Attrs},{blob,Value}}]}] ->
	    {ok, {Key, dec(attrs, Attrs, Enc), dec(value, Value, Enc)}};
	_ ->
	    {error,not_found}
    end.

pop(Db, Table, Q) ->
    case type(Db, Table) of
	set -> error(illegal);
	T ->
	    Remove = fun(Obj, _) ->
			     delete(Db, Table, element(1, Obj))
		     end,
	    do_pop(Db, Table, T, Q, Remove, false)
    end.

prel_pop(Db, Table, Q) ->
    case type(Db, Table) of
	set ->
	    error(illegal);
	T ->
	    Remove = fun(Obj, Enc) ->
			     mark_queue_object(Db, Table, Enc, Obj, inactive)
		     end,
	    do_pop(Db, Table, T, Q, Remove, true)
    end.

mark_queue_object(#db{ref = Ref}, Table, Enc, Obj, St) when St==inactive;
							    St==active ->
    ActiveCol = case St of
		    active   -> {active, 1};
		    inactive -> {active, 0}
		end,
    insert_or_replace(Ref, Table, cols(Obj, Enc) ++ [ActiveCol]).

cols({K,As,V}, Enc) ->
    [{key, {blob, enc(key, K, Enc)}},
     {attrs, {blob, enc(attrs, As, Enc)}},
     {value, {blob, enc(value, V, Enc)}}];
cols({K,V}, Enc) ->
    [{key, {blob, enc(key, K, Enc)}},
     {value, {blob, enc(value, V, Enc)}}].


do_pop(#db{} = Db, Table, Type, Q, Remove, ReturnKey) ->
    Enc = encoding(Db, Table),
    QPfx = kvdb_lib:queue_prefix(Enc, Q),
    EncQPfx = kvdb_lib:enc_prefix(key, QPfx, Enc),
    case case Type of
	     _ when Type==fifo; element(2, Type) == fifo ->
		 prefix_match(Db, Table, EncQPfx, QPfx, Enc,
			      fun(_,Kr,O) -> {keep,{Kr,O}} end,
			      false, 2, asc);
	     _ when Type==lifo; element(2, Type) == lifo ->
		 prefix_match(Db, Table, EncQPfx, QPfx, Enc,
			      fun(_,Kr,O) -> {keep,{Kr,O}} end,
			      false, 2, desc);
	     _ -> error(illegal)
	 end of
	{[{RawKey,Obj}|More], _} ->
	    Remove(setelement(1, Obj, RawKey), Enc),
	    if ReturnKey ->
		    {ok, Obj, RawKey, More == []};
	       true ->
		    {ok, Obj, More == []}
	    end;
	{[], _} ->
	    done;
	{error, _} = Error ->
	    Error
    end.

extract(#db{} = Db, Table, Key) ->
    Type = type(Db, Table),
    if Type == fifo; Type == lifo; element(1, Type) == keyed ->
	    case get(Db, Table, Key) of
		{ok, Obj} ->
		    K = element(1, Obj),
		    {Q, K1} = kvdb_lib:split_queue_key(
				encoding(Db, Table), Type, K),
		    delete(Db, Table, Key),
		    IsEmpty = is_queue_empty(Db, Table, Q),
		    {ok, setelement(1, Obj, K1), Q, IsEmpty};
		{error, _} = Error ->
		    Error
	    end;
       true ->
	    error(illegal)
    end.

is_queue_empty(Db, Table, Q) ->
    case list_queue(Db, Table, Q, 1) of
	{[_], _} ->
	    false;
	_ ->
	    true
    end.

list_queue(Db, Table, Q) ->
    list_queue(Db, Table, Q, infinity).

list_queue(Db, Table, Q, Limit) ->
    list_queue(Db, Table, Q, fun(_,_,O) -> {keep,O} end, false, Limit).

list_queue(Db, Table, Q, Fltr, Inactive, Limit) when Limit > 0 ->
    Type = type(Db, Table),
    list_queue(Db, Table, Q, Fltr, Inactive,
	       case Type of
		   fifo -> asc;
		   lifo -> desc;
		   {keyed,fifo} -> asc;
		   {keyed,lifo} -> desc;
		   _ -> error(illegal)
	       end, Limit).

list_queue(Db, Table, Q, Fltr, Inactive, Order, Limit)
  when Order==asc; Order==desc ->
    Enc = encoding(Db, Table),
    QPfx = kvdb_lib:queue_prefix(Enc, Q),
    EncQPfx = kvdb_lib:enc_prefix(key, QPfx, Enc),
    prefix_match(Db, Table, EncQPfx, QPfx, Enc, Fltr, Inactive, Limit, Order);
list_queue(_, _, _, _, _, _, 0) ->
    [].

first_queue(#db{} = Db, Table) ->
    Type = type(Db, Table),
    case Type of
	set -> error(illegal);
	_ ->
	    Enc = encoding(Db, Table),
	    case first(Db, Table, Enc) of
		{ok, Obj} ->
		    Key = element(1, Obj),
		    {Q, _} = kvdb_lib:split_queue_key(Enc, Type, Key),
		    {ok, Q};
		done ->
		    done
	    end
    end.

next_queue(Db, Table, Q) ->
    Enc = encoding(Db, Table),
    RelK = case list_queue(Db, Table, Q,
			   fun(_,K,_O) -> {keep,K} end, false, desc, 1) of
	       {[Last],_} ->
		   Last;
	       _ ->
		   kvdb_lib:queue_prefix(Enc, Q)
	   end,
    case next(Db, Table, RelK) of
	{ok, Obj} ->
	    Type = type(Db, Table),
	    {Q1,_} = kvdb_lib:split_queue_key(Enc, Type, element(1, Obj)),
	    {ok, Q1};
	done ->
	    done
    end.

get_attrs(#db{ref = Ref} = Db, Table, Key) ->
    Enc = encoding(Db, Table),
    SQL = ["SELECT (attrs) FROM ", Table,
	   " WHERE key == ?"],
    case sqlite3:sql_exec(Ref, SQL, [{blob, enc(key, Key, Enc)}]) of
	[{columns,_},{rows,[{{blob,Attrs}}]}] ->
	    {ok, dec(attrs, Attrs, Enc)};
	_Other ->
	    {error, not_found}
    end.

put_attr(#db{} = Db, Table, Key, Attr, Value) ->
    case encoding(Db, Table) of
	{_, _, _} ->
	    case get(Db, Table, Key) of
		{ok, {K,As0,V}} ->
		    As1 = lists:keysort(1, lists:keystore(Attr, 1, As0, {Attr, Value})),
		    put(Db, Table, {K,As1,V});
		_ ->
		    {error, not_found}
	    end;
	_ ->
	    {error, illegal}
    end.



put_attrs(#db{} = Db, Table, Key, Attrs) when is_list(Attrs) ->
    case encoding(Db, Table) of
	{_, _, _} ->
	    case get(Db, Table, Key) of
		{ok, {K,_,V}} ->
		    put(Db, Table, {K,Attrs,V});
		_ ->
		    {error, not_found}
	    end;
	_ ->
	    {error, illegal}
    end.


get_attr(Db, Table, Key, Attr) ->
    case get_attrs(Db, Table, Key) of
	{ok, As} ->
	    case lists:keysearch(Attr, 1, As) of
		false ->
		    {error, not_found};
		{value, {_, Value}} ->
		    {ok, Value}
	    end;
	Error ->
	    Error
    end.

delete(#db{ref = Ref} = Db, Table, Key) ->
    Enc = encoding(Db, Table),
    sqlite3:delete(Ref, Table, {key,{blob, enc(key, Key, Enc)}}).


prefix_match(Db, Table, Prefix) ->
    prefix_match(Db, Table, Prefix, 100).

prefix_match(Db, Table, Prefix, Limit) ->
    prefix_match(Db, Table, Prefix, Limit, asc).

prefix_match(Db, Table, Prefix, Limit, Dir) ->
    Enc = encoding(Db, Table),
    EncPrefix = enc_prefix(key, Prefix, Enc),
    prefix_match(Db, Table, EncPrefix, Prefix, Enc,
		 fun(_,_,O) -> {keep,O} end, false, Limit, Dir).

prefix_match(#db{ref = Ref} = Db, Table, EncPrefix, Prefix,
	     Enc, Fltr, Inactive, Limit, Dir)
  when (is_integer(Limit) orelse Limit==infinity) ->
    DirS = case Dir of
	       asc  -> "ASC";
	       desc -> "DESC"
	   end,
    AndActive = case {Inactive, type(Db, Table)} of
		    {_, set} -> "";
		    {true,_} -> "";
		    _ -> " AND active == 1"
		end,
    Type = type(Db, Table),
    SQL = ["SELECT ", sel_cols(Enc,Type,Inactive), " FROM ", Table,
	   " WHERE key >= ?", AndActive,
	   " ORDER BY key ", DirS],
    {ok, Handle} = sqlite3:prepare(Ref, SQL),
    ok = sqlite3:bind(Ref, Handle, [{blob, EncPrefix}]),
    SH = track_resource(Ref, Handle),
    prefix_match_([], Dir, Ref, SH, Table, EncPrefix, Prefix, AndActive,
		  Enc, Fltr, Inactive, Type, Limit, Limit, []).

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
	      AndActive, Enc, Fltr, Inactive, Type, 0, Limit0, Acc) ->
    finalize(Ref, Handle),
    Cont = if Prev == []; Limit0 == 0 -> fun() -> done end;
	      true ->
		   SH = case Dir of
			    asc ->
				{ok, NewHandle} =
				    sqlite3:prepare(
				      Ref,
				      ["SELECT ",
				       sel_cols(Enc,Type,Inactive),
				       " FROM ", Table,
				       " WHERE key > ?", AndActive,
				       " ORDER BY key ASC"]),
				ok = sqlite3:bind(Ref, NewHandle, [{blob, Prev}]),
				track_resource(Ref, NewHandle);
			    desc ->
				{ok, NewHandle} =
				    sqlite3:prepare(
				      Ref,
				      ["SELECT ",
				       sel_cols(Enc,Type,Inactive),
				       " FROM ", Table,
				       " WHERE key >= ? AND key < ?", AndActive,
				       " ORDER BY key DESC"]),
				ok = sqlite3:bind(Ref, NewHandle, [{blob, Pfx},
								   {blob, Prev}]),
				track_resource(Ref, NewHandle)
			end,
		   fun() ->
			   prefix_match_(
			     Prev, Dir, Ref, SH, Table, Pfx, Pfx0, AndActive,
			     Enc, Fltr, Inactive, Type, Limit0, Limit0, [])
		   end
	   end,
    {lists:reverse(Acc), Cont};
    %% {lists:reverse(Acc), fun() ->
    %% 				 prefix_match_(Ref, Handle, Pfx, Pfx0, Enc,
    %% 					       Limit0, Limit0, [])
    %% 			 end};
prefix_match_(_Prev, Dir, Ref, {_, _, Handle} = SH, Table,
	      Pfx, Pfx0, AndActive, Enc, Fltr, Inactive, Type,
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
		    Obj = decode_obj(Other, Enc),
		    AbsKey = element(1, Obj),
		    Obj1 = case Type of
			       set -> Obj;
			       _ ->
				   {_,Kx} =
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
			      Inactive, Type, decr(Limit), Limit0, Acc1);
			false ->
			    finalize(Ref, SH),
			    {lists:reverse(Acc1), fun() -> done end}
		    end;
		false ->
		    if Dir == desc, K > Pfx ->
			    prefix_match_(K, Dir, Ref, SH, Table,
					  Pfx, Pfx0, AndActive, Enc, Fltr,
					  Inactive, Type, Limit, Limit0, Acc);
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

decode_obj({{blob,K},{blob,V}, A}, Enc) when A==0; A==1 ->
    {dec(key,K,Enc), dec(value,V,Enc)};
decode_obj({{blob,K},{blob,V}}, Enc) ->
    {dec(key,K,Enc), dec(value,V,Enc)};
decode_obj({{blob,K},{blob,As},{blob,V}, A}, Enc) when A==0; A==1 ->
    {dec(key,K,Enc), dec(attrs, As, Enc), dec(value,V,Enc)};
decode_obj({{blob,K},{blob,As},{blob,V}}, Enc) ->
    {dec(key,K,Enc), dec(attrs, As, Enc), dec(value,V,Enc)}.

obj_status({{blob,_}, {blob,_}, 0}) -> inactive;
obj_status({{blob,_}, {blob,_}, {blob,_}, 0}) -> inactive;
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

last(Db, Table) ->
    last(Db, Table, false).

last(#db{ref = Ref} = Db, Table, InclAll) when is_boolean(InclAll) ->
    Enc = encoding(Db, Table),
    Where = case (InclAll orelse type(Db, Table)==set) of
		true -> "";
		false -> " WHERE active == 1"
	    end,
    select_one(Ref, Enc, ["SELECT ", sel_cols(Enc), " FROM ", Table, Where,
			 " ORDER BY key DESC LIMIT 1"]).

next(Db, Table, Key) ->
    next(Db, Table, Key, false).

next(#db{ref = Ref} = Db, Table, Key, InclAll) when is_boolean(InclAll) ->
    Enc = encoding(Db, Table),
    IsActive = case (InclAll orelse type(Db, Table) == set) of
		   true -> "";
		   false -> " AND active == 1"
	       end,
    select_one(Ref, Enc, ["SELECT ", sel_cols(Enc), " FROM ", Table,
			 " WHERE key > ?", IsActive,
			 " ORDER BY key ASC LIMIT 1"], [{blob, enc(key, Key, Enc)}]).

prev(Db, Table, Key) ->
    prev(Db, Table, Key, false).

prev(#db{ref = Ref} = Db, Table, Key, InclAll) when is_boolean(InclAll) ->
    Enc = encoding(Db, Table),
    IsActive = case (InclAll orelse type(Db, Table) == set) of
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

sel_cols({_,_,_},set,     _) -> "key, attrs, value";
sel_cols({_,_,_},  _, false) -> "key, attrs, value";
sel_cols({_,_,_},  _, true ) -> "key, attrs, value, active";
sel_cols(_,        _, true ) -> "key, value, active";
sel_cols(_,      set,     _) -> "key, value";
sel_cols(_,        _, false) -> "key, value".





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
check_options([], _, Rec) ->
    Rec.

ensure_schema(#db{ref = Ref} = Db) ->
    ETS = ets:new(kvdb_schema, [ordered_set]),
    Db1 = Db#db{metadata = ETS},
    case lists:member(?SCHEMA_TABLE, [to_bin(T) || T <- sqlite3:list_tables(Ref)]) of
	false ->
	    Columns = [{key, blob, primary_key}, {value, blob}],
	    sqlite3:create_table(Ref, ?SCHEMA_TABLE, Columns),
	    Tab = #table{name = ?SCHEMA_TABLE, encoding = sext, columns = [key,value]},
	    schema_write(Db1, {{table, ?SCHEMA_TABLE}, Tab}),
	    schema_write(Db1, {{?SCHEMA_TABLE, encoding}, sext}),
	    Db1;
	true ->
	    [ets:insert(ETS, X) || X <- whole_table(Ref, ?SCHEMA_TABLE, sext)],
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
    put(Db, ?SCHEMA_TABLE, Item).

schema_lookup(_, {?SCHEMA_TABLE,Attr}, Default) ->
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


