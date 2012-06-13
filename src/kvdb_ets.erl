%%% @author Tony Rogvall <tony@rogvall.se>
%%% @author Ulf Wiger <ulf@feuerlabs.com>
%%% @copyright (C) 2011-12, Feuerlabs, Inc
%%% @doc
%%%    ETS backend to kvdb
%%% @end
%%% Created : 29 Dec 2011 by Tony Rogvall <tony@rogvall.se>

-module(kvdb_ets).

-behaviour(kvdb).

-export([open/2, close/1]).
-export([add_table/3, delete_table/2, list_tables/1]).
-export([put/3, get/3, delete/3, update_counter/4]).
-export([push/4, pop/3, prel_pop/3, extract/3]).
-export([list_queue/3, list_queue/6]).
-export([first/2, last/2, next/3, prev/3]).
-export([get_schema_mod/2]).
-export([info/2,
	 is_table/2,
	 dump_tables/1]).

-export([int_read/2,
	 int_write/2]).

-include("kvdb.hrl").

-import(kvdb_lib, [enc/3, dec/3]).

get_schema_mod(Db, Default) ->
    Default.

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
	_ -> undefined
    end.

is_table(#db{ref = ETS}, Tab) ->
    ets:member(ETS, schema_key({table, Tab})).

if_table(Db, Tab, F) ->
    case is_table(Db, Tab) of
	true -> F();
	false -> undefined
    end.

dump_tables(#db{ref = Ref}) ->
    %% FIXME: improve later, e.g. doing decode when necessary
    ets:tab2list(Ref).

open(DbName0, Options) ->
    Enc = proplists:get_value(encoding, Options, raw),
    kvdb_lib:check_valid_encoding(Enc),
    DbName = make_atom_name(DbName0),
    Ets = ets:new(DbName, [ordered_set,public]),
    Db = ensure_schema(#db{ref = Ets, encoding = Enc}),
    FileName = file_name(DbName, Options),
    FileMode  = file_mode(Options, _ReadOnly = true),
    case proplists:get_value(file, Options) of
	undefined ->
	    {ok, Db};
	Filename ->
	    try file:open(FileName, FileMode) of
		{ok,Fd} ->
		    Result = file_load(Fd, Ets),
		    file:close(Fd),
		    case Result of
			ok ->
			    schema_write(Db, {save_mode,
					      {Filename, FileMode}}),
			    {ok, Db};
			Error ->
			    ets:delete(Ets),
			    Error
		    end;
		Error ->
		    Error
	    catch
		error:E ->
		    ets:delete(Ets),
		    {error, E}
	    end
    end.

close(#db{ref = Ets} = Db) ->
    case save_mode(Db) of
	{Filename, [read, raw, binary]} ->
	    ets:delete(Ets);
	{FileName, [read,write,raw,binary]} ->
	    case file:open(FileName, [write,raw,binary]) of
		{ok,Fd} ->
		    %% FIXME: only save if changed!!
		    Res = file_save(Fd, Db),
		    file:close(Fd),
		    ets:delete(Ets),
		    Res;
		Error ->
		    Error
	    end;
	_ ->
	    ets:delete(Ets)
    end.

%% flush? to do a save?

add_table(#db{ref = Ets} = Db, Table, Opts) ->
    TabR0 = check_options(Opts, Db, #table{name = Table}),
    TabR =
	case {lists:keyfind(type,1,Opts),
	      lists:keyfind(encoding,1,Opts)} of
	    {{_,T}, false} when T==fifo;
				T==lifo;
				element(1,T)==fifo;
				element(1,T)==lifo ->
		TabR0#table{encoding = {sext,sext,sext}};
	    _ ->
		TabR0
	end,
    case schema_lookup(Db, {table, Table}, undefined) of
	Tr when Tr =/= undefined ->
	    ok;
	undefined ->
	    [schema_write(Db, {K,V}) ||
		{K,V} <- [{{table,Table}, TabR},
			  {{a,Table,type}, TabR#table.type},
			  {{a,Table,index}, TabR#table.index},
			  {{a,Table,encoding}, TabR#table.encoding}]],
	    ok
    end.

schema_lookup(#db{ref = Ets}, Key, Default) ->
    case ets:lookup(Ets, schema_key(Key)) of
	[] ->
	    Default;
	[{_, V}] ->
	    V
    end.

schema_write(#db{ref = Ets}, {Key, Value}) ->
    ets:insert(Ets, {schema_key(Key), Value}).

schema_key(K) when is_atom(K) -> K;
schema_key({K1, K2}    ) -> {'-schema', K1, K2};
schema_key({K1, K2, K3}) -> {'-schema', K1, K2, K3}.

type(Db, Table) -> schema_lookup(Db, {a, Table, type}, undefined).
index(Db, Table) -> schema_lookup(Db, {a, Table, index}, []).
encoding(Db, Table) -> schema_lookup(Db, {a, Table, encoding}, raw).
schema(Db, Table) -> schema_lookup(Db, {a, Table, schema}, []).
save_mode(Db) -> schema_lookup(Db, save_mode, undefined).


delete_table(#db{ref = Ets} = Db, Table) ->
    case schema_lookup(Db, {table, Table}, undefined) of
	undefined -> ok;
	#table{} ->
	    ets:select_delete(
	      Ets, [{ {{Table,'_','_'},'_'}, [], [true] },
		    { {{'-ix',Table,'_'}}, [], [true] },
		    { {{'-schema',table,Table}, '_'},[], [true] },
		    { {{'-schema',a,Table,'_'},'_'}, [], [true] }])
    end,
    ets:delete(Db, {Table,table}).

list_tables(#db{ref = Ets}) ->
    ets:select(Ets, [{ {{'-schema',table,'$1'},'_'}, [], ['$1'] }]).

put(Db, Table, Obj) ->
    case type(Db, Table) of
	set -> put_(Db, Table, Obj);
	_ -> {error, illegal}
    end.

put_(#db{ref = Ets} = Db, Table, {K, Attrs, Value}) ->
    try
	case Enc = encoding(Db, Table) of
	    {_,_,_} -> ok;
	    _ -> throw({error, illegal})
	end,
	Key = enc(key, K, Enc),
	case index(Db, Table) of
	    [] ->
		ets:insert(Ets, {{Table,2,Key}, Attrs, Value});
	    Ix ->
		OldAttrs = try ets:lookup_element(Ets, {Table, 2, Key}, 2)
			   catch
			       error:_ -> []
			   end,
		OldIxVals = kvdb_lib:index_vals(Ix, OldAttrs),
		NewIxVals = kvdb_lib:index_vals(Ix, Attrs),
		[ets:delete(Ets, {'-ix', Table, {I, Key}}) ||
		    I <- OldIxVals -- NewIxVals],
		NewIxVals = [{{'-ix',Table, {I, Key}}} ||
				I <- NewIxVals -- OldIxVals],
		ets:insert(Ets, [{{Table, 2, Key}, Attrs, Value} | NewIxVals])
	end,
	ok
    catch
	throw:E ->
	    E
    end;
put_(#db{ref = Ets} = Db, Table, {K, Value}) ->
    case encoding(Db, Table) of
	{_, _, _} ->
	    {error, illegal};
	Enc ->
	    ets:insert(Ets, {{Table, 2, enc(key, K, Enc)}, Value}),
	    ok
    end.


update_counter(#db{ref = Ets} = Db, Table, K, Incr) when is_integer(Incr) ->
    case type(Db, Table) of
	set ->
	    case get(Db, Table, K) of
		{ok, Obj} ->
		    Sz = size(Obj),
		    V = element(Sz, Obj),
		    NewV =
			if is_integer(V) ->
				V + Incr;
			   is_binary(V) ->
				BSz = bit_size(V),
				<<I:BSz/integer>> = V,
				NewI = I + Incr,
				<<NewI:Sz/integer>>;
			   true ->
				error(illegal)
			end,
		    NewObj = setelement(Sz, Obj, NewV),
		    put(Db, Table, NewObj),
		    NewV;
		_ ->
		    error(not_found)
	    end;
	_ ->
	    error(illegal)
    end.



push(#db{ref = Ets} = Db, Table, Q, Obj) ->
    Type = type(Db, Table),
    Enc = encoding(Db, Table),
    if Type == fifo; Type == lifo; element(1, Type) == keyed ->
	    case matches_encoding(Db, Table, Obj) of
		true ->
		    ActualKey = kvdb_lib:actual_key(
				  sext, Type, {Q,1},
				  enc(key, element(1,Obj), Enc)),
		    InsertKey = {Table, 2, ActualKey},
		    Data = case Obj of
			       {_, As, Value} ->
				   {InsertKey, As, {active, Value}};
			       {_, Value} ->
				   {InsertKey, {active, Value}}
			   end,
		    ets:insert(Ets, Data),
		    {ok, ActualKey};
		_ ->
		    {error, badarg}
	    end;
       true ->
	    {error, badarg}
    end.


pop(Db, Table, Q) ->
    case type(Db, Table) of
	set -> {error, illegal};
	undefined -> {error, badarg};
	T ->
	    Remove = fun(_Obj, RawKey) ->
			     delete(Db, Table, RawKey)
		     end,
	    do_pop(Db, Table, T, Q, Remove, false)
    end.

prel_pop(Db, Table, Q) ->
    case type(Db, Table) of
	set -> {error, illegal};
	undefined -> {error, badarg};
	T ->
	    Remove = fun(Obj, RawKey) ->
			     io:fwrite("Remove(~p, ~p)~n", [Obj, RawKey]),
			     mark_queue_object(
			       Db, Table,
			       setelement(1, Obj, {Table,2,RawKey}), blocking)
		     end,
	    do_pop(Db, Table, T, Q, Remove, true)
    end.

mark_queue_object(#db{ref = Ets}, Table, Obj, St) when St == inactive;
						       St == blocking;
						       St == active ->
    io:fwrite("mark_queue_object(~p)~n", [Obj]),
    VPos = size(Obj),
    Val = element(VPos, Obj),
    ets:insert(Ets, setelement(VPos, Obj, {St, Val})).

do_pop(#db{ref = Ets} = Db, Table, Type, Q, Remove, ReturnKey) ->
    Enc = encoding(Db, Table),
    io:fwrite("Type = ~p~n", [Type]),
    {First,Next} =
	case Type of
	    _ when Type == fifo; element(1,Type) == fifo ->
		{fun() -> ets:next(Ets, {Table, 2, {{Q,0},0,0}}) end,
		 fun(K) -> ets:next(Ets, {Table, 2, K}) end};
	    _ when Type == lifo; element(1,Type) == lifo ->
		{fun() -> ets:prev(Ets, {Table, 2, {{Q,2},0,0}}) end,
		 fun(K) -> ets:prev(Ets, {Table, 2, K}) end};
	    _ -> error(illegal)
	 end,
    case do_pop_(First(), Table, Q, Next, Ets, Type) of
	blocked -> blocked;
	done -> done;
	{Obj, RawKey} ->
	    IsEmpty = case do_pop_(Next(RawKey), Table, Q, Next, Ets, Type) of
			  {_, _} -> false;
			  blocked -> false;
			  _ -> true
		      end,
	    Remove(Obj, RawKey),
	    if ReturnKey ->
		    {ok, Obj, RawKey, IsEmpty};
	       true ->
		    {ok, Obj, IsEmpty}
	    end
    end.

do_pop_(TKey, Table, Q, Next, Ets, T) ->
    case TKey of
	{Table, 2, {{Q,1},_,_} = RawKey} ->
	    K = if element(1, T) == keyed -> element(2, RawKey);
		   true -> element(3, RawKey)
		end,
	    case ets:lookup(Ets, TKey) of
		[{_, _, {blocked,_}}] -> blocked;
		[{_, {blocked,_}}] -> blocked;
		[{_, _, {inactive, _}}] ->
		    do_pop_(Next(TKey), Table, Q, Next, Ets, T);
		[{_, Attrs, {active, V}}] ->
		    {{K, Attrs, V}, RawKey};
		[{_, {active, V}}] ->
		    {{K, V}, RawKey}
	    end;
	_ ->
	    done
    end.

extract(#db{ref = Ets} = Db, Table, Key) ->
    Type = type(Db, Table),
    if Type == fifo; Type == lifo; element(1, Type) == keyed ->
	    case get(Db, Table, Key) of
		{ok, Obj} ->
		    K = element(1, Obj),
		    {{Q,_},K1} = kvdb_lib:split_queue_key(sext, Type, K),
		    ets:delete(Ets, {Table, 2, Key}),
		    IsEmpty = is_queue_empty(Db, Table, Q),
		    {ok, setelement(1, Obj, K1), Q, IsEmpty};
		{error, _} = Error ->
		    Error
	    end;
       true ->
	    error(illegal)
    end.

is_queue_empty(#db{ref = Ets}, Table, Q) ->
    Guard = [{'or', {'==', '$1', blocking}, {'==', '$1', active}}],
    case ets:select(
	   Ets, [{ {{Table,2,{{Q,1},'_','_'}},'_',{'$1','_'}}, Guard, [1] },
		 { {{Table,2,{{Q,1},'_','_'}},{'$1','_'}}, Guard, [1]}], 1) of
	{[_], _} ->
	    false;
	_ ->
	    true
    end.

list_queue(Db, Table, Q) ->
    list_queue(Db, Table, Q, infinity).

list_queue(Db, Table, Q, Limit) ->
    list_queue(Db, Table, Q, fun(_, _, O) -> {keep,O} end, false, Limit).

list_queue(#db{ref = Ets} = Db, Table, Q, Fltr, HeedBlock, Limit)
  when Limit > 0 ->
    T = type(Db, Table),
    {First,Next} =
	case T of
	    _ when T == fifo; element(1,T) == fifo ->
		{fun() -> ets:next(Ets, {Table, 2, {{Q,0},0,0}}) end,
		 fun(K) -> ets:next(Ets, {Table, 2, K}) end};
	    _ when T == lifo; element(1,T) == lifo ->
		{fun() -> ets:prev(Ets, {Table, 2, {{Q,2},0,0}}) end,
		 fun(K) -> ets:prev(Ets, {Table, 2, K}) end};
	    _ -> error(illegal)
	end,
    list_queue(Limit, First(), Next, Ets, Table, T, Q, Fltr, HeedBlock,
	       Limit, []).

list_queue(Limit, {Table,2,{{Q,1},_,_} = AbsKey} = K, Next, Ets, Table, T, Q,
	   Fltr, HeedBlock, Limit0, Acc)
  when (is_integer(Limit) andalso Limit > 0) orelse Limit==infinity ->
    [Obj] = ets:lookup(Ets, K),
    {St,V} = element(size(Obj), Obj),
    if HeedBlock, St == blocking ->
	    if Acc == [] ->
		    blocked;
	       true ->
		    {lists:reverse(Acc), fun() -> blocked end}
	    end;
       true ->
	    {_, Kx} = kvdb_lib:split_queue_key(sext, T, AbsKey),
	    {Cont,Acc1} = case Fltr(St, AbsKey, setelement(1, Obj, Kx)) of
			      {keep, X} -> {true,  [X|Acc]};
			      {stop, X} -> {false, [X|Acc]};
			      stop      -> {false, Acc};
			      skip      -> {false, Acc}
			  end,
	    case Cont of
		true ->
		    case decr(Limit) of
			0 ->
			    {lists:reverse(Acc1),
			     fun() ->
				     list_queue(Limit0, Next(AbsKey), Next,
						Ets, Table, T, Q, Fltr,
						HeedBlock, Limit0, [])
			     end};
			Limit1 ->
			    list_queue(Limit1, Next(AbsKey), Next, Ets, Table,
				       T, Q, Fltr, HeedBlock, Limit0, Acc1)
		    end;
		false ->
		    {lists:reverse(Acc1), fun() -> done end}
	    end
    end;
list_queue(_, _, _, _, _, _, _, _, _, _, Acc) ->
    {lists:reverse(Acc), fun() -> done end}.

decr(infinity) -> infinity;
decr(I) when is_integer(I) -> I - 1.

matches_encoding(Db, Table, Obj) ->
    case {encoding(Db, Table), Obj} of
	{{_,_,_}, {_,_,_}} ->
	    true;
	{_, {_,_}} ->
	    true;
	_ ->
	    false
    end.

get(#db{ref = Ets} = Db, Table, Key) ->
    Enc = encoding(Db, Table),
    EncKey = enc(key, Key, Enc),
    case ets:lookup(Ets, {Table,2,EncKey}) of
	[] ->
	    {error,not_found};
	[{_, Value}] ->
	    {ok,{Key, Value}};
	[{_, As, Value}] ->
	    {ok, {Key, As, Value}}
    end.

index_get(#db{ref = Ets} = Db, Table, IxName, IxVal) ->
    case index(Db, Table) of
	[] ->
	    {error, no_index};
	[_|_] = Ix ->
	    case lists:member(IxName, Ix) orelse
		lists:keymember(IxName, 1, Ix) of
		true ->
		    Keys =
			ets:select(
			  Ets, [{ {{'-ix',Table,{{IxName,IxVal},'$1'}}},
				  [], ['$1'] }]),
		    lists:foldr(
		      fun(K, Acc) ->
			      case ets:lookup(Ets, {Table, 2, K}) of
				  [] -> Acc;
				  [{_,_,_} = Obj] -> [Obj|Acc]
			      end
		      end, [], Keys);
		false ->
		    {error, invalid_index}
	    end
    end.

delete(Db, Table, Key) ->
    case type(Db, Table) of
	set ->
	    delete_(Db, Table, Key);
	T when T==fifo; element(1,T) == fifo ->
	    delete_q_entry(Db, Table, Key);
	T when T==lifo; element(1,T) == lifo ->
	    delete_q_entry(Db, Table, Key);
	undefined -> {error, badarg};
	_ -> {error, illegal}
    end.

delete_(#db{ref = Ets} = Db, Table, K) ->
    Enc = encoding(Db, Table),
    Key = enc(key, K, Enc),
    case index(Db, Table) of
	[] ->
	    ets:delete(Ets, {Table,2,Key});
	Ix ->
	    OldAttrs = case ets:lookup(Ets, {Table, 2, Key}) of
			   [{_, OldAs, _}] -> OldAs;
			   _ -> []
		       end,
	    OldIxVals = kvdb_lib:index_vals(Ix, OldAttrs),
	    [ets:delete(Ets, {'-ix',Table,{I,Key}}) || I <- OldIxVals],
	    ets:delete(Ets, {Table, 2, Key})
    end,
    ok.

delete_q_entry(#db{ref = Ets}, Table, {{_,1},TS,_} = K) when is_integer(TS) ->
    ets:delete(Ets, {Table, 2, K});
delete_q_entry(_, _, _) ->
    {error, badarg}.


first(#db{ref = Ets}, Table) ->
    case ets:select(Ets, [{ {{Table,2,'_'},'_'}, [], ['$_'] }], 1) of
	{[Obj], _} ->
	    {Table,_,K} = element(1, Obj),
	    {ok, setelement(1, Obj, K)};
	_ ->
	    done
    end.

next(#db{ref = Ets} = Db, Table, RelKey) ->
    Enc = encoding(Db, Table),
    EncRelKey = enc(key, RelKey, Enc),
    case ets:next(Ets, {Table, 2, EncRelKey}) of
	{Table, 2, K} = Next ->
	    [Obj] = ets:lookup(Ets, Next),
	    {ok, setelement(1, Obj, dec(key, K, Enc))};
	_ ->
	    done
    end.

last(#db{ref = Ets} = Db, Table) ->
    Enc = encoding(Db, Table),
    case ets:prev(Ets, {Table,3,0}) of
	{Table,2,K} = Prev ->
	    [Obj] = ets:lookup(Ets, Prev),
	    {ok, setelement(1, Obj, dec(key, K, Enc))};
	_ ->
	    done
    end.

prev(#db{ref = Ets} = Db, Table, Rel) ->
    Enc = encoding(Db, Table),
    case ets:prev(Ets, {Table, 2, enc(key, Rel, Enc)}) of
	{Table, 2, K} = Prev ->
	    [Obj] = ets:lookup(Ets, Prev),
	    {ok, setelement(1, Obj, dec(key, K, Enc))};
	_ ->
	    done
    end.

%% Internal

make_atom_name(A) when is_atom(A) ->
    A;
make_atom_name(B) when is_binary(B) ->
    binary_to_atom(B, latin1);
make_atom_name(X) ->
    list_to_atom(lists:flatten(io_lib:fwrite("~w", [X]))).

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
	    error({invalid_index, Bad})
    end;
check_options([], _, Rec) ->
    Rec.

encode_key({K,V}, Enc) -> {enc(key, K, Enc), V};
encode_key({K,As,V}, Enc) -> {enc(key, K, Enc), As, V}.

decode_key({K,V}, Enc)-> {dec(key, K, Enc), V};
decode_key({K,As,V}, Enc)-> {dec(key, K, Enc), As, V}.


ensure_schema(#db{ref = Ets} = Db) ->
    case ets:member(Ets, {table, ?SCHEMA_TABLE}) of
	true ->
	    Db;
	false ->
	    ets:insert(Ets, [{{table, ?SCHEMA_TABLE},
			      #table{name = ?SCHEMA_TABLE,
				     encoding = raw,
				     columns = [key,value]}},
			     {{a, ?SCHEMA_TABLE, encoding}, raw},
			     {{a, ?SCHEMA_TABLE, index}, []},
			     {{a, ?SCHEMA_TABLE, type}, set}]),
	    Db
    end.

file_name(Db, Options) ->
    case proplists:get_value(file, Options) of
	undefined ->
	    atom_to_list(Db)++".db";
	Name ->
	    Name
    end.

file_mode(Options, Default) ->
    case proplists:get_bool(read_only, Options) of
	true  -> [read,raw,binary];
	false -> [read,write,raw,binary]
    end.

%%
%% Load ets table from file
%%

file_load(Fd, Ets) ->
    file_load(Fd, Ets, undefined).

file_load(Fd, Ets, Table) ->
    case read_blob(Fd) of
	{ok,Key} ->
	    %% io:format("Table:~w, Key: ~w\n", [Table, Key]),
	    case read_blob(Fd) of
		{ok,Value} ->
		    %% io:format("Value: ~w\n", [Value]),
		    case Key of
			{Table1,table} ->
			    ets:insert(Ets, {Key,binary_to_term(Value)}),
			    file_load(Fd, Ets,Table1);
			_ when Table =/= undefined ->
			    ets:insert(Ets, {{Table,Key},Value}),
			    file_load(Fd, Ets,Table)
		    end;
		eof ->
		    io:format("Warning: trailing value at eof\n"),
		    ok;
		Error ->
		    Error
	    end;
	eof ->
	    ok;
	Error ->
	    Error
    end.

read_blob(Fd) ->
    case file:read(Fd, 4) of
	{ok,<<0:32>>} ->
	    {ok, <<Len>>} = file:read(Fd, 1),
	    {ok, Name} = file:read(Fd, Len),
	    {ok,{binary_to_atom(Name,latin1), table}};
	{ok,<<Size:32>>} -> 
	    file:read(Fd, Size);  %% not 100% correct!
	Other ->
	    Other
    end.

%%
%% Save ets table to file:
%% <0><table-name><table-info>
%% <key><value>
%%
file_save(Fd, Ets) ->
    file_save(Fd, ets:first(Ets), Ets).

file_save(_Fd, '$end_of_table', _Ets) ->
    ok;
file_save(Fd, EKey, Ets) ->
    %% io:format("Save: Key=~w\n", [EKey]),
    case ets:lookup(Ets, EKey) of
	[{{Table,table},Columns}] when is_atom(Table) ->
	    file:write(Fd, << 0:32 >>),  %% mark table def
	    TableName = atom_to_binary(Table, latin1),
	    file:write(Fd, <<(byte_size(TableName)):8, TableName/binary>>),
	    CBin = term_to_binary(Columns),
	    file:write(Fd, <<(byte_size(CBin)):32, CBin/binary>>),
	    file_save(Fd, ets:next(Ets, EKey), Ets);
	[{{Table,Key},Value}] when is_atom(Table) ->
	    file:write(Fd, <<(byte_size(Key)):32, Key/binary,
			     (byte_size(Value)):32, Value/binary>>),
	    file_save(Fd, ets:next(Ets, EKey), Ets);
	[] ->
	    io:format("Warning: no value for key ~p\n", [EKey]),
	    file_save(Fd, ets:next(Ets, EKey), Ets)
    end.

%% calculate X+1 when X is atom
next_atom(X) when is_atom(X) ->
    list_to_atom(next_byte_list(atom_to_list(X))).

%% add 1 to a byte list
next_byte_list(Xs) ->
    lists:reverse(next_byte_r_list(lists:reverse(Xs))).

next_byte_r_list([255|Xs]) ->
    [0|next_byte_r_list(Xs)];
next_byte_r_list([X|Xs]) when X < 255 ->
    [X+1|Xs];
next_byte_r_list([]) ->
    [1].


int_read(#db{ref = Ets}, Item) ->
    LookupKey = case Item of
		    {deleted, _T, _K} = Del -> Del;
		    {schema, What} ->
			schema_key(What)
		end,
    case ets:lookup(Ets, LookupKey) of
	[{_, V}] ->
	    {ok, V};
	[] ->
	    {error, not_found}
    end.

int_write(#db{ref = Ets}, Item, Value) ->
    Obj = case Item of
	      {deleted, _T, _K} = Del when is_boolean(Value) ->
		  {Del, Value};
	      {schema, What} ->
		  {schema_key(What), Value}
	  end,
    ets:insert(Ets, Obj).

