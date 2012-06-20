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
-export([put/3, get/3, delete/3, update_counter/4,
	index_get/4, index_keys/4]).
-export([push/4, pop/3, prel_pop/3, extract/3]).
-export([list_queue/3, list_queue/6, is_queue_empty/3]).
-export([first_queue/2, next_queue/3]).
-export([mark_queue_object/4]).
-export([first/2, last/2, next/3, prev/3]).
-export([prefix_match/3, prefix_match/4]).
-export([get_schema_mod/2]).
-export([info/2,
	 is_table/2,
	 dump_tables/1]).

%% used by kvdb_trans.erl
-export([int_read/2,
	 int_write/3,
	 int_delete/2,
	 commit_set/1]).
-export([switch_logs/2]).

-include("kvdb.hrl").
-include_lib("lager/include/log.hrl").
-record(k, {t, i=2, k}).  % internal key representation
-import(kvdb_lib, [enc/3, dec/3]).

get_schema_mod(_Db, Default) ->
    Default.

-define(if_table(Db, Tab, Expr), if_table(Db, Tab, fun() -> Expr end)).

info(#db{} = Db, What) ->
    case What of
	tables   -> list_tables(Db);
	encoding -> Db#db.encoding;
	ref      -> Db#db.ref;
	save_mode-> save_mode(Db);
	{Tab,encoding} -> ?if_table(Db, Tab, encoding(Db, Tab));
	{Tab,index   } -> ?if_table(Db, Tab, index(Db, Tab));
	{Tab,type    } -> ?if_table(Db, Tab, type(Db, Tab));
	{Tab,schema  } -> ?if_table(Db, Tab, schema(Db, Tab));
	{Tab,tabrec  } -> ?if_table(Db, Tab, schema_lookup(
					       Db, {table, Tab}, undefined));
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

open(DbName, Options) ->
    case do_open(DbName, Options) of
	{ok, #db{} = Db} ->
	    SaveMode = proplists:get_value(save_mode, Options, []),
	    kvdb_meta:write(Db, save_mode, SaveMode),
	    kvdb_lib:common_open(
	      DbName, Db, Options);
	Error ->
	    Error
    end.

do_open(DbName, Options) ->
    case proplists:get_value(file, Options) of
	undefined ->
	    create_new_ets(DbName, Options);
	File ->
	    %% FIXME: should we compare options with what is restored?
	    load_from_file(File, DbName, Options)
    end.


create_new_ets(_DbName, Options) ->
    Enc = proplists:get_value(encoding, Options, raw),
    kvdb_lib:check_valid_encoding(Enc),
    Ets = ets:new(kvdb_ets, [ordered_set,public]),
    Db = ensure_schema(#db{ref = Ets, encoding = Enc, metadata = Ets}, Options),
    {ok, Db}.

load_from_file(FileName, DbName, Options) ->
    case filelib:is_regular(FileName) of
	false ->
	    ?error("Cannot load from file; ~s doesn't exist~n", [FileName]),
	    create_new_ets(DbName, Options);
	true ->
	    case ets:file2tab(FileName) of
		{ok, T} ->
		    %% FIXME: also need to read transaction log, but as yet,
		    %% we don't have one.
		    Db0 = #db{ref = T},
		    Enc = encoding(Db0, ?SCHEMA_TABLE),
		    {ok, Db0#db{metadata = T, encoding = Enc}};
		Error ->
		    Error
	    end
    end.

switch_logs(#db{ref = Ets, log = {_OldLog, Thr}} = Db, LogInfo) ->
    {_, Log} = lists:keyfind(id, LogInfo),
    NewT = ets:new(kvdb_ets, [ordered_set, public]),
    copy_ets(ets:select(Ets, [{'_',[],['$_']}], 100), NewT),
    NewDb = Db#db{ref = NewT, metadata = NewT, log = {Log, Thr}},
    kvdb_lib:clear_log_thresholds(NewDb),
    kvdb_meta:write(NewDb, log_info, LogInfo),
    maybe_save_to_file(on_switch, NewDb),
    NewDb.

copy_ets({Objs, Cont}, T) ->
    ets:insert(T, Objs),
    copy_ets(ets:select(Cont), T);
copy_ets('$end_of_table', _T) ->
    ok.


maybe_save_to_file(Event, #db{ref = Ets} = Db) ->
    When = kvdb_meta:read(Db, save_mode, []),
    case lists:member(Event, When) of
	true ->
	    FileName = file_name(Db, options(Db)),
	    ets:tab2file(Ets, FileName);
	false ->
	    ok
    end.

close(#db{ref = Ets} = Db) ->
    maybe_save_to_file(on_close, Db),
    ets:delete(Ets).

%% flush? to do a save?

add_table(#db{} = Db, Table, Opts) when is_list(Opts) ->
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
    add_table(Db, Table, TabR);
add_table(Db, Table, #table{} = TabR) ->
    case schema_lookup(Db, {table, Table}, undefined) of
	Tr when Tr =/= undefined ->
	    ok;
	undefined ->
	    store_tabrec(Db, Table, TabR)
    end.

store_tabrec(Db, Table, TabR) ->
    [schema_write(Db, {K,V}) ||
	{K,V} <- [{{table,Table}, TabR},
		  {{a,Table,type}, TabR#table.type},
		  {{a,Table,index}, TabR#table.index},
		  {{a,Table,encoding}, TabR#table.encoding}]],
    ok.

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
save_mode(Db) -> kvdb_meta:read(Db, save_mode, []).
options(Db) -> schema_lookup(Db, options, []).


key_encoding(E) when E==raw; E==sext -> E;
key_encoding(T) when is_tuple(T) ->
    key_encoding(element(1,T)).

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
    ets:delete(Ets, {Table,table}),
    ok.

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
		ets:insert(Ets, {#k{t=Table,k=Key}, Attrs, Value});
	    Ix ->
		OldAttrs = try ets:lookup_element(Ets, #k{t=Table, k=Key}, 2)
			   catch
			       error:_ -> []
			   end,
		OldIxVals = kvdb_lib:index_vals(Ix, K, OldAttrs,
						fun() ->
							get_value(Db,Table,K)
						end),
		NewIxVals = kvdb_lib:index_vals(
			      Ix, K, Attrs, fun() -> Value end),
		[ets:delete(Ets, {'-ix', Table, {I, Key}}) ||
		    I <- OldIxVals -- NewIxVals],
		NewIxVals2 = [{{'-ix',Table, {I, Key}}} ||
				I <- NewIxVals -- OldIxVals],
		ets:insert(Ets, [{#k{t=Table, k=Key}, Attrs, Value}
				 | NewIxVals2])
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
	    ets:insert(Ets, {#k{t=Table, k=enc(key, K, Enc)}, Value}),
	    ok
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



update_counter(#db{} = Db, Table, K, Incr) when is_integer(Incr) ->
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
		    InsertKey = #k{t=Table, k=ActualKey},
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
			     ?debug("Remove(~p, ~p)~n", [Obj, RawKey]),
			     mark_queue_object_(
			       Db, Table,
			       setelement(1, Obj, #k{t=Table,k=RawKey}),
			       blocking)
		     end,
	    do_pop(Db, Table, T, Q, Remove, true)
    end.

mark_queue_object(#db{} = Db, Table, Obj, St) when St == inactive;
						   St == blocking;
						   St == active ->
    Key = #k{t = Table, k = element(1, Obj)},
    mark_queue_object_(Db, Table, setelement(1, Obj, Key), St).

mark_queue_object_(#db{ref = Ets}, _Table, Obj, St) when St == inactive;
							 St == blocking;
							 St == active ->
    ?debug("mark_queue_object_(~p)~n", [Obj]),
    VPos = size(Obj),
    Val = element(VPos, Obj),
    ets:insert(Ets, setelement(VPos, Obj, {St, Val})).

do_pop(#db{ref = Ets} = Db, Table, Type, Q, Remove, ReturnKey) ->
    Enc = encoding(Db, Table),
    {First,Next} =
	case Type of
	    _ when Type == fifo; element(2,Type) == fifo ->
		{fun() -> ets:next(Ets, #k{t=Table, k={{Q,0},0,0}}) end,
		 fun(K) -> ets:next(Ets, #k{t=Table, k=K}) end};
	    _ when Type == lifo; element(2,Type) == lifo ->
		{fun() -> ets:prev(Ets, #k{t=Table, k={{Q,2},0,0}}) end,
		 fun(K) -> ets:prev(Ets, #k{t=Table, k=K}) end};
	    _ -> error(illegal)
	 end,
    case do_pop_(First(), Table, Q, Next, Ets, Type, Enc) of
	blocked -> blocked;
	done -> done;
	{Obj, RawKey} ->
	    IsEmpty =
		case do_pop_(Next(RawKey), Table, Q, Next, Ets, Type, Enc) of
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

do_pop_(TKey, Table, Q, Next, Ets, T, Enc) ->
    case TKey of
	#k{t=Table, k={{Q,1},_,_} = RawKey} ->
	    K = if element(1, T) == keyed -> element(2, RawKey);
		   true -> element(3, RawKey)
		end,
	    case ets:lookup(Ets, TKey) of
		[{_, _, {blocking,_}}] -> blocked;
		[{_, {blocking,_}}] -> blocked;
		[{_, _, {inactive, _}}] ->
		    do_pop_(Next(TKey), Table, Q, Next, Ets, T, Enc);
		[{_, Attrs, {active, V}}] ->
		    {{dec(key,K,Enc), Attrs, V}, RawKey};
		[{_, {active, V}}] ->
		    {{dec(key,K,Enc), V}, RawKey}
	    end;
	_ ->
	    done
    end.

first_queue(#db{ref = Ets} = Db, Table) ->
    case type(Db, Table) of
	Type when Type==fifo; Type==lifo; element(1,Type) == keyed ->
	    case ets:select(Ets, [{ {#k{t=Table,k={{'$1',1},'_','_'}},
				     '_', '_'}, [], ['$1']},
				  { {#k{t=Table,k={{'$1',1},'_','_'}}, '_'},
				    [], ['$1']}], 1) of
		'$end_of_table' ->
		    done;
		{[Q], _} ->
		    {ok, Q}
	    end;
	_ ->
	    error(illegal)
    end.

next_queue(#db{ref = Ets} = Db, Table, Q) ->
    case type(Db, Table) of
	Type when Type==fifo; Type==lifo; element(1,Type) == keyed ->
	    case ets:next(Ets, #k{t=Table,k={{Q,2},0,0}}) of
		#k{t=Table,k={{Q1,1},_,_}} when Q1 =/= Q ->
		    {ok, Q1};
		_ ->
		    done
	    end;
	_ ->
	    error(illegal)
    end.

extract(#db{ref = Ets} = Db, Table, Key) ->
    case type(Db, Table) of
	undefined -> {error, not_found};
	Type ->
	    if Type == fifo; Type == lifo; element(1, Type) == keyed ->
		    case get(Db, Table, Key) of
			{ok, Obj} ->
			    K = element(1, Obj),
			    {{Q,_},K1} = kvdb_lib:split_queue_key(sext, Type, K),
			    ets:delete(Ets, #k{t=Table, k=Key}),
			    IsEmpty = is_queue_empty(Db, Table, Q),
			    Enc = encoding(Db, Table),
			    %% fix value part
			    Sz = size(Obj),
			    Value =
				case element(Sz, Obj) of
				    {St,V} when St==blocking;St==active;
						St==inactive ->
					V
				end,
			    DecK = dec(key,K1,Enc),
			    Obj1 = setelement(
				     1, setelement(Sz,Obj,Value), DecK),
			    {ok, Obj1, Q, IsEmpty};
			{error, _} = Error ->
			    Error
		    end;
	       true ->
		    error(illegal)
	    end
    end.

is_queue_empty(#db{ref = Ets}, Table, Q) ->
    Guard = [{'or', {'==', '$1', blocking}, {'==', '$1', active}}],
    case ets:select(
	   Ets, [{ {#k{t=Table,k={{Q,1},'_','_'}},'_',{'$1','_'}}, Guard, [1] },
		 { {#k{t=Table,k={{Q,1},'_','_'}},{'$1','_'}}, Guard, [1]}], 1)
    of
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
		{fun() -> ets:next(Ets, #k{t=Table, k={{Q,0},0,0}}) end,
		 fun(K) -> ets:next(Ets, #k{t=Table, k=K}) end};
	    _ when T == lifo; element(1,T) == lifo ->
		{fun() -> ets:prev(Ets, #k{t=Table, k={{Q,2},0,0}}) end,
		 fun(K) -> ets:prev(Ets, #k{t=Table, k=K}) end};
	    _ -> error(illegal)
	end,
    list_queue(Limit, First(), Next, Ets, Table, T, Q, Fltr, HeedBlock,
	       Limit, []).

list_queue(Limit, #k{t=Table,k={{Q,1},_,_} = AbsKey} = K, Next, Ets,
	   Table, T, Q, Fltr, HeedBlock, Limit0, Acc)
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
	    {Cont,Acc1} = case Fltr(St, AbsKey, setelement(
						  1,
						  setelement(size(Obj),
							     Obj, V), Kx)) of
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
    case type(Db, Table) of
	undefined ->
	    {error, not_found};
	Type ->
	    Enc = encoding(Db, Table),
	    EncKey =
		if Type==set -> enc(key, Key, Enc);
		   Type==fifo;Type==lifo;element(1,Type)==keyed ->
			Key
		end,
	    case ets:lookup(Ets, #k{t=Table,k=EncKey}) of
		[] ->
		    {error,not_found};
		[{_, Value}] ->
		    {ok,{Key, Value}};
		[{_, As, Value}] ->
		    {ok, {Key, As, Value}}
	    end
    end.

index_get(#db{ref = Ets} = Db, Table, IxName, IxVal) ->
    Enc = encoding(Db, Table),
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
			      case ets:lookup(Ets, #k{t=Table, k=K}) of
				  [] -> Acc;
				  [{_,_,_} = Obj] ->
				      [setelement(1,Obj,dec(key,K,Enc))|Acc]
			      end
		      end, [], Keys);
		false ->
		    {error, invalid_index}
	    end
    end.

index_keys(#db{ref = Ets} = Db, Table, IxName, IxVal) ->
    Enc = encoding(Db, Table),
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
			      case ets:member(Ets, #k{t=Table, k=K}) of
				  false -> Acc;
				  true ->
				      [dec(key,K,Enc)|Acc]
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
	T when T==fifo; T==lifo; element(1,T) == keyed ->
	    delete_q_entry(Db, Table, Key);
	undefined -> {error, badarg};
	_ -> {error, illegal}
    end.

delete_(#db{ref = Ets} = Db, Table, K) ->
    Enc = encoding(Db, Table),
    Key = enc(key, K, Enc),
    case index(Db, Table) of
	[] ->
	    ets:delete(Ets, #k{t=Table,k=Key});
	Ix ->
	    OldAttrs = case ets:lookup(Ets, #k{t=Table, k=Key}) of
			   [{_, OldAs, _}] -> OldAs;
			   _ -> []
		       end,
	    OldIxVals = kvdb_lib:index_vals(Ix, K, OldAttrs,
					    fun() ->
						    get_value(Db, Table, K)
					    end),
	    [ets:delete(Ets, {'-ix',Table,{I,Key}}) || I <- OldIxVals],
	    ets:delete(Ets, #k{t=Table, k=Key})
    end,
    ok.

delete_q_entry(#db{ref = Ets}, Table, {{_,1},TS,_} = K) when is_integer(TS) ->
    ets:delete(Ets, #k{t=Table, k=K});
delete_q_entry(#db{ref = Ets}, Table, {{_,1},_,TS} = K) when is_integer(TS) ->
    ets:delete(Ets, #k{t=Table, k=K});
delete_q_entry(_, _, _) ->
    {error, badarg}.


first(#db{ref = Ets} = Db, Table) ->
    Enc = encoding(Db, Table),
    case ets:select(Ets, [{ {#k{t=Table,k='_'},'_'}, [], ['$_'] }], 1) of
	{[Obj], _} ->
	    #k{t=Table,k=K} = element(1, Obj),
	    {ok, setelement(1, Obj, dec(key, K, Enc))};
	_ ->
	    done
    end.

next(#db{ref = Ets} = Db, Table, RelKey) ->
    Enc = encoding(Db, Table),
    EncRelKey = enc(key, RelKey, Enc),
    case ets:next(Ets, #k{t=Table, k=EncRelKey}) of
	#k{t=Table, k=K} = Next ->
	    [Obj] = ets:lookup(Ets, Next),
	    {ok, setelement(1, Obj, dec(key, K, Enc))};
	_ ->
	    done
    end.

last(#db{ref = Ets} = Db, Table) ->
    Enc = encoding(Db, Table),
    case ets:prev(Ets, #k{t=Table,i=3,k=0}) of
	#k{t=Table,k=K} = Prev ->
	    [Obj] = ets:lookup(Ets, Prev),
	    {ok, setelement(1, Obj, dec(key, K, Enc))};
	_ ->
	    done
    end.

prev(#db{ref = Ets} = Db, Table, Rel) ->
    Enc = encoding(Db, Table),
    case ets:prev(Ets, #k{t=Table, k=enc(key, Rel, Enc)}) of
	#k{t=Table, k=K} = Prev ->
	    [Obj] = ets:lookup(Ets, Prev),
	    {ok, setelement(1, Obj, dec(key, K, Enc))};
	_ ->
	    done
    end.

prefix_match(Db, Table, Prefix) ->
    prefix_match(Db, Table, Prefix, 100).

prefix_match(#db{ref = Ets} = Db, Table, Prefix0, Limit)
  when (is_integer(Limit) orelse Limit == infinity) ->
    Enc = encoding(Db, Table),
    KeyEnc = key_encoding(Enc),
    {Mode, Prefix} = case KeyEnc of
			 raw when is_binary(Prefix0) -> {raw, Prefix0};
			 sext ->
			     if is_binary(Prefix0) -> {dec, Prefix0};
				true ->
				     EncPfx = kvdb_lib:enc_prefix(
						key, Prefix0, sext),
				     {raw, EncPfx}
			     end;
			 _ ->
			     error(badarg)
		     end,
    Pat = if tuple_size(Enc) == 3 ->
		  %% attributes
		  [{ {#k{t=Table, k='$1'}, '$2', '$3'}, [],
		     [{{ '$1', '$2', '$3' }}] }];
	     true ->
		  [{ {#k{t=Table, k='$1'}, '$2'}, [],
		     [{{ '$1', '$2' }}] }]
	  end,
    prefix_match_(ets:select(Ets, Pat, Limit), Prefix, Mode, KeyEnc,
		  [], Limit, Limit).

prefix_match_('$end_of_table', _, _, _, Acc, _, _) ->
    if Acc == [] -> done;
       true ->
	    {lists:reverse(Acc), fun() -> done end}
    end;
prefix_match_({Cands, Cont}, Pfx, Mode, Enc, Acc, Limit0, Limit) ->
    %% check if we need to continue
    FirstK = element(1, FirstObj = hd(Cands)),
    case match_prefix(FirstK, Pfx, Mode, Enc) of
	true ->
	    Acc1 = [dec_key(FirstObj, Enc)|Acc],
	    case decr(Limit, 1) of
		0 ->
		    {lists:reverse(Acc1),
		     prefix_match_sel_cont(
		       tl(Cands), Cont, Pfx, Mode, Enc, [], Limit0, Limit)};
		Limit1 ->
		    match_cands(tl(Cands), Cont, Pfx, Mode, Enc, Acc1,
				Limit0, Limit1)
	    end;
	false ->
	    match_cands(tl(Cands), Cont, Pfx, Mode, Enc, Acc, Limit0, Limit);
	done ->
	    %% Keys are larger than prefix - no need to continue
	    if Acc == [] -> done;
	       true ->
		    {lists:reverse(Acc), fun() -> done end}
	    end
    end.

prefix_match_sel_cont([], Cont, Pfx, Mode, Enc, Acc, Limit0, Limit) ->
    fun() ->
	    prefix_match_(ets:select(Cont), Pfx, Mode, Enc, Acc, Limit0, Limit)
    end;
prefix_match_sel_cont([_|_] = Cands, Cont, Pfx, Mode, Enc, Acc, Limit0, Limit) ->
    fun() ->
	    prefix_match_({Cands, Cont}, Pfx, Mode, Enc, Acc, Limit0, Limit)
    end.

match_prefix(K, Pfx, raw, _) ->
    case kvdb_lib:binary_match(K, Pfx) of
	true -> true;
	false ->
	    if K > Pfx ->
		    done;
	       true ->
		    false
	    end
    end;
match_prefix(K, Pfx, dec, Enc) ->
    match_prefix(dec(key, K, Enc), Pfx, raw, Enc).


match_cands(Cands, Cont, Pfx, Mode, Enc, Acc, Limit0, Limit) ->
    {RevFound, Rest, LimLeft} =
	match_cands(Cands, Pfx, Mode, Enc, Limit),
    Acc1 = RevFound ++ Acc,
    if LimLeft == 0 ->
	    {lists:reverse(Acc1),
	     prefix_match_sel_cont(
	       Rest, Cont, Pfx, Mode, Enc, [], Limit0, Limit0)};
       true ->
	    prefix_match_(ets:select(Cont), Pfx, Mode, Enc, Acc1,
			  Limit0, LimLeft)
    end.



match_cands(Cands, Pfx, Mode, Enc, Limit) ->
    match_cands(Cands, Pfx, Mode, Enc, Limit, []).

match_cands([H|T], Pfx, Mode, Enc, Limit, Acc) ->
    K = element(1,H),
    case match_prefix(K, Pfx, Mode, Enc) of
	true ->
	    Acc1 = [dec_key(H, Enc)|Acc],
	    case decr(Limit, 1) of
		0 -> {Acc1, T, 0};
		L1 ->
		    match_cands(T, Pfx, Mode, Enc, L1, Acc1)
	    end;
	false ->
	    match_cands(T, Pfx, Mode, Enc, Limit, Acc);
	done ->
	    {Acc, [], 0}
    end;
match_cands([], Pfx, _, _, Limit, Acc) ->
    {Acc, [], Limit}.

dec_key(Obj, Enc) ->
    setelement(1, Obj, dec(key, element(1, Obj), Enc)).


decr(infinity, _) ->
    infinity;
decr(Limit, N) ->
    Limit - N.

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


ensure_schema(#db{ref = Ets} = Db, Options) ->
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
			     {{a, ?SCHEMA_TABLE, type}, set},
			     {{a, ?SCHEMA_TABLE, options}, Options}]),
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

int_write(#db{ref = Ets} = Db, Item, Value) ->
    case Item of
	{tabrec, T} when is_record(Value, table) ->
	    store_tabrec(Db, T, Value);
	{deleted, _T, _K} = Del when is_boolean(Value) ->
	    ets:insert(Ets, {Del, Value});
	{deleted, _T} = Del when is_boolean(Value) ->
	    ets:insert(Ets, {Del, Value});
	{add_table, _T} = AddT ->
	    ets:insert(Ets, {AddT, true});
	{schema, What} ->
	    ets:insert(Ets, {schema_key(What), Value})
    end.

int_delete(#db{ref = Ets} = Db, Item) ->
    case Item of
	{deleted, _T} = Del ->
	    ets:delete(Ets, Del);
	{deleted, _T, _K} = Del ->
	    ets:delete(Ets, Del)
    end.

commit_set(#db{ref = Ets} = Db) ->
    Writes = ets:select(Ets, [ { {#k{t='$1',k='$2'},'$3'}, [],
				 [{{ '$1', {{'$2','$3'}} }}] },
			       { {#k{t='$1',k='$2'},'$3','$4'}, [],
				  [{{ '$1', {{'$2','$3','$4'}} }}] } ]),
    Deletes = ets:select(Ets, [ { {{deleted, '$1', '$2'}, true},
				  [], [{{'$1','$2'}}] } ]),
    DelTabs = ets:select(Ets, [ { {{deleted, '$1'}, true},
				  [], ['$1'] } ]),
    AddTabs = ets:select(Ets, [ { {{add_table, '$1'}, '$2'},
				  [], [{{'$1','$2'}}] } ]),
    #commit{write = decode_writes(Writes, Db),
	    delete = Deletes,
	    add_tables = [{T,schema_lookup(Db,{table,T}, undefined),DelFirst} ||
			     {T,DelFirst} <- AddTabs],
	    del_tables = DelTabs}.

decode_writes([{T,_}|_] = Writes, Db) ->
    decode_writes(Writes, T, encoding(Db,T), Db);
decode_writes([], _) ->
    [].

decode_writes([{T, Obj}|Rest], T, Enc, Db) ->
    [{T, setelement(1, Obj, dec(key, element(1,Obj), Enc))}|
     decode_writes(Rest, T, Enc, Db)];
decode_writes([{T, Obj}|Rest], _, _, Db) ->
    Enc = encoding(Db, T),
    [{T, setelement(1, Obj, dec(key, element(1,Obj), Enc))}|
     decode_writes(Rest, T, Enc, Db)];
decode_writes([], _, _, _) ->
    [].


