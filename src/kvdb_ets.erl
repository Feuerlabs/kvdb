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
%%% @author Ulf Wiger <ulf@feuerlabs.com>
%%% @doc
%%%    ETS backend to kvdb
%%% @end

-module(kvdb_ets).

-behaviour(kvdb).

-export([open/2, close/1, save/1, save/2]).
-export([add_table/3, delete_table/2, list_tables/1]).
-export([put/3, get/3, delete/3, update_counter/4, get_attrs/4,
	index_get/4, index_keys/4]).
-export([push/4, pop/3, prel_pop/3, extract/3,
	 queue_read/3]).
-export([list_queue/3, list_queue/6, is_queue_empty/3]).
-export([first_queue/2, next_queue/3]).
-export([mark_queue_object/4]).
-export([first/2, last/2, next/3, prev/3]).
-export([prefix_match/3, prefix_match/4, prefix_match_rel/5]).
-export([get_schema_mod/2]).
-export([info/2,
	 is_table/2,
	 dump_tables/1,
	 queue_insert/5,
	 queue_delete/3]).

%% used by kvdb_trans.erl
-export([int_read/2,
	 int_write/3,
	 int_delete/2,
	 store_event/2,
	 commit_set/1]).
-export([switch_logs/2]).

-include("kvdb.hrl").
-include_lib("lager/include/log.hrl").
-record(k, {t, i=2, k}).  % internal key representation
-import(kvdb_lib, [enc/3, dec/3]).

%% Macro for lazy caching of frequently used meta-data
%% This should perhaps be in kvdb_lib and used in all backends, but for now
%% it's used here, not least since the ets backend is used by kvdb_trans too.
%%
-define(cache_meta(Db, Tab, EncExpr, TypeExpr),
	cache_meta(Db, Tab, fun() -> EncExpr end, fun() -> TypeExpr end)).

cache_meta(#db{st = #dbst{}} = Db, _, _, _) ->
    Db;
cache_meta(Db, Tab, E, T) ->
    Db#db{st = #dbst{encoding = {Tab,E()},
		     type = {Tab, T()}}}.



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
	{Tab,tabrec  } ->
	    is_table(Db, Tab),
	    schema_lookup(Db, {table, Tab}, undefined);
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
    ?debug("open(~p, ~p)~n", [DbName, Options]),
    case do_open(Options) of
	{ok, #db{} = Db} ->
	    SaveMode = proplists:get_value(save_mode, Options, []),
	    kvdb_meta:write(Db, save_mode, SaveMode),
	    kvdb_meta:write(Db, options, Options),
	    kvdb_lib:common_open(
	      DbName, ?MODULE, Db, Options);
	Error ->
	    Error
    end.

do_open(Options) ->
    ?debug("do_open(~p)~n", [Options]),
    case proplists:get_value(file, Options) of
	undefined ->
	    create_new_ets(Options);
	File ->
	    %% FIXME: should we compare options with what is restored?
	    load_from_file(File, Options)
    end.


create_new_ets(Options) ->
    Enc = proplists:get_value(encoding, Options, raw),
    kvdb_lib:check_valid_encoding(Enc),
    Ets = ets:new(kvdb_ets, [ordered_set,public]),
    Db = ensure_schema(#db{ref = Ets, encoding = Enc, metadata = Ets}, Options),
    {ok, Db}.

load_from_file(FileName, Options) ->
    ?debug("load_from_file(~p, ~p)~n", [FileName, Options]),
    case filelib:is_regular(FileName) of
	false ->
	    ?error("Cannot load from file; ~s doesn't exist~n", [FileName]),
	    create_new_ets(Options);
	true ->
	    ?debug("Loading from file ~p~n", [FileName]),
	    case ets:file2tab(FileName) of
		{ok, T} ->
		    %% FIXME: also need to read transaction log, but as yet,
		    %% we don't have one.
		    Db0 = #db{ref = T},
		    Enc = encoding(Db0, ?META_TABLE),
		    {ok, Db0#db{metadata = T, encoding = Enc}};
		Error ->
		    Error
	    end
    end.

switch_logs(#db{ref = Ets, log = {_OldLog, Thr}} = Db, LogInfo) ->
    %% io:fwrite("switch_logs; Ets=~p, ~p~n", [Ets, LogInfo]),
    {_, Log} = lists:keyfind(id, 1, LogInfo),
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


maybe_save_to_file(Event, #db{} = Db) ->
    When = kvdb_meta:read(Db, save_mode, []),
    case lists:member(Event, When) of
	true ->
	    save(Db);
	false ->
	    ok
    end.

save(#db{} = Db) ->
    save(Db, file_name(Db, options(Db))).

save(#db{ref = Ets} = Db, FileName) ->
    kvdb_meta:write(Db, last_dump, os:timestamp()),
    ets:tab2file(Ets, FileName).


close(#db{ref = Ets} = Db) ->
    maybe_save_to_file(on_close, Db),
    ets:delete(Ets).

%% flush? to do a save?

add_table(#db{encoding = Enc0} = Db, Table, Opts) when is_list(Opts) ->
    TabR0 = check_options(Opts, Db, #table{name = Table}),
    Enc = proplists:get_value(encoding, Opts, Enc0),
    TabR = TabR0#table{encoding = Enc},
    add_table(Db, Table, TabR);
add_table(Db, Table, #table{} = TabR) ->
    case schema_lookup(Db, {table, Table}, undefined) of
	Tr when Tr =/= undefined ->
	    ok;
	undefined ->
	    kvdb_lib:log(Db, ?KVDB_LOG_ADD_TABLE(Table, TabR)),
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

%% cached for better performance, since we check these often
type(#db{st = #dbst{type = {Tab,T}}}, Tab) ->
    T;
type(Db, Table) ->
    schema_lookup(Db, {a, Table, type}, undefined).
encoding(#db{st = #dbst{encoding = {Tab,E}}}, Tab) ->
    E;
encoding(Db, Table) ->
    schema_lookup(Db, {a, Table, encoding}, raw).

index(Db, Table) -> schema_lookup(Db, {a, Table, index}, []).

schema(Db, Table) -> schema_lookup(Db, {a, Table, schema}, []).
save_mode(Db) -> kvdb_meta:read(Db, save_mode, []).
options(Db) -> kvdb_meta:read(Db, options, []).

key_encoding(E) when E==raw; E==sext -> E;
key_encoding(T) when is_tuple(T) ->
    key_encoding(element(1,T)).

delete_table(#db{ref = Ets} = Db, Table) ->
    case schema_lookup(Db, {table, Table}, undefined) of
	undefined -> ok;
	#table{} ->
	    kvdb_lib:log(Db, ?KVDB_LOG_DELETE_TABLE(Table)),
	    ets:select_delete(
	      Ets, [{ {#k{t=Table,_='_'},'_'}, [], [true] },
		    { {#k{t=Table,_='_'},'_','_'}, [], [true] },
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
	set -> put_(?cache_meta(Db, Table, encoding(Db, Table), set),
		    Table, Obj);
	_ -> {error, illegal}
    end.

put_(#db{ref = Ets} = Db, Table, {K, Attrs, Value} = Obj) ->
    try
	case Enc = encoding(Db, Table) of
	    {_,_,_} -> ok;
	    _ -> throw({error, illegal})
	end,
	Key = enc(key, K, Enc),
	kvdb_lib:log(Db, ?KVDB_LOG_INSERT(Table, Obj)),
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
put_(#db{ref = Ets} = Db, Table, {K, Value} = Obj) ->
    case encoding(Db, Table) of
	{_, _, _} ->
	    {error, illegal};
	Enc ->
	    kvdb_lib:log(Db, ?KVDB_LOG_INSERT(Table, Obj)),
	    ets:insert(Ets, {#k{t=Table, k=enc(key, K, Enc)}, Value}),
	    ok
    end.

queue_insert(#db{ref = Ets} = Db, Table, #q_key{} = QKey, St, Obj) ->
    case type(Db, Table) of
	set ->
	    error(illegal);
	T ->
	    Enc = encoding(Db, Table),
	    case matches_encoding(Enc, Obj) of
		true ->
		    AbsKey = q_key_to_int(QKey, T),
		    InsertKey = #k{t=Table, k=AbsKey},
		    Data = case Obj of
			       {_, As, Value} ->
				   {InsertKey, As, {St, Value}};
			       {_, Value} ->
				   {InsertKey, {St, Value}}
			   end,
		    kvdb_lib:log(Db, ?KVDB_LOG_Q_INSERT(Table, QKey, St, Obj)),
		    ets:insert(Ets, Data),
		    ok;
		false ->
		    %% oops! But this function is called from the log replay...
		    error({encoding_mismatch, [Enc, Obj]})
	    end
    end.

queue_delete(Db, Table, #q_key{} = QKey) ->
    _ = extract(Db, Table, QKey),
    ok.

queue_read(#db{ref = Ets} = Db, Table, #q_key{} = QKey) ->
    case type(Db, Table) of
	set ->
	    error(illegal);
	T ->
	    AbsKey = q_key_to_int(QKey, T),
	    LookupKey = #k{t=Table, k=AbsKey},
	    case ets:lookup(Ets, LookupKey) of
		[] ->
		    {error, not_found};
		[Obj] ->
		    Sz = size(Obj),
		    {St, Val} = element(Sz, Obj),
		    Obj1 = setelement(1, setelement(Sz, Obj, Val),
				      QKey#q_key.key),
		    {ok, St, Obj1}
	    end
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



update_counter(#db{} = Db0, Table, K, Incr) when is_integer(Incr) ->
    case type(Db0, Table) of
	set ->
	    Db = ?cache_meta(Db0, Table, encoding(Db0, Table), set),
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
		    put(Db, Table, NewObj),  % logs an insert operation
		    NewV;
		_ ->
		    error(not_found)
	    end;
	_ ->
	    error(illegal)
    end.


%% NOTE: We don't encode the object keys for the queues. Strictly speaking,
%% we don't have to, and only do it otherwise because of the prefix_match()
%% functionality (perhaps we should revisit that too?).
%% Erlang preserves the ordering anyway. We do need to check that the key
%% matches the defined encoding, though, since we don't want to allow non-
%% binary keys with raw encoding.
%%
push(#db{ref = Ets} = Db, Table, Q, Obj) ->
    Type = type(Db, Table),
    Enc = encoding(Db, Table),
    if Type == fifo; Type == lifo; element(1, Type) == keyed ->
	    case matches_encoding(Enc, Obj) of
		true ->
		    {_ActualKey, QKey} = kvdb_lib:actual_key(
					   sext, Type, Q,
					   element(1,Obj)),
		    InsertKey = #k{t=Table, k=q_key_to_int(QKey, Type)},
		    Data = case Obj of
			       {_, As, Value} ->
				   {InsertKey, As, {active, Value}};
			       {_, Value} ->
				   {InsertKey, {active, Value}}
			   end,
		    kvdb_lib:log(Db, ?KVDB_LOG_Q_INSERT(
					Table, QKey, active, Obj)),
		    ets:insert(Ets, Data),
		    {ok, QKey};
		_ ->
		    {error, badarg}
	    end;
       true ->
	    {error, badarg}
    end.


pop(Db0, Table, Q) ->
    case type(Db0, Table) of
	set -> {error, illegal};
	undefined -> {error, badarg};
	T ->
	    Db = ?cache_meta(Db0, Table, encoding(Db0, Table), T),
	    Remove = fun(_Obj, RawKey) ->
			     delete(Db, Table, RawKey)
		     end,
	    do_pop(Db, Table, T, Q, Remove, false)
    end.

prel_pop(Db0, Table, Q) ->
    case type(Db0, Table) of
	set -> {error, illegal};
	undefined -> {error, badarg};
	T ->
	    Db = ?cache_meta(Db0, Table, encoding(Db0, Table), T),
	    Remove = fun(Obj, RawKey) ->
			     ?debug("Remove(~p, ~p)~n", [Obj, RawKey]),
			     mark_queue_object_(
			       Db, int_to_q_key(Db, Table, RawKey),
			       #k{t=Table,k=RawKey}, Obj,
			       blocking)
		     end,
	    do_pop(Db, Table, T, Q, Remove, true)
    end.

mark_queue_object(#db{ref = Ets} = Db, Table, #q_key{} = QK, St) when
      St == inactive; St == blocking; St == active ->
    Int = q_key_to_int(QK, type(Db, Table)),
    Key = #k{t = Table, k = Int},
    case ets:lookup(Ets, Key) of
	[Obj] ->
	    mark_queue_object_(Db, QK, Key, Obj, St);
	[] ->
	    {error, not_found}
    end.

q_key_to_int(#q_key{queue = Q, ts = TS, key = K}, Type) ->
    case Type of
	{keyed,_} ->
	    {{Q,1}, K, TS};
	_ when Type == fifo; Type == lifo ->
	    {{Q,1}, TS, K}
    end.

int_to_q_key(Db, Table, Int) ->
    Type = type(Db, Table),
    #q_key{queue = {Q,_}} = QK = kvdb_lib:split_queue_key(sext, Type, Int),
    QK#q_key{queue = Q}.

mark_queue_object_(#db{ref = Ets} = Db, #q_key{} = QK, #k{} = K, Obj, St) when
      St == inactive; St == blocking; St == active ->
    ?debug("mark_queue_object_(~p)~n", [Obj]),
    VPos = size(Obj),
    Val = element(VPos, Obj),
    kvdb_lib:log(Db, ?KVDB_LOG_Q_INSERT(K#k.t, QK, St, Obj)),
    ets:update_element(Ets, K, {VPos, {St, Val}}).

do_pop(#db{ref = Ets} = Db, Table, Type, Q, Remove, ReturnKey) ->
    Enc = encoding(Db, Table),
    {First,Next} =
	case Type of
	    _ when Type == fifo; element(2,Type) == fifo ->
		{fun() -> ets:next(Ets, #k{t=Table, k={{Q,0},0,0}}) end,
		 fun(K) -> ets:next(Ets, K) end};
	    _ when Type == lifo; element(2,Type) == lifo ->
		{fun() -> ets:prev(Ets, #k{t=Table, k={{Q,2},0,0}}) end,
		 fun(K) -> ets:prev(Ets, K) end};
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
		    {ok, Obj, int_to_q_key(Db,Table,RawKey), IsEmpty};
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
		[{_, {inactive, _}}] ->
		    do_pop_(Next(TKey), Table, Q, Next, Ets, T, Enc);
		[{_, Attrs, {active, V}}] ->
		    {{K, Attrs, V}, RawKey};
		[{_, {active, V}}] ->
		    {{K, V}, RawKey}
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

extract(#db{ref = Ets} = Db0, Table, #q_key{queue = Q} = QKey) ->
    case type(Db0, Table) of
	undefined -> {error, not_found};
	Type ->
	    if Type == fifo; Type == lifo; element(1, Type) == keyed ->
		    Db = ?cache_meta(Db0, Table, encoding(Db0, Table), Type),
		    Key = q_key_to_int(QKey, Type),
		    EtsKey = #k{t = Table, k = Key},
		    case ets:lookup(Ets, EtsKey) of
			[Obj] ->
			    kvdb_lib:log(Db, ?KVDB_LOG_Q_DELETE(Table, QKey)),
			    ets:delete(Ets, EtsKey),
			    IsEmpty = is_queue_empty(Db, Table, Q),
			    %% fix value part
			    Sz = size(Obj),
			    Value =
				case element(Sz, Obj) of
				    {St,V} when St==blocking;St==active;
						St==inactive ->
					V
				end,
			    Obj1 = setelement(
				     1, setelement(Sz,Obj,Value),
				     QKey#q_key.key),
			    {ok, Obj1, Q, IsEmpty};
			[] ->
			    {error, not_found}
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

list_queue(#db{ref = Ets} = Db0, Table, Q, Fltr, HeedBlock, Limit)
  when Limit > 0 ->
    T = type(Db0, Table),
    Db = ?cache_meta(Db0, Table, encoding(Db0, Table), T),
    {First,Next} =
	case T of
	    _ when T == fifo; element(2,T) == fifo ->
		{fun() -> ets:next(Ets, #k{t=Table, k={{Q,0},0,0}}) end,
		 fun(K) -> ets:next(Ets, #k{t=Table, k=K}) end};
	    _ when T == lifo; element(2,T) == lifo ->
		{fun() -> ets:prev(Ets, #k{t=Table, k={{Q,2},0,0}}) end,
		 fun(K) -> ets:prev(Ets, #k{t=Table, k=K}) end};
	    _ -> error(illegal)
	end,
    list_queue(Limit, First(), Next, Ets, Db, Table, T, Q, Fltr, HeedBlock,
	       Limit, []).

list_queue(Limit, #k{t=Table,k={{Q,1},_,_} = AbsKey} = K, Next, Ets,
	   Db, Table, T, Q, Fltr, HeedBlock, Limit0, Acc)
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
	    #q_key{key = Kx} = QKey = int_to_q_key(Db, Table, AbsKey),
	    {Cont,Acc1} = case Fltr(St, QKey, setelement(
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
						Ets, Db, Table, T, Q, Fltr,
						HeedBlock, Limit0, [])
			     end};
			Limit1 ->
			    list_queue(
			      Limit1, Next(AbsKey), Next, Ets, Db, 
			      Table, T, Q, Fltr, HeedBlock, Limit0, Acc1)
		    end;
		false ->
		    {lists:reverse(Acc1), fun() -> done end}
	    end
    end;
list_queue(_, _, _, _, _, _, _, _, _, _, _, Acc) ->
    {lists:reverse(Acc), fun() -> done end}.

decr(infinity) -> infinity;
decr(I) when is_integer(I) -> I - 1.

%% matches_encoding(Db, Table, Obj) ->
%%     matches_encoding(encoding(Db, Table), Obj),

%% FIXME! This function should also check that we are type-compatible with
%% the requested encoding, even if we don't actually encode.
matches_encoding(Enc, Obj) ->
    case {Enc, Obj} of
	{{_,_,_}, {_,_,_}} ->
	    true;
	{_, {_,_}} ->
	    true;
	_ ->
	    false
    end.

get(#db{ref = Ets} = Db, Table, Key) ->
    ?debug("get: Ets = ~p, Db = ~p, Table = ~p, Key = ~p ~n",  
	   [Ets, Db, Table, Key]),
    case type(Db, Table) of
	undefined ->
	    {error, not_found};
	Type ->
	    Enc = encoding(Db, Table),
	    EncKey =
		if Type==set -> enc(key, Key, Enc);
		   Type==fifo;Type==lifo;element(1,Type)==keyed ->
			?KVDB_THROW(illegal)
			%% case Key of
			%%     #q_key{} ->
			%% 	q_key_to_int(Key, Type);
			%%     _ ->
			%% 	?KVDB_RETURN({error, not_found})
			%% end
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

get_attrs(Db0, Table, Key, As) ->
    case encoding(Db0, Table) of
	{_, _, _} = E ->
	    Db = ?cache_meta(Db0, Table, E, type(Db0, Table)),
	    case get(Db, Table, Key) of
		{ok, {_, Attrs, _}} ->
		    if As == all ->
			    {ok, Attrs};
		       is_list(As) ->
			    {ok, [{K,V} || {K, V} <- Attrs,
					   lists:member(K, As)]}
		    end;
		_ ->
		    {error, not_found}
	    end;
	_ ->
	    error(badarg)
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


delete(Db0, Table, Key) ->
    case type(Db0, Table) of
	set ->
	    Db = ?cache_meta(Db0, Table, encoding(Db0, Table), set),
	    delete_(Db, Table, Key);
	T when T==fifo; T==lifo; element(1,T) == keyed ->
	    %% no need to cache - only adds one access.
	    delete_q_entry(Db0, Table, Key);
	undefined -> {error, badarg};
	_ -> {error, illegal}
    end.

delete_(#db{ref = Ets} = Db, Table, K) ->
    Enc = encoding(Db, Table),
    Key = enc(key, K, Enc),
    kvdb_lib:log(Db, ?KVDB_LOG_DELETE(Table, Key)),
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

delete_q_entry(#db{ref = Ets} = Db, Table, #q_key{} = QKey) ->
    kvdb_lib:log(Db, ?KVDB_LOG_Q_DELETE(Table, QKey)),
    Int = q_key_to_int(QKey, type(Db, Table)),
    ets:delete(Ets, #k{t = Table, k = Int});
delete_q_entry(#db{ref = Ets} = Db,Table,{{Q,1},TS,_}=K) when is_integer(TS) ->
    kvdb_lib:log(Db, ?KVDB_LOG_Q_DELETE(Table, setelement(1,K,Q))),
    ets:delete(Ets, #k{t=Table, k=K});
delete_q_entry(#db{ref = Ets}=Db,Table,{{Q,1},_,TS}=K) when is_integer(TS) ->
    kvdb_lib:log(Db, ?KVDB_LOG_Q_DELETE(Table, setelement(1,K,Q))),
    ets:delete(Ets, #k{t=Table, k=K});
delete_q_entry(_, _, _) ->
    {error, badarg}.


first(#db{ref = Ets} = Db, Table) ->
    Enc = encoding(Db, Table),
    Pat = case Enc of
	      {_, _, _} -> [{ {#k{t=Table,k='_'},'_','_'}, [], ['$_'] }];
	      _         -> [{ {#k{t=Table,k='_'},'_'}, [], ['$_'] }]
	  end,
    case ets:select(Ets, Pat, 1) of
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

prefix_match(Db, Table, Prefix, Limit) ->
    prefix_match_(Db, Table, Prefix, false, Limit).

prefix_match_rel(Db, Table, Prefix, StartPoint, Limit) ->
    prefix_match_(Db, Table, Prefix, {true, StartPoint}, Limit).

prefix_match_(#db{ref = Ets} = Db, Table, Prefix0, Rel, Limit)
  when (is_integer(Limit) orelse Limit == infinity) ->
    Enc = encoding(Db, Table),
    KeyEnc = key_encoding(Enc),
    {Mode, Prefix} = enc_match_prefix(KeyEnc, Prefix0),
    Pat = if tuple_size(Enc) == 3 ->
		  %% attributes
		  [{ {#k{t=Table, k='$1'}, '$2', '$3'}, match_guard(Rel, KeyEnc),
		     [{{ '$1', '$2', '$3' }}] }];
	     true ->
		  [{ {#k{t=Table, k='$1'}, '$2'}, match_guard(Rel, KeyEnc),
		     [{{ '$1', '$2' }}] }]
	  end,
    prefix_match_(ets_select(Ets, Pat, Limit), Prefix, Mode, KeyEnc,
		  [], Limit, Limit).

enc_match_prefix(raw, Prefix) -> {raw, Prefix};
enc_match_prefix(sext, Prefix) when is_binary(Prefix) ->
    %% can't match binary prefixes with a match spec
    {dec, Prefix};
enc_match_prefix(sext, Prefix) ->
    {ms, {match_spec([{Prefix, [], ['$_']}]),
	  not is_ms_var(Prefix),
	  Prefix}}.

is_ms_var('_') -> true;
is_ms_var(A) when is_atom(A) ->
    case atom_to_list(A) of
	[$$ | Rest] ->
	    try  _ = list_to_integer(Rest),
		 true
	    catch
		error:_ -> false
	    end;
	_ -> false
    end;
is_ms_var(_) ->
    false.

match_spec(Ms) ->
    ets:match_spec_compile(Ms).

match_guard(false, _) -> [];
match_guard({true, StartP}, Enc) ->
    Prefix = enc(key, StartP, Enc),
    [{'>', '$1', Prefix}].


ets_select(Ets, Pat, infinity) ->
    {ets:select(Ets, Pat), '$end_of_table'};
ets_select(Ets, Pat, Limit) when is_integer(Limit) ->
    ets:select(Ets, Pat, Limit).


prefix_match_(End, _, _, _, Acc, _, _)
  when End=='$end_of_table'; End=={[], '$end_of_table'} ->
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
	false -> maybe_done(K, Pfx)
    end;
match_prefix(K, {Ms,Comparable,Pfx}, ms, Enc) ->
    DecK = dec(key, K, Enc),
    case ets:match_spec_run([DecK], Ms) of
	[_] -> true;
	[]  -> if Comparable -> maybe_done(K, Pfx);
		  true -> false
	       end
    end;
match_prefix(K, Pfx, dec, Enc) ->
    case dec(key, K, Enc) of
	DecK when is_binary(DecK) ->
	    case kvdb_lib:binary_match(DecK, Pfx) of
		true -> true;
		false ->
		    maybe_done(DecK, Pfx)
	    end;
	Other -> maybe_done(Other, Pfx)
    end.

maybe_done(K, Pfx) when K > Pfx -> done;
maybe_done(_, _) ->
    false.

%% match_prefix(K, Pfx, dec, Enc) ->
%%     match_prefix(dec(key, K, Enc), Pfx, raw, Enc).


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
match_cands([], _, _, _, Limit, Acc) ->
    {Acc, [], Limit}.

dec_key(Obj, Enc) ->
    setelement(1, Obj, dec(key, element(1, Obj), Enc)).


decr(infinity, _) ->
    infinity;
decr(Limit, N) ->
    Limit - N.

%% Internal

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

%% encode_key({K,V}, Enc) -> {enc(key, K, Enc), V};
%% encode_key({K,As,V}, Enc) -> {enc(key, K, Enc), As, V}.

%% decode_key({K,V}, Enc)-> {dec(key, K, Enc), V};
%% decode_key({K,As,V}, Enc)-> {dec(key, K, Enc), As, V}.


ensure_schema(#db{ref = Ets} = Db, Options) ->
    case ets:member(Ets, {table, ?META_TABLE}) of
	true ->
	    Db;
	false ->
	    ets:insert(Ets, [{{table, ?META_TABLE},
			      #table{name = ?META_TABLE,
				     encoding = raw,
				     columns = [key,value]}},
			     {{a, ?META_TABLE, encoding}, raw},
			     {{a, ?META_TABLE, index}, []},
			     {{a, ?META_TABLE, type}, set},
			     {{a, ?META_TABLE, options}, Options}]),
	    Db
    end.

file_name(Db, Options) ->
    case proplists:get_value(file, Options) of
	undefined ->
	    kvdb_lib:good_string(
	      kvdb_meta:read(Db, name, {unknown_db,?MODULE})) ++ ".db";
	F ->
	    F
    end.

%% file_mode(Options, Default) ->
%%     case proplists:get_bool(read_only, Options) of
%% 	true  -> [read,raw,binary];
%% 	false -> [read,write,raw,binary]
%%     end.

%%
%% Load ets table from file
%%

%% file_load(Fd, Ets) ->
%%     file_load(Fd, Ets, undefined).

%% file_load(Fd, Ets, Table) ->
%%     case read_blob(Fd) of
%% 	{ok,Key} ->
%% 	    %% io:format("Table:~w, Key: ~w\n", [Table, Key]),
%% 	    case read_blob(Fd) of
%% 		{ok,Value} ->
%% 		    %% io:format("Value: ~w\n", [Value]),
%% 		    case Key of
%% 			{Table1,table} ->
%% 			    ets:insert(Ets, {Key,binary_to_term(Value)}),
%% 			    file_load(Fd, Ets,Table1);
%% 			_ when Table =/= undefined ->
%% 			    ets:insert(Ets, {{Table,Key},Value}),
%% 			    file_load(Fd, Ets,Table)
%% 		    end;
%% 		eof ->
%% 		    io:format("Warning: trailing value at eof\n"),
%% 		    ok;
%% 		Error ->
%% 		    Error
%% 	    end;
%% 	eof ->
%% 	    ok;
%% 	Error ->
%% 	    Error
%%     end.

%% read_blob(Fd) ->
%%     case file:read(Fd, 4) of
%% 	{ok,<<0:32>>} ->
%% 	    {ok, <<Len>>} = file:read(Fd, 1),
%% 	    {ok, Name} = file:read(Fd, Len),
%% 	    {ok,{binary_to_atom(Name,latin1), table}};
%% 	{ok,<<Size:32>>} ->
%% 	    file:read(Fd, Size);  %% not 100% correct!
%% 	Other ->
%% 	    Other
%%     end.

%%
%% Save ets table to file:
%% <0><table-name><table-info>
%% <key><value>
%%
%% file_save(Fd, Ets) ->
%%     file_save(Fd, ets:first(Ets), Ets).

%% file_save(_Fd, '$end_of_table', _Ets) ->
%%     ok;
%% file_save(Fd, EKey, Ets) ->
%%     %% io:format("Save: Key=~w\n", [EKey]),
%%     case ets:lookup(Ets, EKey) of
%% 	[{{Table,table},Columns}] when is_atom(Table) ->
%% 	    file:write(Fd, << 0:32 >>),  %% mark table def
%% 	    TableName = atom_to_binary(Table, latin1),
%% 	    file:write(Fd, <<(byte_size(TableName)):8, TableName/binary>>),
%% 	    CBin = term_to_binary(Columns),
%% 	    file:write(Fd, <<(byte_size(CBin)):32, CBin/binary>>),
%% 	    file_save(Fd, ets:next(Ets, EKey), Ets);
%% 	[{{Table,Key},Value}] when is_atom(Table) ->
%% 	    file:write(Fd, <<(byte_size(Key)):32, Key/binary,
%% 			     (byte_size(Value)):32, Value/binary>>),
%% 	    file_save(Fd, ets:next(Ets, EKey), Ets);
%% 	[] ->
%% 	    io:format("Warning: no value for key ~p\n", [EKey]),
%% 	    file_save(Fd, ets:next(Ets, EKey), Ets)
%%     end.

%% calculate X+1 when X is atom
%% next_atom(X) when is_atom(X) ->
%%     list_to_atom(next_byte_list(atom_to_list(X))).

%% add 1 to a byte list
%% next_byte_list(Xs) ->
%%     lists:reverse(next_byte_r_list(lists:reverse(Xs))).

%% next_byte_r_list([255|Xs]) ->
%%     [0|next_byte_r_list(Xs)];
%% next_byte_r_list([X|Xs]) when X < 255 ->
%%     [X+1|Xs];
%% next_byte_r_list([]) ->
%%     [1].


int_read(#db{ref = Ets}, Item) ->
    LookupKey = case Item of
		    {deleted, _T} = Del -> Del;
		    {deleted, _T, _K} = Del -> Del;
		    {queue_op, _Q, _Op} = Op -> Op;
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
	{queue_op, _Q, _X} = Op ->
	    ets:insert(Ets, {Op, Value});
	{add_table, _T} = AddT ->
	    ets:insert(Ets, {AddT, true});
	{schema, What} ->
	    ets:insert(Ets, {schema_key(What), Value})
    end.

int_delete(#db{ref = Ets}, Item) ->
    case Item of
	{add_table, _T} = Add ->
	    ets:delete(Ets, Add);
	{queue_op, _, _} = QOp ->
	    ets:delete(Ets, QOp);
	{deleted, _T} = Del ->
	    ets:delete(Ets, Del);
	{deleted, _T, _K} = Del ->
	    ets:delete(Ets, Del)
    end.

store_event(#db{ref = Ets}, #event{} = E) ->
    ets:insert(Ets, {{evt, os:timestamp()}, E}),
    ok.

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
    Events = ets:select(Ets, [ { {{evt,'_'}, '$1'}, [], ['$1'] } ]),
    #commit{write = decode_writes(Writes, Db),
	    delete = Deletes,
	    add_tables = [{T,schema_lookup(Db,{table,T}, undefined),DelFirst} ||
			     {T,DelFirst} <- AddTabs],
	    del_tables = DelTabs,
	    events = Events}.

decode_writes([{T,_}|_] = Writes, Db) ->
    decode_writes(Writes, T, encoding(Db,T), Db);
decode_writes([], _) ->
    [].

decode_writes([{T, _}|_] = Writes, T1, _, Db) when T =/= T1 ->
    %% Switched to different table - re-cache encoding info
    decode_writes(Writes, T, encoding(Db, T), Db);
decode_writes([{T, {QKi, {St,Val}}}|Rest], T, Enc, Db) when is_tuple(QKi) ->
    QKey = int_to_q_key(Db, T, QKi),
    Obj = {QKey#q_key.key, Val},
    [{T, QKey, St, Obj}|decode_writes(Rest, T, Enc, Db)];
decode_writes([{T, {QKi, As, {St,Val}}}|Rest], T, Enc, Db) when is_tuple(QKi) ->
    QKey = int_to_q_key(Db, T, QKi),
    Obj = {QKey#q_key.key, As, Val},
    [{T, QKey, St, Obj}|decode_writes(Rest, T, Enc, Db)];
decode_writes([{T, Obj}|Rest], T, Enc, Db) ->
    [{T, setelement(1, Obj, dec(key, element(1,Obj), Enc))}|
     decode_writes(Rest, T, Enc, Db)];
decode_writes([{T, Obj}|Rest], _, _, Db) ->
    Enc = encoding(Db, T),
    [{T, setelement(1, Obj, dec(key, element(1,Obj), Enc))}|
     decode_writes(Rest, T, Enc, Db)];
decode_writes([], _, _, _) ->
    [].
