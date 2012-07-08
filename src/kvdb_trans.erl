-module(kvdb_trans).
-behaviour(kvdb).

-export([run/2, require/2,
	 is_transaction/1,
	 on_update/4]).

-export([info/2,
	 dump_tables/1,
	 get_schema_mod/2,
	 open/2,
	 close/1,
	 add_table/3,
	 delete_table/2,
	 put/3,
	 get/3,
	 get_attrs/4,
	 index_get/4,
	 index_keys/4,
	 update_counter/4,
	 push/4,
	 pop/3,
	 prel_pop/3,
	 extract/3,
	 list_queue/3,
	 is_queue_empty/3,
	 is_table/2,
	 first_queue/2,
	 mark_queue_object/4,
	 next_queue/3,
	 delete/3,
	 first/2,
	 last/2,
	 next/3,
	 prev/3
	]).

%% debug_function
-export([tstore_to_list/1]).

-include("kvdb.hrl").
-include_lib("lager/include/log.hrl").

-define(q_done(Res), (Res == done orelse Res == blocked)).

-spec require(#kvdb_ref{}, fun( (#kvdb_ref{}) -> T )) -> T.
%% @doc Ensures a transaction context, either reusing one, or creating one.
%%
%% This function allows for a function `F' to run inside a transaction context.
%% If no such context exists, a new transaction is started, just as if
%% {@link run/2} had been called from the beginning. If there is an existing
%% context, `F' is run inside that context. Note that the transaction is not
%% committed when `F' returns - this is the responsibility of the topmost
%% function. Also, if the provided reference has a valid `tref' (i.e. it is a
%% transaction context, that context is reused explicitly. This could be used
%% to participate in a transaction started by another process.
%% @end
require(#kvdb_ref{name=Name, tref=undefined} = Ref, F) when is_function(F,1) ->
    case get({kvdb_trans, Name}) of
	undefined -> run(Ref, F);
	[Kt|_]    -> F(Kt)
    end;
require(#kvdb_ref{tref = TRef} = Ref, F) when is_function(F, 1) ->
    Key = {kvdb_trans, Name = name(Ref)},
    case get(Key) of
	[#kvdb_ref{tref = TRef} = Kt|_] ->
	    F(Kt);
	Other ->
	    push_trans(Name, Ref, Other),
	    try F(Ref)
	    after
		pop_trans(Name)
	    end
    end.

run(#kvdb_ref{schema = Schema} = KR0, F) when is_function(F,1) ->
    %% - find the name of the original DB
    %% - use the schema of the current level
    Name = name(KR0),  %% finds the original name
    TRef = make_ref(),
    {ok, DbE} = kvdb_ets:open("trans", []),
    KR1 = #kvdb_ref{mod = kvdb_ets, db = DbE},
    K = #kvdb_ref{name = Name, schema = Schema,
		  mod = ?MODULE, tref = TRef,
		  db = #db{ref = {KR1, KR0}}},
    kvdb_server:begin_trans(Name, TRef, K),
    push_trans(Name, K),
    try  Result = F(K),
	 commit(K),
	 Result
    after
	kvdb_server:end_trans(Name, TRef),
	pop_trans(Name),
	#db{ref = Ets} = DbE,
	ets:delete(Ets)
    end;
run(Name, F) when is_function(F, 1) ->
    run(#kvdb_ref{} = kvdb:db(Name), F).

name(#kvdb_ref{tref = undefined, name = Name}) ->
    Name;
name(#kvdb_ref{db = #db{ref = {_, KR}}}) ->
    name(KR).


push_trans(Name, K) ->
    push_trans(Name, K, get({kvdb_trans,Name})).

push_trans(Name, K, undefined) ->
    put({kvdb_trans,Name}, [K]);
push_trans(Name, K, [_|_] = Running) ->
    put({kvdb_trans, Name}, [K|Running]).

pop_trans(Name) ->
    Key = {kvdb_trans, Name},
    case get(Key) of
	[_] ->
	    erase(Key);
	[_|Rest] ->
	    put(Key, Rest)
    end.

is_transaction(#kvdb_ref{mod = ?MODULE} = Ref) ->
    {true, Ref};
is_transaction(Name) ->
    case get({kvdb_trans, Name}) of
	[#kvdb_ref{name = Name} = Ref|_] ->
	    {true, Ref};
	_ ->
	    false
    end.

tstore_to_list(#kvdb_ref{db = #db{ref = {#kvdb_ref{db = #db{ref = Ets}},_}}}) ->
    ets:tab2list(Ets).

on_update(Event, #kvdb_ref{schema = Schema} = Ref0, Tab, Info) ->
    Name = name(Ref0),
    case is_transaction(Name) of
	{true, #kvdb_ref{db = #db{ref = {#kvdb_ref{mod = M, db = Db},_}}}} ->
	    Rec = #event{event = Event,
			 tab = Tab,
			 info = Info},
	    M:store_event(Db, Rec);
 	false ->
	    Schema:on_update(Event, Ref0, Tab, Info)
    end.

fire_events(#commit{events = Events},
	    #kvdb_ref{schema = Schema} = Ref) ->
    deep_foreach(
      fun(#event{event = E, tab = T, info = I}) ->
	      Schema:on_update(E, Ref, T, I)
      end, Events).

deep_foreach(F, [[]|T]) -> deep_foreach(F, T);
deep_foreach(F, [[_|_] = H|T]) -> deep_foreach(F, H), deep_foreach(F, T);
deep_foreach(F, [H|T]) -> F(H), deep_foreach(F, T);
deep_foreach(_, []) ->
    ok.

commit(#kvdb_ref{tref = TRef, schema = Schema} = Ref0) ->
    Name = name(Ref0),
    NewDb = kvdb_server:start_commit(Name, TRef),
    #kvdb_ref{db = #db{ref = {#kvdb_ref{mod = M1,db=Db1},
			       #kvdb_ref{} = KR2}}} = Ref =
	switch_db(Ref0, NewDb),
    #commit{} = Set = M1:commit_set(Db1),
    #commit{} = Set1 = Schema:pre_commit(Set, Ref),
    ?debug("Commit set: ~p~n", [Set]),
    kvdb_lib:commit(Set1, KR2),
    catch Schema:post_commit(Set1, Ref),
    fire_events(Set1, Ref).

switch_db(#kvdb_ref{tref = undefined}, #kvdb_ref{} = New) ->
    New;
switch_db(#kvdb_ref{db = #db{ref = {K1,K2}} = Db} = R, New) ->
    R#kvdb_ref{db = Db#db{ref = {K1, switch_db(K2, New)}}}.



info(#db{ref = {#kvdb_ref{mod=M1,db=Db1},
		#kvdb_ref{mod=M2,db=Db2}}}, Item) ->
    case M1:info(Db1, Item) of
	undefined ->
	    case is_tuple(Item) andalso
		ok_true(M1:int_read(Db1, {deleted, element(1,Item)})) of
		true ->
		    undefined;
		false ->
		    M2:info(Db2, Item)
	    end;
	Other ->
	    Other
    end.

ok_true({ok, true}) -> true;
ok_true(_) -> false.

%% dump only the transaction store
dump_tables(#db{ref = {#kvdb_ref{mod = M, db = Db}, _}}) ->
    M:dump_tables(Db).

put(#db{ref = {#kvdb_ref{mod = M1, db = Db1} = KR1, KR2}}, Tab, Obj) ->
    ensure_table(Tab, KR1, KR2),
    case M1:put(Db1, Tab, Obj) of
	{ok, _} = Res ->
	    M1:int_delete(Db1, {deleted, Tab, element(1, Obj)}),
	    Res;
	Other ->
	    Other
    end.

get(#db{ref = {#kvdb_ref{mod = M1, db = Db1} = KR1,
	       #kvdb_ref{mod = M2, db = Db2} = KR2}}, Tab, Key) ->
    ensure_table(Tab, KR1, KR2),
    case M1:get(Db1, Tab, Key) of
	{ok, _} = Ret -> Ret;
	{error, not_found} ->
	    case M1:int_read(Db1, {deleted, Tab, Key}) of
		{ok, true} ->
		    {error, not_found};
		_ ->
		    M2:get(Db2, Tab, Key)
	    end
    end.

delete(#db{ref = {#kvdb_ref{mod=M1,db=Db1}, _KR2}}, Tab, Key) ->
    Res = M1:delete(Db1, Tab, Key),
    M1:int_write(Db1, {deleted, Tab, Key}, true),
    Res.


get_schema_mod(#db{ref = {#kvdb_ref{mod=M1,db=Db1},
			  #kvdb_ref{mod=M2,db=Db2}}}, Tab) ->
    case M1:info(Db1, Item = {Tab, schema}) of
	undefined ->
	    M2:info(Db2, Item);
	Other ->
	    Other
    end.

open(#db{} = _Db, _Opts) ->
    error(nyi).

add_table(#db{ref = {#kvdb_ref{mod=M1,db=Db1}, _}} = DbT, Tab, Opts) ->
    case info(DbT, {Tab, type}) of
	undefined ->
	    DelFirst = ok_true(M1:int_read(Db1, {deleted, Tab})),
	    case M1:add_table(Db1, Tab, Opts) of
		ok ->
		    M1:int_write(Db1, {add_table, Tab}, DelFirst),
		    M1:int_delete(Db1, {deleted, Tab}),
		    ok;
		Other ->
		    Other
	    end;
	_ ->
	    ok
    end.

close(_Db) ->
    %% does this even make sense in a transaction?
    error(illegal).

delete_table(#db{ref = {#kvdb_ref{mod=M1,db=Db1},_}} = Db, Tab) ->
    case info(Db, {Tab, type}) of
	undefined ->
	    ok;
	_ ->
	    case M1:delete_table(Db1, Tab) of
		ok ->
		    M1:int_delete(Db1, {add_table,Tab}),
		    M1:int_write(Db1, {deleted,Tab}, true),
		    ok;
		Other ->
		    Other
	    end
    end.

extract(#db{ref = {#kvdb_ref{mod=M1,db=Db1} = KR1,
		   #kvdb_ref{mod=M2,db=Db2} = KR2}}, Tab, QKey) ->
    ensure_table(Tab, KR1, KR2),
    case M1:int_read(Db1, {deleted, Tab, QKey}) of
	{ok, true} ->
	    {error, not_found};
	_ ->
	    case M1:extract(Db1, Tab, QKey) of
		{error, not_found} ->
		    case M2:extract(Db2, Tab, QKey) of
			{error, not_found} = E ->
			    E;
			{ok, _, _, _} = Res ->  % {ok, Obj, Queue, IsEmpty}
			    M1:int_write(Db1, {deleted, Tab, QKey}, true),
			    Res
		    end;
		{ok, _, _, _} = Res ->
		    M1:int_write(Db1, {deleted, Tab, QKey}, true),
		    Res
	    end
    end.

first(#db{ref = {#kvdb_ref{mod=M1,db=Db1} = KR1,
		 #kvdb_ref{mod=M2,db=Db2} = KR2}}, Tab) ->
    ensure_table(Tab, KR1, KR2),
    R1 = M1:first(Db1, Tab),
    case {R1, M2:first(Db2, Tab)} of
	{done, done} ->
	    done;
	{_, done} ->
	    R1;
	{_, {ok,_} = Res} ->
	    check_next(Res, R1, Tab, M1,Db1, M2,Db2)
    end.

check_next(done, R1, _Tab, _M1,_Db1, _M2, _Db2) ->
    R1;
check_next({ok,O2} = R2, R1, Tab, M1,Db1, M2,Db2) ->
    K2 = element(1, O2),
    case obj_deleted(K2, key, M1, Db1, Tab) of
	true ->
	    check_next(M2:next(Db2,Tab,K2), R1, Tab, M1,Db1, M2,Db2);
	_ ->
	    case {K2, R1} of
		{_, {ok,O1}} when K2 > element(1,O1) ->
		    R1;
		_ ->
		    R2
	    end
    end.

check_prev(done, R1, _Tab, _M1,_Db1, _M2, _Db2) ->
    R1;
check_prev({ok,O2} = R2, R1, Tab, M1,Db1, M2,Db2) ->
    K2 = element(1, O2),
    case obj_deleted(K2, key, M1, Db1, Tab) of
	true ->
	    check_prev(M2:prev(Db2,Tab,K2), R1, Tab, M1,Db1, M2,Db2);
	_ ->
	    case {K2, R1} of
		{_, {ok,O1}} when K2 < element(1,O1) ->
		    R1;
		_ ->
		    R2
	    end
    end.

first_queue(#db{ref = {#kvdb_ref{mod=M1,db=Db1} = K1,
		       #kvdb_ref{mod=M2,db=Db2} = K2}}, Tab) ->
    %% A dilemma: we don't really have a delete_queue() function, so we
    %% cannot check whether the result from the persistent store has been
    %% deleted. We assume it can't be for now.
    ensure_table(Tab, K1, K2),
    R1 = M1:first_queue(Db1, Tab),
    R2 = M2:first_queue(Db2, Tab),
    case {R1, R2} of
	{_   , done} -> R1;
	{done, _} -> R2;
	{{ok,A},{ok,B}} ->
	    if A > B ->
		    R2;
	       true ->
		    R1
	    end
    end.

get_attrs(#db{ref = {#kvdb_ref{mod=M1,db=Db1} = KR1,
		     #kvdb_ref{mod=M2,db=Db2} = KR2}}, Tab, K, Attrs) ->
    ensure_table(Tab, KR1, KR2),
    case M1:get_attrs(Db1, Tab, K, Attrs) of
	{error, not_found} ->
	    M2:get_attrs(Db2, Tab, K, Attrs);
	Other ->
	    Other
    end.

index_get(#db{ref = {#kvdb_ref{mod=M1,db=Db1} = KR1,
		     #kvdb_ref{mod=M2,db=Db2} = KR2}}, Tab, Ix, IxK) ->
    ensure_table(Tab, KR1, KR2),
    Set1 = M1:index_get(Db1, Tab, Ix, IxK),
    Set2 = M2:index_get(Db2, Tab, Ix, IxK),
    merge_sets(Set1, Set2, obj, M1, Db1, Tab).

merge_sets(S1, S2, Type, M, Db, Tab) ->
    %% small optimization: we *hope* that most people will not perform
    %% deletes before they do selects and index lookups, so we do a single
    %% lookup to see if there are any deleted objects in the transaction store
    %% for Tab. If not, we don't have to check for each object fetched from
    %% the persistent store.
    merge_sets(S1, S2, Type, M, Db, Tab, any_deleted(Db, Tab)).

merge_sets([], Set, Type, M, Db, Tab, AnyDel) ->
    if AnyDel ->
	    [Obj || Obj <- Set,
		    not obj_deleted(Obj, Type, M, Db, Tab)];
       true ->
	    Set
    end;
merge_sets([H|T1], [H|T2], Type, M, Db, Tab, AnyDel) ->
    [H|merge_sets(T1, T2, Type, M, Db, Tab, AnyDel)];
merge_sets([H1|T1], [H2|_] = S2, obj, M, Db, Tab, AnyDel)
  when element(1,H1) < element(1,H2) ->
    [H1|merge_sets(T1, S2, obj, M, Db, Tab, AnyDel)];
merge_sets([H1|T1], [H2|_] = S2, key, M, Db, Tab, AnyDel) when H1 < H2 ->
    [H1|merge_sets(T1, S2, key, M, Db, Tab, AnyDel)];
merge_sets([_|_] = S1, [H2|T2], Type, M, Db, Tab, AnyDel) ->
    case AnyDel andalso obj_deleted(H2, Type, M, Db, Tab) of
	false ->
	    [H2|merge_sets(S1, T2, Type, M, Db, Tab, AnyDel)];
	true ->
	    merge_sets(S1, T2, Type, M, Db, Tab, AnyDel)
    end;
merge_sets(S1, [], _, _, _, _, _) ->
    S1.

-record(q_merge, {c1, c2, m, db, tab, filter, heedblock, anydel}).
merge_q_sets(S1, S2, QM, Acc) ->
    merge_q_sets(S1, S2, QM, Acc, []).

merge_q_sets([{K,{S,O}}|T1], [{K,_}|T2], QM, Limit, Acc) ->
    merge_q_acc(K,S,O, T1,T2, QM, Limit, Acc);
merge_q_sets([{K1,{S,O}}|T1], [{K2,_}|_] = S2, QM, Limit, Acc) when K1 < K2 ->
    merge_q_acc(K1,S,O, T1,S2, QM, Limit, Acc);
merge_q_sets([_|_] = S1, [{K2,{St2,O2}}|T2], QM, Limit, Acc) ->
    %% K2 < K1
    #q_merge{anydel = AnyDel, m = M, db = Db, tab = Tab} = QM,
    case AnyDel andalso obj_deleted(K2, key, M, Db, Tab) of
	false ->
	    merge_q_acc(K2,St2,O2, S1,T2, QM, Limit, Acc);
	true ->
	    merge_q_sets(S1,T2, QM, Limit, Acc)
    end;
merge_q_sets(done, [{K,{St,O}}|T2], QM, Limit, Acc) ->
    #q_merge{anydel = AnyDel, m = M, db = Db, tab = Tab} = QM,
    case AnyDel andalso obj_deleted(K, key, M, Db, Tab) of
	false ->
	    merge_q_acc(K,St,O, done,T2, QM, Limit, Acc);
	true ->
	    merge_q_sets(done, T2, QM, Limit, Acc)
    end;
merge_q_sets([{K,{St,O}}|T1], done, QM, Limit, Acc) ->
    merge_q_acc(K,St,O, T1,done, QM, Limit, Acc);
merge_q_sets([],S2, QM, Limit, Acc) ->
    {S1, NewC1} = case (QM#q_merge.c1)() of
		      done -> {done, done};
		      {_, _} = Res -> Res
		  end,
    merge_q_sets(S1,S2, QM#q_merge{c1 = NewC1}, Limit, Acc);
merge_q_sets(S1,[], QM, Limit, Acc) ->
    {S2, NewC2} = case (QM#q_merge.c2)() of
		      done -> {done, done};
		      {_, _} = Res -> Res
		  end,
    merge_q_sets(S1,S2, QM#q_merge{c2 = NewC2}, Limit, Acc);
merge_q_sets(done,done, QM, _Limit, Acc) ->
    {lists:reverse(Acc), QM, stop}.


merge_q_acc(K,St,O, S1,S2, #q_merge{heedblock = Heed,
				    filter = Filter} = QM, Limit, Acc) ->
    case Heed andalso St == blocked of
	true ->
	    {Acc, QM, stop};
	false ->
	    {Acc1,Limit1} = case Filter(St,K,O) of
				{keep, X} -> {[X|Acc], decr(Limit)};
				{stop, X} -> {[X|Acc], stop};
				stop      -> {Acc, stop};
				skip      -> {Acc, Limit}
			    end,
	    if Limit1 == 0 ->
		    {lists:reverse(Acc1), set_cont(S1,S2, QM), 0};
	       Limit1 == stop ->
		    {lists:reverse(Acc1), QM, stop};
	       true ->
		    merge_q_sets(S1, S2, QM, Limit1, Acc1)
	    end
    end.

set_cont(S1, S2, #q_merge{c1 = C1, c2 = C2} = QM) ->
    QM#q_merge{c1 = set_cont_(S1, C1), c2 = set_cont_(S2, C2)}.

set_cont_(done, _) -> fun() -> done end;
set_cont_(blocking, _) -> fun() -> blocking end;
set_cont_([], C) -> C;
set_cont_([_|_] = S, C) -> fun() -> {S, C} end.


obj_deleted({#q_key{} = K, _}, obj, M, Db, Tab) ->
    ok_true(M:int_read(Db, {deleted, Tab, K}));
obj_deleted(Obj, Type, M, Db, Tab) ->
    K = if Type==obj -> element(1, Obj);
	   Type==key -> Obj
	end,
    ok_true(M:int_read(Db, {deleted, Tab, K})).

any_deleted(#db{ref = Ets}, Tab) ->
    case ets:select(Ets, [{{{deleted,Tab,'_'},true},[],[true]}], 1) of
	'$end_of_table' -> false;
	_ -> true
    end.


index_keys(#db{ref = {#kvdb_ref{mod=M1,db=Db1} = KR1,
		      #kvdb_ref{mod=M2,db=Db2} = KR2}}, Tab, Ix, IxK) ->
    ensure_table(Tab, KR1, KR2),
    Set1 = M1:index_keys(Db1, Tab, Ix, IxK),
    Set2 = M2:index_keys(Db2, Tab, Ix, IxK),
    merge_sets(Set1, Set2, key, M1, Db1, Tab).

is_queue_empty(#db{ref = {#kvdb_ref{mod=M1,db=Db1} = K1,
			  #kvdb_ref{mod=M2,db=Db2} = K2}}, Tab, Q) ->
    ensure_table(Tab, K1, K2),
    Filter = fun(active, K, O) -> {keep,{K,O}};
		(_, _, _) -> skip
	     end,
    case list_queue_(
	   M1:list_queue(Db1, Tab, Q, Filter, true, 1),
	   M2:list_queue(Db2, Tab, Q, Filter, true, 1),
	   M1,Db1, M2,Db2, Tab, Q, Filter, true, 1, 1, []) of
	{[_|_], _} ->
	    false;
	_ ->
	    true
    end.

is_table(#db{ref = {#kvdb_ref{} = K1, #kvdb_ref{} = K2}}, Tab) ->
    try ensure_table(Tab, K1, K2)
    catch
	error:_ ->
	    false
    end.


last(#db{ref = {#kvdb_ref{mod=M1,db=Db1} = K1,
		#kvdb_ref{mod=M2,db=Db2} = K2}}, Tab) ->
    ensure_table(Tab, K1, K2),

    R1 = M1:last(Db1, Tab),
    R2 = M2:last(Db2, Tab),
    case {R1, R2} of
	{done, _} -> R2;
	{_, done} -> R1;
	{{ok,A},{ok,B}} ->
	    case obj_deleted(B, obj, M1, Db1, Tab) of
		true -> R1;
		false ->
		    if element(1,A) < element(1, B) ->
			    R2;
		       true ->
			    R1
		    end
	    end
    end.

list_queue(Db, Tab, Q) ->
    list_queue(Db, Tab, Q, fun(_,_,O) -> {keep,O} end, false, infinity).

list_queue(#db{ref = {K1, K2}} = Ref, Tab, Q, Filter, HeedBlock, Limit) ->
    ensure_table(Tab, K1, K2),
    do_list_queue(Ref, Tab, Q, Filter, HeedBlock, Limit).

do_list_queue(#db{ref = {#kvdb_ref{mod=M1,db=Db1},
			 #kvdb_ref{mod=M2,db=Db2}}}, Tab, Q,
	      Filter, HeedBlock, Limit) ->
    MyFilter = fun(S,K,O) -> {keep, {K,{S,O}}} end,
    QM = #q_merge{filter = Filter, heedblock = HeedBlock,
		  m = M1, db = Db1, tab = Tab,
		  anydel = any_deleted(Db1, Tab)},
    list_queue_(M1:list_queue(Db1, Tab, Q, MyFilter, HeedBlock, Limit),
		M2:list_queue(Db2, Tab, Q, MyFilter, HeedBlock, Limit),
		QM, Limit, Limit).

list_queue_(R1, R2, _, _, _) when ?q_done(R1), ?q_done(R2) ->
    most_done(R1, R2);
list_queue_(blocked, _, _, _, _) ->
    blocked;
list_queue_({S1,C1}, {S2,C2}, QM, Limit0, Limit) ->
    case merge_q_sets(S1,S2, QM#q_merge{c1 = C1, c2 = C2}, Limit) of
	{blocked, _, _} -> blocked;
	{done, _, _}    -> done;
	{RetSet, _, stop} ->
	    {RetSet, fun() -> done end};
	{RetSet, #q_merge{c1 = NewC1, c2 = NewC2} = QM1, 0} ->
	    {RetSet,
	     fun() ->
		     list_queue_(NewC1(), NewC2(), QM1, Limit0, Limit0)
	     end}
    end.


list_queue_(done,done, _,_, _,_, _, _, _, _, _, _, _) ->
    done;
list_queue_(Done1, Done2, _M1,_Db1, _M2,_Db2, _Tab, _Q, _Filter,
	    _HeedBlock, _Limit0, _Limit, Acc) when
      ?q_done(Done1) orelse ?q_done(Done2) ->
    Status = most_done(Done1, Done2),
    if Acc == [] ->
	    Status;
       true ->

	    {lists:reverse(Acc), fun() -> Status end}
    end;
list_queue_(R1, R2, M1,Db1, M2,Db2, Tab, Q, Filter,
	    HeedBlock, Limit0, Limit, Acc) when
      ?q_done(R1) orelse ?q_done(R2) ->
    {Set, C1, C2} = if R1 == done ->
			    {element(1, R2), fun() -> done end, element(2, R2)};
		       R2 == done ->
			    {element(1, R1), element(2, R1), fun() -> done end}
		    end,
    if Acc == [] ->
	    R2;
       true ->
	    {RetSet, NewAcc, NewLimit} = fill_limit(Set, Acc, Limit, Limit0),
	    {RetSet, fun() ->
			     list_queue_(
			       C1(), C2(), M1,Db1, M2,Db2, Tab, Q,
			       Filter, HeedBlock, Limit0, NewLimit, NewAcc)
		     end}
    end;
list_queue_({S1, C1}, {S2, C2}, M1,Db1, M2,Db2, Tab, Q, Filter,
	    HeedBlock, Limit0, Limit, Acc) ->
    Set = merge_sets(S1, S2, obj, M1, Db1, Tab),
    case fill_limit(Set, Acc, Limit, Limit0) of
	{[], [], _} ->
	    {[], fun() -> done end};
	{RetSet, NewAcc, NewLimit} ->
	    {RetSet, fun() ->
			     list_queue_(
			       C1(), C2(), M1, Db1, M2,Db2, Tab, Q,
			       Filter, HeedBlock, Limit0, NewLimit, NewAcc)
		     end}
    end.



most_done(blocked, _) -> blocked;
most_done(_, blocked) -> blocked;
most_done(_, _) -> done.

decr(infinity) -> infinity;
decr(L) when is_integer(L) ->
    L-1.

fill_limit(Set, [], infinity, _) ->
    %% reasonably, we shouldn't have anything in Acc if Limit==infinity
    {Set, [], infinity};
fill_limit(Set, Acc, Limit, Limit0) when is_integer(Limit) ->
    Concat = lists:reverse(Acc) ++ Set,
    case length(Concat) of
	L when L =< Limit ->
	    {Concat, [], Limit - L};
	L when L > Limit->
	    {Ret, NewAcc} = lists:split(Limit, Concat),
	    {Ret, NewAcc, Limit0}
    end.

mark_queue_object(#db{ref = {#kvdb_ref{mod=M1,db=Db1} = K1,
			     #kvdb_ref{} = K2}} = Ref,
		  Tab, #q_key{} = QK, St)
  when St==active; St==inactive; St==blocking ->
    ensure_table(Tab, K1, K2),
    case queue_read(Ref, Tab, QK) of
	{error, not_found} = E ->
	    E;
	{ok, _OldSt, Obj} ->
	    M1:queue_insert(Db1, Tab, QK, St, Obj)
    end.

queue_read(#db{ref = {K1, K2}} = Ref, Tab, #q_key{} = QKey) ->
    ensure_table(Tab, K1, K2),
    do_queue_read(Ref, Tab, QKey).

do_queue_read(#db{ref = {#kvdb_ref{mod=M1,db=Db1},
			 #kvdb_ref{mod=M2,db=Db2}}}, Tab, #q_key{} = QKey) ->
    case M1:queue_read(Db1, Tab, QKey) of
	{ok, _St, _Obj} = Res1 ->
	    Res1;
	{error, not_found} ->
	    case obj_deleted(QKey, key, M1, Db1, Tab) of
		false ->
		    case M2:queue_read(Db2, Tab, QKey) of
			{ok, _, _} = Res2 ->
			    Res2;
			{error, _} = Err2 ->
			    Err2
		    end;
		true ->
		    {error, not_found}
	    end
    end.

next(#db{ref = {#kvdb_ref{mod=M1,db=Db1} = K1,
		#kvdb_ref{mod=M2,db=Db2} = K2}}, Tab, K) ->
    ensure_table(Tab, K1, K2),
    R1 = M1:next(Db1, Tab, K),
    case {R1, M2:next(Db2, Tab, K)} of
	{done, done} ->
	    done;
	{_, done} ->
	    R1;
	{_, {ok,_} = Res} ->
	    check_next(Res, R1, Tab, M1,Db1, M2,Db2)
    end.

next_queue(_Db, _Tab, _Q) ->
    error(nyi).

pop(#db{ref = {#kvdb_ref{mod=M1,db=Db1} = K1, K2}} = Ref, Tab, Q) ->
    ensure_table(Tab, K1, K2),
    Filter = fun(active, K, O) -> {keep,{K,O}};
		(_, _, _) -> skip
	     end,
    case do_list_queue(Ref, Tab, Q, Filter, true, 2) of
	{[{QKey, Obj}|Rest], _} ->
	    _ = M1:extract(Db1, Tab, QKey), % we don't know where it came from.
	    M1:int_write(Db1, {deleted, Tab, QKey}, true),
	    IsEmpty = (Rest =/= []),
	    {ok, Obj, IsEmpty};
	R when ?q_done(R) ->
	    R;
	{error, _} = E ->
	    E
    end.

prel_pop(#db{ref = {#kvdb_ref{mod=M1,db=Db1} = K1, K2}} = Ref, Tab, Q) ->
    ensure_table(Tab, K1, K2),
    Filter = fun(active, K, O) -> {keep,{K,O}};
		(_, _, _) -> skip
	     end,
    case do_list_queue(Ref, Tab, Q, Filter, true, 2) of
	{[{QKey, Obj}|Rest], _} ->
	    M1:queue_insert(Db1, Tab, QKey, blocking, Obj),
	    IsEmpty = (Rest =/= []),
	    {ok, Obj, QKey, IsEmpty};
	R when ?q_done(R) ->
	    R;
	{error, _} = E ->
	    E
    end.

prev(#db{ref = {#kvdb_ref{mod=M1,db=Db1} = K1,
		#kvdb_ref{mod=M2,db=Db2} = K2}}, Tab, K) ->
    ensure_table(Tab, K1, K2),
    R1 = M1:prev(Db1, Tab, K),
    case {R1, M2:prev(Db2, Tab, K)} of
	{done, done} ->
	    done;
	{_, done} ->
	    R1;
	{_, {ok, _} = Res} ->
	    check_prev(Res, R1, Tab, M1,Db1, M2,Db2)
    end.

push(#db{ref = {#kvdb_ref{mod=M1,db=Db1} = K1, K2}}, Tab, Q, Obj) ->
    ensure_table(Tab, K1, K2),
    M1:push(Db1, Tab, Q, Obj).

update_counter(#db{ref = {#kvdb_ref{mod=M1,db=Db1},_}} = Db, Tab, K, Incr) ->
    case get(Db, Tab, K) of
	{ok, Obj} ->
	    Sz = size(Obj),
	    NewV = case element(Sz, Obj) of
		       I when is_integer(I) ->
			   I+1;
		       B when is_binary(B) ->
			   BSz = bit_size(B),
			   <<I:BSz/integer>> = B,
			   NewI = I + Incr,
			   <<NewI:BSz/integer>>
		   end,
	    M1:put(Db1, Tab, setelement(Sz, Obj, NewV)),
	    NewV;
	_ ->
	    error(not_found)
    end.

ensure_table(Tab, #kvdb_ref{mod = M1, db = Db1},
	     #kvdb_ref{mod = M2, db = Db2}) ->
    case M1:info(Db1, {Tab, tabrec}) of
	undefined ->
	    case M2:info(Db2, {Tab, tabrec}) of
		undefined ->
		    error({no_such_table, Tab});
		#table{} = TabR ->
		    M1:int_write(Db1, {tabrec, Tab}, TabR),
		    true
	    end;
	#table{} ->
	    true
    end.
