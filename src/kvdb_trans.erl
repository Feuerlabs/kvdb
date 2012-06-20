-module(kvdb_trans).
-behaviour(kvdb).

-export([run/2,
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

-include("kvdb.hrl").
-include_lib("lager/include/log.hrl").

-record(event, {event, tab, info}).

run(#kvdb_ref{name = Name, schema = Schema} = KR0, F) when is_function(F,1) ->
    KR2 = try kvdb_server:trans_db(Name)
	  catch
	      exit:{noproc,_} -> KR0
	  end,
    {ok, DbE} = kvdb_ets:open("trans", []),
    KR1 = #kvdb_ref{mod = kvdb_ets, db = DbE},
    K = #kvdb_ref{name = Name, schema = Schema, mod = ?MODULE,
		  db = #db{ref = {KR1, KR2}}},
    put({kvdb_trans, Name}, K),
    try  Result = F(K),
	 commit(K),
	 Result
    after
	catch kvdb_server:end_trans(Name),
	erase({kvdb_trans, Name}),
	erase({kvdb_trans_events, Name}),
	#db{ref = Ets} = DbE,
	ets:delete(Ets)
    end;
run(Name, F) when is_function(F, 1) ->
    run(#kvdb_ref{} = kvdb:db(Name), F).

is_transaction(#kvdb_ref{mod = ?MODULE} = Ref) ->
    {true, Ref};
is_transaction(Name) ->
    case get({kvdb_trans, Name}) of
	#kvdb_ref{name = Name} = Ref ->
	    {true, Ref};
	_ ->
	    false
    end.

on_update(Event, #kvdb_ref{name = Name, schema = Schema} = Ref0, Tab, Info) ->
    Key = {kvdb_trans_events, Name},
    case is_transaction(Name) of
	{true, _} ->
	    Rec = #event{event = Event,
			 tab = Tab,
			 info = Info},
	    case get(Key) of
		undefined ->
		    put(Key, [Rec]);
		List ->
		    put(Key, [Rec|List])
	    end;
	false ->
	    Schema:on_update(Event, Ref0, Tab, Info)
    end.

fire_events(#kvdb_ref{name = Name, schema = Schema} = Ref) ->
    case get({kvdb_trans_events, Name}) of
	undefined ->
	    ok;
	List ->
	    lists:foreach(
	      fun(#event{event = E, tab = T, info = I}) ->
		      Schema:on_update(E, Ref, T, I)
	      end, lists:reverse(List))
    end.

commit(#kvdb_ref{db = #db{ref = {#kvdb_ref{mod = M1,db=Db1},
				 #kvdb_ref{} = KR2}}} = Ref) ->
    #commit{} = Set = M1:commit_set(Db1),
    ?debug("Commit set: ~p~n", [Set]),
    kvdb_lib:commit(Set, KR2),
    fire_events(Ref).


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

add_table(#kvdb_ref{db = {#kvdb_ref{mod=M1,db=Db1},_}} = Ref, Tab, Opts) ->
    case info(Ref, {Tab, type}) of
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

extract(_Db, _Tab, _AbsKey) ->
    error(nyi).

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
		{_, {ok,O1}} when K2 >= element(1,O1) ->
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
    case M1:int_read(Db1, {queue_op, Tab, Q}) of
	{ok, true} ->
	    M1:is_queue_empty(Db1, Tab, Q);
	_ ->
	    M2:is_queue_empty(Db2, Tab, Q)
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

list_queue(_Db, _Tab, _Q) ->
    error(nyi).

mark_queue_object(_Db, _Tab, _AbsKey, _St) ->
    error(nyi).

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

pop(_Db, _Tab, _Q) ->
    error(nyi).

prev(_Db, _Tab, _K) ->
    error(nyi).

push(_Db, _Tab, _Q, _Obj) ->
    error(nyi).

update_counter(#db{ref = {#kvdb_ref{mod=M1,db=Db1},_}} = Db, Tab, K, Incr) ->
    case get(Db, Tab, K) of
	{ok, Obj} ->
	    Sz = size(Obj),
	    NewV = case element(Sz, Obj) of
		       I when is_integer(I) ->
			   I+1;
		       B when is_binary(B) ->
			   Sz = bit_size(B),
			   <<I:Sz/integer>> = B,
			   NewI = I + Incr,
			   <<NewI:Sz/integer>>
		   end,
	    M1:put(Db1, Tab, setelement(Sz, Obj, NewV));
	_ ->
	    error(not_found)
    end.

ensure_table(Tab, #kvdb_ref{mod = M1, db = Db1},
	     #kvdb_ref{mod = M2, db = Db2}) ->
    case M2:info(Db2, {Tab, tabrec}) of
	undefined ->
	    error({no_such_table, Tab});
	#table{} = TabR ->
	    M1:int_write(Db1, {tabrec, Tab}, TabR),
	    true
    end.
