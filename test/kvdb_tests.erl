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
%%% Created : 30 Dec 2011 by Tony Rogvall <tony@rogvall.se>

-module(kvdb_tests).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").
-include("kvdb.hrl").

%% for testing only
-export([test_tree/0]).

-define(match(X, A, B), ?assertMatch({_,X,A}, {?LINE,X,B})).

%% Inspired by eunit's ?debugVal(E)
-define(dbg(Db,E),
	(fun() ->
		 try (E) of
		     __V ->
			 ?debugFmt(<<"db = ~p; ~s = ~P">>, [Db,(??E), __V, 15]),
			 __V
		 catch
		     error:__Err ->
			 io:fwrite(user,
				   "FAIL: db = ~p; test = ~s~n"
				   "Error = ~p~n"
				   "Trace = ~p~n", [Db,(??E), __Err,
						    erlang:get_stacktrace()]),
			 error(__Err)
		 end
	  end)()).

-define(CATCH(E),
	try (E)
	catch
	    error:Err ->
		io:fwrite(user, "~w/~w: ERROR: ~p~n    ~p~n",
			  [?MODULE,?LINE,Err,
			   erlang:get_stacktrace()]),
		{'EXIT', Err}
	end).

-compile(export_all).

%%
basic_test_() ->
    {setup,
     fun() ->
	     ?debugVal(application:start(gproc)),
	     ?debugVal(application:start(locks)),
	     ?debugVal(application:start(kvdb)),
	     ok
     end,
     fun(_) ->
	     ?debugVal(application:stop(kvdb)),
	     ?debugVal(application:stop(locks)),
	     ?debugVal(application:stop(gproc)),
	     ?debugVal([catch delete_db_file(Name,Backend) ||
			   {Name, _, Backend} <- dbs()])
     end,
     [{foreachx,
       fun({Db, Enc, Backend}) ->
               try create_db(Db, Enc, Backend)
               catch
                   error:Reason ->
                       io:fwrite(user,
                                 "ERROR: ~p~n"
                                 "create_db(~p, ~p, ~p)~n",
                                 [Reason, Db,Enc,Backend]),
                       error(Reason)
               end
       end,
       [{{Db,E,B}, fun({Db1,_,_},_) ->
			   [
			    ?_test(?dbg(Db,fill_db(Db1)))
			    , ?_test(?dbg(Db,first_next(Db1)))
			    , ?_test(?dbg(Db,first_next2(Db1)))
			    , ?_test(?dbg(Db,last_prev(Db1)))
			    , ?_test(?dbg(Db,get_attrs(Db1)))
			    , ?_test(?dbg(Db,prefix_match(Db1)))
			    , ?_test(?dbg(Db,prefix_match2(Db1)))
			    , ?_test(?dbg(Db,prefix_match_rel(Db1)))
			    , ?_test(?dbg(Db,add_delete_add_tab(Db1)))
			    , ?_test(?dbg(Db,queue(Db1)))
			    , ?_test(?dbg(Db,subqueues(Db1)))
			    , ?_test(?dbg(Db,first_next_queue(Db1)))
			    , ?_test(?dbg(Db,prel_pop(Db1)))
			    , ?_test(?dbg(Db,q_inactive(Db1)))
			    , ?_test(?dbg(Db,each_index(Db1)))
			    , ?_test(?dbg(Db,word_index(Db1)))
			   ]
		   end} ||
	   {Db,E,B} <- dbs()]
      }]}.

%% open_test_() ->
%%     {setup,
%%      fun() ->
%% 	     application:start(gproc),
%% 	     application:load(kvdb)
%%      end,
%%      fun(_) ->
%% 	     application:stop(kvdb),
%% 	     applicaiton:stop(gproc)
%%      end,
%%      [?_test(?dbg(dbs, fun(_) ->
%% 			       application:set_env(kvdb, databases,
%% 						   [{N,[{encoding,E},
%% 							{backend,B}]} ||
%% 						       {N,E,B} <- dbs()]),
%% 			       ok = application:start(kvdb)
%% 		       end))]}.

dbs() ->
    [
     {s1,sext,sqlite3},
     {s2,raw,sqlite3},
     {l1,sext,leveldb},
     {l2,raw,leveldb},
     {e1,sext,ets},
     {e2,raw,ets},
     {p1,raw,{kvdb_paired, [{module1, kvdb_ets},
			    {module2, kvdb_leveldb},
			    {options2, [{update_index, false}]}]}}
    ].

create_db(Name, Encoding, Backend) ->
    BackendName = if is_tuple(Backend) -> element(1, Backend);
		     is_atom(Backend)  -> Backend
	      end,
    File = db_file(Name, BackendName),
    ok = delete_db_file(Name, BackendName),
    BasicOpts = [{file,File},{encoding,Encoding}],
    Opts = case Backend of
	       {B, ExtraOpts} when is_list(ExtraOpts) ->
		   BasicOpts ++ [{backend, B} | ExtraOpts];
	       B when is_atom(Backend) ->
		   [{backend, B} | BasicOpts]
	   end,
    {ok,Db} = kvdb:open(Name, Opts),
    kvdb:add_table(Db, type),
    kvdb:add_table(Db, value),
    Name.


fill_db(Db) ->
    Types = types(),
    lists:foreach(fun({K,V}) ->
			  ?match(Db, ok, kvdb:put(Db,type,{K,V}))
		  end, Types),

    Values = values(),
    lists:foreach(fun({K,V}) ->
			  ?match(Db, ok, kvdb:put(Db,value,{K,V}))
		  end, Values),

    lists:foreach(fun({K,V}) ->
			  ?match(Db, {ok,{K,V}}, kvdb:get(Db,value,K))
		  end, Values),

    AllValues = get_all(Db, value),
    timer:sleep(50),
    ?match(Db, AllValues, lists:sort(Values)),

    AllTypes = get_all(Db, type),
    timer:sleep(50),
    ?match(Db, AllTypes, lists:sort(Types)),
    ok.

types() ->
   [{ <<"/config">>, <<"dir:interface">> },
    { <<"/config/test/1">>, <<"a">> },
    { <<"/config/test/2">>, <<"b">> },
    { <<"/config/test/3">>, <<"c">> },
    { <<"/config/interface">>, <<"list">> },
    { <<"/config/interface/*">>, <<"dir:name,ether,media,status">> },
    { <<"/config/interface/*/name">>,    <<"leaf:string">> },
    { <<"/config/interface/*/ether">>,   <<"leaf:ethermac">> },
    { <<"/config/interface/*/media">>,   <<"leaf:enum">> },
    { <<"/config/interface/*/status">>,  <<"leaf:enum">> }].

values() ->
    [{<<"/config/interface/1/name">>, <<"eth0">>},
     {<<"/config/interface/1/ether">>,<<"00:26:08:e6:ae:47">>},
     {<<"/config/interface/1/media">>,<<"autoselect">>},
     {<<"/config/interface/1/status">>,<<"active">>}].

first_next(Db) ->
    SortedTypes = lists:sort(types()),
    SortedVals = lists:sort(values()),
    FirstType = hd(SortedTypes),
    FirstValue = hd(SortedVals),
    ?match({Db,FirstType}, {ok, FirstType}, kvdb:first(Db, type)),
    ?match({Db,FirstValue}, {ok, FirstValue}, kvdb:first(Db, value)),
    next(SortedTypes, Db, type),
    next(SortedVals, Db, value).

first_next2(Db) ->
    ok = kvdb:add_table(Db, tfn, [{encoding, {raw,sext,term}}]),
    ok = kvdb:put(Db, tfn, {<<"a">>, [{a,1}], 1}),
    ok = kvdb:put(Db, tfn, {<<"b">>, [{b,2}], 2}),
    ok = kvdb:put(Db, tfn, {<<"c">>, [{c,3}], 3}),
    {ok, {<<"a">>,[{a,1}],1}} = kvdb:first(Db, tfn),
    {ok, {<<"b">>,[{b,2}],2}} = kvdb:next(Db, tfn, <<"a">>),
    {ok, {<<"b">>,[{b,2}],2}} = kvdb:prev(Db, tfn, <<"c">>),
    {ok, {<<"c">>,[{c,3}],3}} = kvdb:last(Db, tfn),
    kvdb:delete_table(Db, tfn).

next([{K,_}], Db, Tab) ->
    ?match({Db,K}, done, kvdb:next(Db, Tab, K)),
    ok;
next([{K,_},Next|Tail], Db, Tab) ->
    ?match({Db,K, Next}, {ok, Next}, kvdb:next(Db, Tab, K)),
    next([Next|Tail], Db, Tab).

last_prev(Db) ->
    SortedTypes = lists:reverse(lists:sort(types())),
    SortedVals = lists:reverse(lists:sort(values())),
    LastType = hd(SortedTypes),
    LastValue = hd(SortedVals),
    ?assertMatch({LastType, {ok, LastType}}, {LastType, kvdb:last(Db, type)}),
    ?assertMatch({LastValue, {ok, LastValue}}, {LastValue, kvdb:last(Db, value)}),
    prev(SortedTypes, Db, type),
    prev(SortedVals, Db, value).

prev([{K,_}], Db, Tab) ->
    ?assertMatch({K, done}, {K, kvdb:prev(Db, Tab, K)}),
    ok;
prev([{K,_},Next|Tail], Db, Tab) ->
    ?assertMatch({K, Next, {ok, Next}}, {K, Next, kvdb:prev(Db, Tab, K)}),
    prev([Next|Tail], Db, Tab).

get_attrs(Db) ->
    ok = kvdb:add_table(Db, ta, [{encoding, {raw,sext,term}}]),
    ok = kvdb:put(Db, ta, {<<"a">>, [{a,1},{b,2},{c,3}], 1}),
    {ok, [{a,1},{c,3}]} = kvdb:get_attrs(Db, ta, <<"a">>, [a,c]),
    {ok, [{a,1},{b,2},{c,3}]} = kvdb:get_attrs(Db, ta, <<"a">>, all),
    kvdb:delete_table(Db, ta).

prefix_match(Db) ->
    kvdb:add_table(Db, tr, [{encoding, raw}]),
    kvdb:put(Db, tr, {<<"aabb">>, <<"1">>}),
    kvdb:put(Db, tr, {<<"aacc">>, <<"2">>}),
    kvdb:put(Db, tr, {<<"aadd">>, <<"3">>}),
    kvdb:put(Db, tr, {<<"abcc">>, <<"4">>}),
    kvdb:put(Db, tr, {<<"abcd">>, <<"5">>}),
    {[ {<<"aabb">>, <<"1">>}, {<<"aacc">>, <<"2">>}], C1} =
	kvdb:prefix_match(Db, tr, <<"aa">>, 2),
    case C1() of
	{[ {<<"aadd">>, <<"3">>}], C2} ->
	    case C2() of
		{[], _} -> ok;
		done -> ok;
		Other2 ->
		    error({badmatch, Other2})
	    end;
	Other1 ->
	    error({badmatch, Other1})
    end,
    kvdb:delete_table(Db, tr).

prefix_match2(Db) ->
    kvdb:add_table(Db, ts, [{encoding, sext}]),
    kvdb:put(Db, ts, {{a,1}, 1}),
    kvdb:put(Db, ts, {{a,2}, 2}),
    kvdb:put(Db, ts, {{a,3}, 3}),
    kvdb:put(Db, ts, {{b,1}, 4}),
    kvdb:put(Db, ts, {{b,2}, 5}),
    { [{{a,1},1}, {{a,2},2}], C1} =
	kvdb:prefix_match(Db, ts, {a,'_'}, 2),
    case C1() of
	{[{{a,3}, 3}], C2} ->
	    case C2() of
		{[], _} -> ok;
		done -> ok;
		Other2 ->
		    error({badmatch, Other2})
	    end;
	Other1 ->
	    error({badmatch, Other1})
    end,
    { [{{a,1},1}, {{a,2},2}, {{a,3},3}, {{b,1},4}, {{b,2},5}], C3} =
	kvdb:prefix_match(Db, ts, '_'),
    done = C3().

%% prefix_match(Db) ->
%%     Prefix = <<"/config/test">>,
%%     SortedTypes = lists:sort(types()),
%%     Subset = [{K,V} || {K,V} <- SortedTypes,
%% 		       is_tuple(binary:match(K, Prefix))],
%%     ?debugVal(Subset),
%%     ?match(Db, {SortedTypes, _}, catch kvdb:prefix_match(Db, type, <<>>)),
%%     ?match(Db, {Subset, _}, catch kvdb:prefix_match(Db, type, Prefix)),
%%     ok.

prefix_match_rel(Db) ->
    Prefix = <<"/config/test">>,
    SortedTypes = lists:sort(types()),
    Subset = [{K,V} || {K,V} <- SortedTypes,
		       is_tuple(binary:match(K, Prefix))],
    ?match(Db, {Subset, _}, catch kvdb:prefix_match(Db, type, Prefix)),
    {[{K1,_} = O1],_} = kvdb:prefix_match(Db, type, Prefix, 1),
    {[{K2,_} = O2],_} = kvdb:prefix_match_rel(Db,type,Prefix,K1,1),
    {[{_,_}  = O3],_} = kvdb:prefix_match_rel(Db,type,Prefix,K2,1),
    ?match(Db, Subset, [O1,O2,O3]),
    ok.


add_delete_add_tab(Db) ->
    add_delete_tab(Db),
    add_delete_tab(Db).

add_delete_tab(Db) ->
    case Db of
	s ->
	    ?debugFmt("sqlite3 driver error messages expected~n", []),
	    timer:sleep(50);
	_ -> ok
    end,
    ?match(Db, {error, not_found}, catch kvdb:get(Db, x, <<"1">>)),
    ?match(Db, ok, catch kvdb:add_table(Db, x, [])),
    ?match(Db, ok, catch kvdb:put(Db, x, {<<"1">>, <<"a">>})),
    ?match(Db, {ok,{<<"1">>,<<"a">>}}, catch kvdb:get(Db, x, <<"1">>)),
    ?match(Db, ok, catch kvdb:delete_table(Db, x)),
    ?match(Db, {error, not_found}, catch kvdb:get(Db, x, <<"1">>)),
    ok.

queue(Db) ->
    queue(Db, fifo, raw),
    queue(Db, lifo, raw),
    queue(Db, fifo, sext),
    queue(Db, lifo, sext).

queue(Db, Type, Enc) ->
    M = {Db,Type,Enc},
    Q = <<"q1_", (atom_to_binary(Db, latin1))/binary, "_",
	  (atom_to_binary(Type,latin1))/binary, "_",
	  (atom_to_binary(Enc, latin1))/binary>>, % binary, parameterized table name
    ?match(M, ok, kvdb:add_table(Db, Q, [{type, Type},{encoding, Enc}])),
    [{ok,_K1},
     {ok,K2},
     {ok,_K3}] = [kvdb:push(Db, Q, Obj)
		  || Obj <- [{<<"1">>,<<"a">>},
			     {<<"2">>,<<"b">>},
			     {<<"3">>,<<"c">>}]],
    ?match(M, #q_key{}, K2),
    ?match(M, {ok, {<<"2">>,<<"b">>}}, kvdb:extract(Db, Q, K2)),
    if Type == lifo ->
	    ?match(M, {ok, {<<"3">>,<<"c">>}}, catch kvdb:pop(Db, Q)),
	    ?match(M, {ok, {<<"1">>,<<"a">>}}, catch kvdb:pop(Db, Q)),
	    ?match(M, done, catch kvdb:pop(Db, Q));
       Type == fifo ->
	    ?match(M, {ok, {<<"1">>,<<"a">>}}, ?CATCH(kvdb:pop(Db, Q))),
	    ?match(M, {ok, {<<"3">>,<<"c">>}}, ?CATCH(kvdb:pop(Db, Q))),
	    ?match(M, done, catch kvdb:pop(Db, Q))
    end,
    ?match(M, ok, catch kvdb:delete_table(Db, Q)),
    ok.

subqueues(Db) ->
    subqueues(Db, fifo, raw),
    subqueues(Db, lifo, raw),
    subqueues(Db, fifo, sext),
    subqueues(Db, lifo, sext).

subqueues(Db, Type, Enc) ->
    io:fwrite(user, "subqueues(~p, ~p, ~p)~n", [Db, Type, Enc]),
    T = <<"q_",(atom_to_binary(Type,latin1))/binary, "_",
	  (atom_to_binary(Enc, latin1))/binary>>, % binary, parameterized table name
    M = {Db,Type,Enc,T},
    Qs = if Enc == raw -> [<<"q1">>, <<"q2">>, <<"q3">>];
	    Enc == sext -> [1,2,3]
	 end,
    ?match(M, ok, kvdb:add_table(Db, T, [{type, Type},{encoding, Enc}])),
    PushResults =
	[kvdb:push(Db, T, Q, Obj) || Q <- Qs,
				     Obj <- [{<<"1">>,<<"a">>},
					     {<<"2">>,<<"b">>},
					     {<<"3">>,<<"c">>}]],
    ?match(M, true, lists:all(fun({ok,_}) -> true; (_) -> false end, PushResults)),
    if Type == lifo ->
	    ?match(
	       M,
	       [{ok,{<<"3">>,<<"c">>}},
		{ok, {<<"2">>,<<"b">>}},
		{ok, {<<"1">>,<<"a">>}},
		done], [kvdb:pop(Db, T, lists:nth(2,Qs)) || _ <- [1, 2, 3, 4]]),
	    ?match(
	       M,
	       [{ok,{<<"3">>,<<"c">>}},
		{ok, {<<"2">>,<<"b">>}},
		{ok, {<<"1">>,<<"a">>}},
		done], [kvdb:pop(Db, T, lists:nth(3,Qs)) || _ <- [1, 2, 3, 4]]),
	    ?match(
	       M,
	       [{ok,{<<"3">>,<<"c">>}},
		{ok, {<<"2">>,<<"b">>}},
		{ok, {<<"1">>,<<"a">>}},
		done], [kvdb:pop(Db, T, lists:nth(1,Qs)) || _ <- [1, 2, 3, 4]]);
       Type == fifo ->
	    ?match(
	       M,
	       [{ok,{<<"1">>,<<"a">>}},
		{ok, {<<"2">>,<<"b">>}},
		{ok, {<<"3">>,<<"c">>}},
		done], [kvdb:pop(Db, T, lists:nth(2,Qs)) || _ <- [1, 2, 3, 4]]),
	    ?match(
	       M,
	       [{ok,{<<"1">>,<<"a">>}},
		{ok, {<<"2">>,<<"b">>}},
		{ok, {<<"3">>,<<"c">>}},
		done], [kvdb:pop(Db, T, lists:nth(3,Qs)) || _ <- [1, 2, 3, 4]]),
	    ?match(
	       M,
	       [{ok,{<<"1">>,<<"a">>}},
		{ok, {<<"2">>,<<"b">>}},
		{ok, {<<"3">>,<<"c">>}},
		done], [kvdb:pop(Db, T, lists:nth(1,Qs)) || _ <- [1, 2, 3, 4]])
    end,
    ?match(M, ok, catch kvdb:delete_table(Db, T)),
    io:fwrite(user, "...done~n", []),
    ok.

first_next_queue(Db) ->
    first_next_queue(Db, fifo, raw),
    first_next_queue(Db, lifo, raw),
    first_next_queue(Db, fifo, sext),
    first_next_queue(Db, lifo, sext),
    first_next_queue(Db, {keyed,fifo}, raw),
    first_next_queue(Db, {keyed,fifo}, sext).

type_to_binary({keyed,T}) ->
    <<"keyed_", (atom_to_binary(T, latin1))/binary>>;
type_to_binary(T) ->
    atom_to_binary(T, latin1).


first_next_queue(Db, Type, Enc) ->
    T = <<"q1_",(atom_to_binary(Db,latin1))/binary, "_",
	  (type_to_binary(Type))/binary, "_",
	  (atom_to_binary(Enc, latin1))/binary>>, % binary, parameterized table name
    M = {Db,Type,Enc,T},
    Qs = if Enc == raw -> [<<"q1">>, <<"q2">>, <<"q3">>, <<"q4">>];
	    Enc == sext -> [1,2,3,4]
	 end,
    ?match(M, ok, kvdb:add_table(Db, T, [{type, Type},{encoding, Enc}])),
    [First, Second, Third, Fourth] = lists:sort(Qs),
    _PushResults =
	[kvdb:push(Db, T, Q, Obj)
	 || Q <- lists:delete(Third, Qs),
	    Obj <- [{<<"1">>,<<"a">>},
		    {<<"2">>,<<"b">>},
		    {<<"3">>,<<"c">>}]],
    kvdb:push(Db, T, Third, {<<"4">>, <<"d">>}),
    ?match(M, {ok,{<<"4">>,<<"d">>}}, kvdb:pop(Db, T, Third)),
    %% Third is now empty
    ?match(M, {ok, First}, kvdb:first_queue(Db, T)),
    ?match(M, {ok, Second}, kvdb:next_queue(Db, T, First)),
    ?match(M, {ok, Fourth}, kvdb:next_queue(Db, T, Second)),
    ?match(M, {ok, Fourth}, kvdb:next_queue(Db, T, Third)),
    ?match(M, done, kvdb:next_queue(Db, T, Fourth)),
    ?match(M, ok, catch kvdb:delete_table(Db, T)),
    ok.

prel_pop(Db) ->
    M = {_,Type,Enc,T} = {Db,fifo,sext,qp_fifo_sext},
    ?match(M, ok, kvdb:add_table(Db, T, [{type,Type},{encoding,Enc}])),
    Q = qp,
    _PushResults =
	[kvdb:push(Db, T, Q, Obj)
	 || Obj <- [{1,a},
		    {2,b}]],
    PopRes = kvdb:prel_pop(Db, T, Q),
    ?match(M, {ok, {1,a}, _}, PopRes),
    {ok, _, Key} = PopRes,
    ?match(M, {ok, blocking, {1,a}}, catch kvdb:queue_read(Db, T, Key)),
    ?match(M, {'EXIT',_}, catch kvdb:get(Db, T, Key)),
    ?match(M, blocked, kvdb:pop(Db, T, Q)),
    ?match(M, blocked, kvdb:prel_pop(Db, T, Q)),
    ?match(M, ok, kvdb:queue_delete(Db, T, Key)),
    ?match(M, {ok, {2,b}}, kvdb:pop(Db, T, Q)),
    ?match(M, ok, catch kvdb:delete_table(Db, T)),
    ok.

q_inactive(Db) ->
    M = {_, Type,Enc,T} = {Db,fifo,sext,qi_fifo_sext},
    Q = <<"q">>,
    ?match(M, ok, kvdb:add_table(Db, T, [{type,Type},{encoding,Enc}])),
    ok = kvdb:queue_insert(Db, T, #q_key{queue = Q,
					 ts = 0,
					 key = <<>>}, inactive, {0,a}),
    _PushResults =
	[kvdb:push(Db, T, Q, Obj)
	 || Obj <- [{1,a},
		    {2,b}]],
    {ok, {1,a}} = kvdb:pop(Db, T, Q),
    ?match(M, ok, catch kvdb:delete_table(Db, T)),
    ok.

queue_insert(Db) ->
    M = {_, Type,Enc,T} = {Db,fifo,sext,q_fifo_sext},
    ?match(M, ok, kvdb:add_table(Db, T, [{type,Type},{encoding,Enc}])),
    Q = q,
    {ok, QKey} = kvdb:push(Db, T, Q, {1,a}),
    ?match(M, {ok, active, {1,a}}, kvdb:queue_read(Db, T, QKey)),
    ?match(M, ok, catch kvdb:delete_table(Db, T)).

indexing(Db) ->
    [indexing_(X) || X <- [{Db, set, {sext,term,sext}},
			   {Db, set, {sext,sext,sext}}]].

indexing_({Db,Type,Enc}) ->
    T = tabname(<<"ix">>, ?LINE, Db, Type, Enc),
    M = {Db,Type,Enc,T},
    ?match(M, ok, kvdb:add_table(Db, T, [{type,Type},{encoding,Enc},
					 {index, [a,
						  {"b",each,b},
						  {{c},words,c}]}])),
    ?match(M, ok, kvdb:put(Db, T, {1,[{a,11}],a})),
    ?match(M, [{1,[{a,11}],a}], kvdb:index_get(Db, T, a, 11)),
    ?match(M, ok, kvdb:put(Db, T, {2,[{a,11}],b})),
    ?match(M, [{1,[{a,11}],a},
	       {2,[{a,11}],b}], kvdb:index_get(Db, T, a, 11)),
    ?match(M, ok, catch kvdb:delete_table(Db, T)),
    ok.

each_index(Db) ->
    T = tabname(<<"ix_each">>, ?LINE, Db, Type = set, Enc = {sext,sext,sext}),
    M = {Db,Type,Enc,T},
    ?match(M, ok, kvdb:add_table(Db, T, [{type,Type},{encoding,Enc},
					 {index, [a,
						  {"b",each,b},
						  {{c},words,c}]}])),
    ?match(M, ok, kvdb:put(Db, T, {1,[{b,[x,y,z]}],a})),
    ?match(M, [{1,[{b,[x,y,z]}],a}], kvdb:index_get(Db, T, "b", x)),
    ?match(M, [{1,[{b,[x,y,z]}],a}], kvdb:index_get(Db, T, "b", y)),
    ?match(M, [{1,[{b,[x,y,z]}],a}], kvdb:index_get(Db, T, "b", z)),
    ?match(M, ok, kvdb:put(Db, T, {1,[{b,[x,y]}],a})),
    ?match(M, [{1,[{b,[x,y]}],a}], kvdb:index_get(Db, T, "b", x)),
    ?match(M, [{1,[{b,[x,y]}],a}], kvdb:index_get(Db, T, "b", y)),
    ?match(M, [], kvdb:index_get(Db, T, "b", z)),
    ?match(M, ok, kvdb:put(Db, T, {2,[{b,[y,z]}],b})),
    ?match(M, [{1,[{b,[x,y]}],a}], kvdb:index_get(Db, T, "b", x)),
    ?match(M, [{1,[{b,[x,y]}],a},
	       {2,[{b,[y,z]}],b}], kvdb:index_get(Db, T, "b", y)),
    ?match(M, [{2,[{b,[y,z]}],b}], kvdb:index_get(Db, T, "b", z)),
    ?match(M, ok, catch kvdb:delete_table(Db, T)),
    ok.

word_index(Db) ->
    T = tabname(<<"ix_words">>, ?LINE, Db, Type = set, Enc = {sext,sext,sext}),
    M = {Db,Type,Enc,T},
    ?match(M, ok, kvdb:add_table(Db, T, [{type,Type},{encoding,Enc},
					 {index, [a,
						  {"b",each,b},
						  {{c},words,c},
						  {val, value, {value}},
						  {v, each, {value}},
						  {w, words,{value}} ]}])),
    Txt1 = <<"a b c 123">>,
    Txt2 = <<"c 123">>,
    Obj1 = {1,[{c,Txt1}],a},
    Obj2 = {1,[{c, Txt2}],[a,b,c]},
    Obj3 = {2,[{c,Txt1}],b},
    ?match(M, ok, kvdb:put(Db, T, Obj1)),
    ?match(M, [Obj1], kvdb:index_get(Db, T, {c}, <<"a">>)),
    ?match(M, [Obj1], kvdb:index_get(Db, T, {c}, <<"b">>)),
    ?match(M, [Obj1], kvdb:index_get(Db, T, {c}, <<"123">>)),
    ?match(M, [Obj1], kvdb:index_get(Db, T, val, a)),
    ?match(M, ok, kvdb:put(Db, T, Obj2)),
    ?match(M, [Obj2], kvdb:index_get(Db, T, {c}, <<"c">>)),
    ?match(M, [], kvdb:index_get(Db, T, {c}, <<"a">>)),
    ?match(M, [Obj2], kvdb:index_get(Db, T, v, a)),
    ?match(M, [Obj2], kvdb:index_get(Db, T, v, b)),
    ?match(M, [Obj2], kvdb:index_get(Db, T, v, c)),
    ?match(M, [], kvdb:index_get(Db, T, {c}, <<"a">>)),
    ?match(M, [], kvdb:index_get(Db, T, {c}, <<"a">>)),
    ?match(M, ok, kvdb:put(Db, T, Obj3)),
    ?match(M, [{2,[{c,Txt1}],b}], kvdb:index_get(Db, T, {c}, <<"a">>)),
    ?match(M, [Obj2, Obj3], kvdb:index_get(Db, T, {c}, <<"c">>)),
    ?match(M, ok, kvdb:delete(Db, T, 1)),
    ?match(M, [], kvdb:index_get(Db, T, v, a)),
    ?match(M, [], kvdb:index_get(Db, T, v, b)),
    ?match(M, [], kvdb:index_get(Db, T, v, c)),
    ?match(M, [Obj3], kvdb:index_get(Db, T, {c}, <<"c">>)),
    ?match(M, ok, catch kvdb:delete_table(Db, T)),
    ok.

tabname(Pfx, L, Db,Type,Enc) ->
    <<Pfx/binary, "_", (list_to_binary(integer_to_list(L)))/binary, "_",
      (atom_to_binary(Db, latin1))/binary, "_",
      (type_bin(Type))/binary, "_", (enc_bin(Enc))/binary>>.

type_bin({keyed,T}) ->
    <<"keyed_", (atom_to_binary(T, latin1))/binary>>;
type_bin(T) when is_atom(T) ->
    atom_to_binary(T, latin1).

enc_bin({A,B,C}) ->
    <<(atom_to_binary(A,latin1))/binary, "_",
      (atom_to_binary(B,latin1))/binary, "_",
      (atom_to_binary(C,latin1))/binary>>;
enc_bin({A,B}) ->
    <<(atom_to_binary(A,latin1))/binary, "_",
      (atom_to_binary(B,latin1))/binary>>;
enc_bin(E) ->
    atom_to_binary(E, latin1).


%% These are not run automatically by eunit
%%
basic_test(Backend) ->
    File = db_file("test", Backend),
    {ok,Db} = kvdb:open({test,Backend}, [{file,File},{backend,Backend}]),
    kvdb:add_table(Db, type),
    kvdb:add_table(Db, value),
    Types = [
	     { <<"/config">>, <<"dir:interface">> },
	     { <<"/config/interface">>, <<"list">> },
	     { <<"/config/interface/*">>, <<"dir:name,ether,media,status">>},
	     { <<"/config/interface/*/name">>,    <<"leaf:string">> },
	     { <<"/config/interface/*/ether">>,   <<"leaf:ethermac">> },
	     { <<"/config/interface/*/media">>,   <<"leaf:enum">> },
	     { <<"/config/interface/*/status">>,  <<"leaf:enum">> }
	    ],

    lists:foreach(fun({K,V}) ->
			  ?assertMatch(ok, kvdb:put(Db,type,{K,V}))
		  end, Types),

    Values = [
	      {<<"/config/interface/1/name">>, <<"eth0">>},
	      {<<"/config/interface/1/ether">>,<<"00:26:08:e6:ae:47">>},
	      {<<"/config/interface/1/media">>,<<"autoselect">>},
	      {<<"/config/interface/1/status">>,<<"active">>}
	     ],

    lists:foreach(fun({K,V}) ->
			  ?assertMatch(ok, kvdb:put(Db,value,{K,V}))
		  end, Values),

    lists:foreach(fun({K,V}) ->
			  ?assertMatch({ok,{K,V}}, kvdb:get(Db,value,K))
		  end, Values),

    AllValues = get_all(Db, value),
    ?assertMatch(AllValues, lists:sort(Values)),

    AllTypes = get_all(Db, type),
    ?assertMatch(AllTypes, lists:sort(Types)),
    ok.

speed_test(N) ->
    speed_test(N, kvdb_sqlite3),
    speed_test(N, kvdb_leveldb),
    speed_test(N, kvdb_ets).

speed_test(N, Backend) ->
    {_,Ratio1} = speed_insert_test(N, Backend),
    {_,Ratio2} = speed_update_test(N, Backend),
    {_,Ratio3} = speed_fetch_test(N, Backend),
    io:format("backend: ~w, n=~w, insert=~f, update=~f, fetch=~f\n",
	      [Backend, N,
	       Ratio1*1000000.0,
	       Ratio2*1000000.0,
	       Ratio3*1000000.0]).

%% insert N unique keys into a date base with a value 256 binary data
speed_insert_test(N, Backend) ->
    ok = delete_db_file("speed", Backend),
    Value = list_to_binary(lists:seq(0,255)),
    File = db_file("speed", Backend),
    {ok,Db} = kvdb:open(test, [{file,File},{backend,Backend}]),
    kvdb:add_table(Db, speed),
    T0 = os:timestamp(),
    speed_put(Db, N, Value),
    T1 = os:timestamp(),
    kvdb:close(Db),
    T = timer:now_diff(T1, T0),
    {N, N/T}.

speed_update_test(N, Backend) ->
    Value = list_to_binary(lists:duplicate(255, 0)),
    File = db_file("speed", Backend),
    {ok,Db} = kvdb:open(test, [{file,File},{backend,Backend}]),
    T0 = os:timestamp(),
    speed_put(Db, N, Value),
    T1 = os:timestamp(),
    kvdb:close(Db),
    T = timer:now_diff(T1, T0),
    {N, N/T}.


speed_fetch_test(N, Backend) ->
    Value = list_to_binary(lists:duplicate(255, 0)),
    speed_get_test(N, Backend, Value).

speed_get_test(N, Backend, Value) ->
    File = db_file("speed", Backend),
    {ok,Db} = kvdb:open(test, [{file,File},{backend,Backend}]),
    T0 = os:timestamp(),
    speed_get(Db, N, Value),
    T1 = os:timestamp(),
    kvdb:close(Db),
    T = timer:now_diff(T1, T0),
    {N, N/T}.

speed_put(_, 0, _Value) ->
    ok;
speed_put(Db, I, Value) ->
    Key = erlang:md5(integer_to_list(I)),
    kvdb:put(Db, speed, {Key, Value}),
    %% {ok,Value} = kvdb:get(Db, speed, Key), %% verify!
    speed_put(Db, I-1, Value).

speed_get(_, 0, _Value) ->
    ok;
speed_get(Db, I, Value) ->
    Key = erlang:md5(integer_to_list(I)),
    case kvdb:get(Db, speed, Key) of
	{ok,{_,Value}} ->
	    speed_get(Db, I-1, Value);
	{ok,OtherValue} ->
	    io:format("FAIL: key=~w got value: ~w, expected value ~w\m",
		      [Key, OtherValue, Value]),
	    speed_get(Db, I-1, Value)
    end.



get_all(Db, Table) ->
    get_all(kvdb:first(Db, Table), Db, Table).

get_all(done, _, _) ->
    [];
get_all({ok,{Key,Value}}, Db, Table) ->
    [{Key, Value} | get_all(kvdb:next(Db, Table, Key), Db, Table)].


delete_db_file(Name, Backend) ->
    File = db_file(Name, backend_name(Backend)),
    case file:delete(File) of
	{error,enoent} -> ok;
	ok -> ok;
	{error,eperm} ->
	    delete_db_dir(File, Backend, {error,eperm});
	Result -> Result
    end.

backend_name({B, _}) ->
    B;
backend_name(B) when is_atom(B) ->
    B.


delete_db_dir(Dir, {kvdb_paired, Opts}, Error) ->
    {_, M2} = lists:keyfind(module2, 1, Opts),
    delete_db_dir(Dir, M2, Error);
delete_db_dir(Dir, Be, _Error) when Be==leveldb; Be==kvdb_leveldb ->
    case file:read_file_info(Dir) of
	{ok,Info} when Info#file_info.type =:= directory ->
	    case file:list_dir(Dir) of
		{ok, Files} ->
		    delete_db_dir_files(Dir, Files);
		Error1 ->
		    Error1
	    end;
	{ok,_} ->
	    {error, bad_file_type};
	Error1 ->
	    Error1
    end;
delete_db_dir(_Dir, _Backend, Error) ->
    Error.


delete_db_dir_files(Dir, []) ->
    file:del_dir(Dir);
delete_db_dir_files(Dir, [File|Files]) ->
    FileName = filename:join(Dir, File),
    case file:read_file_info(FileName) of
	{ok,Info} when Info#file_info.type =:= directory ->
	    SubDir = FileName,
	    case file:list_dir(SubDir) of
		{ok, Files1} ->
		    delete_db_dir_files(SubDir, Files1),
		    delete_db_dir_files(Dir, Files);
		Error ->
		    Error
	    end;
	{ok,Info} when Info#file_info.type =:= regular ->
	    file:delete(FileName),
	    delete_db_dir_files(Dir, Files);
	Error ->
	    Error
    end.



db_file(Name0, Backend) ->
    Dir = code:lib_dir(kvdb),
    Name = if is_atom(Name0) -> atom_to_list(Name0); true -> Name0 end,
    FileName = Name ++ "_" ++ atom_to_list(Backend) ++ ".db",
    filename:join([Dir, "test", FileName]).

test_tree() ->
    [
     {<<"a">>,[],<<"a data">>,
      [{<<"1">>,[],<<"1 data">>,
	[
	 {<<"aa">>,[],<<"aa data">>},
	 {<<"bb">>,[],<<"bb data">>}
	]
       },
       {<<"2">>,[],<<"2 data">>,
	[
	 {<<"zz">>, [], <<"zz data">>},
	 {<<"yy">>, [], <<"yy data">>}
	]}
      ]},
     {<<"b">>,[],<<"b data">>,
      [{<<"3">>,[],<<"3 data">>,
	[
	 {<<"cc">>,[],<<"cc data">>},
	 {<<"dd">>,[],<<"dd data">>}
	]}
      ]}].

-endif.
