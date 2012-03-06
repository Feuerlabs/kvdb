%%% @author Tony Rogvall <tony@rogvall.se>
%%% @copyright (C) 2011, Tony Rogvall
%%% @doc
%%%    Some tests of kvdb
%%% @end
%%% Created : 30 Dec 2011 by Tony Rogvall <tony@rogvall.se>

-module(kvdb_test).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").

%% for testing only
-export([test_tree/0]).


-define(match(X, A, B), ?assertMatch({X,A}, {X,B})).

-define(CATCH(E),
	try (E)
	catch
	    error:Err ->
		io:fwrite(user, "~w/~w: ERROR: ~p~n    ~p~n", [?MODULE,?LINE,Err,
							       erlang:get_stacktrace()]),
		{'EXIT', Err}
	end).

-compile(export_all).

%%
basic_test_() ->
    {setup,
     fun() ->
	     ?debugVal(application:start(gproc)),
	     %% dbg:tracer(),
	     %% dbg:tp(kvdb,x),
	     %% dbg:ctp(kvdb,handle_call),
	     %% dbg:tp(sqlite3,x),
	     %% dbg:ctp(sqlite3,handle_call),
	     %% dbg:ctp(kvdb,handle_call),
	     %% dbg:tpl(kvdb_leveldb,x),
	     %% dbg:tpl(kvdb_test,x),
	     %% dbg:tpl(kvdb_leveldb, prefix_match_,x),
	     %% dbg:tp(kvdb_lib,is_prefix,x),
	     %% dbg:tp(binary,match,x),
	     %% dbg:tpl(kvdb_sqlite3,x),
	     %% dbg:tp(eleveldb,x),
	     %% dbg:tp(gproc,x),
	     %% dbg:p(all,[c]),
	     ?debugVal(application:start(kvdb)),
	     ok
     end,
     fun(_) ->
	     ?debugVal(application:stop(kvdb)),
	     ?debugVal(application:stop(gproc))
     end,
     [{foreachx,
       fun({Db, Enc, Backend}) -> ?debugVal(catch create_db(Db, Enc, Backend)) end,
       [{{Db,E,B}, fun({Db1,_,_},_) ->
			   [
			    ?_test(?debugVal(fill_db(Db1)))
			    , ?_test(?debugVal(first_next(Db1)))
			    , ?_test(?debugVal(last_prev(Db1)))
			    , ?_test(?debugVal(prefix_match(Db1)))
			    , ?_test(?debugVal(add_delete_add_tab(Db1)))
			    , ?_test(?debugVal(queue(Db1)))
			    , ?_test(?debugVal(subqueues(Db1)))
			    , ?_test(?debugVal(first_next_queue(Db1)))
			   ]
		   end} ||
	   {Db,E,B} <- [
			{s1,sext,sqlite3},
			{s2,raw,sqlite3},
			{l1,sext,leveldb},
			{l2,raw,leveldb}]]
      }]}.

create_db(Name, Encoding, Backend) ->
    F = "test" ++ atom_to_list(Name),
    File = db_file(F, Backend),
    ok = delete_db_file(F, Backend),
    {ok,Db} = kvdb:open(Name, [{file,File},{backend,Backend},{encoding,Encoding}]),
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

prefix_match(Db) ->
    Prefix = <<"/config/test">>,
    SortedTypes = lists:sort(types()),
    Subset = [{K,V} || {K,V} <- SortedTypes,
		       is_tuple(binary:match(K, Prefix))],
    ?debugVal(Subset),
    ?match(Db, {SortedTypes, _}, catch kvdb:prefix_match(Db, type, <<>>)),
    ?match(Db, {Subset, _}, catch kvdb:prefix_match(Db, type, Prefix)),
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
    ok.

first_next_queue(Db) ->
    first_next_queue(Db, fifo, raw),
    first_next_queue(Db, lifo, raw),
    first_next_queue(Db, fifo, sext),
    first_next_queue(Db, lifo, sext).

first_next_queue(Db, Type, Enc) ->
    T = <<"q1_",(atom_to_binary(Db,latin1))/binary, "_",
	  (atom_to_binary(Type,latin1))/binary, "_",
	  (atom_to_binary(Enc, latin1))/binary>>, % binary, parameterized table name
    M = {Db,Type,Enc,T},
    Qs = if Enc == raw -> [<<"q1">>, <<"q2">>, <<"q3">>, <<"q4">>];
	    Enc == sext -> [1,2,3,4]
	 end,
    ?match(M, ok, kvdb:add_table(Db, T, [{type, Type},{encoding, Enc}])),
    [First, Second, Third, Fourth] = lists:sort(Qs),
    PushResults =
	[kvdb:push(Db, T, Q, Obj)
	 || Q <- lists:delete(Third, Qs),
	    Obj <- [{<<"1">>,<<"a">>},
		    {<<"2">>,<<"b">>},
		    {<<"3">>,<<"c">>}]],
    kvdb:push(Db, T, Third, {<<"1">>, <<"a">>}),
    ?match(M, {ok,{<<"1">>,<<"a">>}}, kvdb:pop(Db, T, Third)),
    %% Third is now empty
    kvdb:first_queue(Db, T),
    ?match(M, {ok, Second}, kvdb:next_queue(Db, T, First)),
    ?match(M, {ok, Fourth}, kvdb:next_queue(Db, T, Second)),
    ?match(M, {ok, Fourth}, kvdb:next_queue(Db, T, Third)),
    ?match(M, done, kvdb:next_queue(Db, T, Fourth)),
    ?match(M, ok, catch kvdb:delete_table(Db, T)),
    ok.

%% with_trace(false, F)-> F();
%% with_trace(true, F) ->
%%     try
%% 	dbg:tracer(),
%% 	dbg:tpl(kvdb_sqlite3,x),
%% 	dbg:tp(kvdb_lib,x),
%% 	dbg:tp(sqlite3,read,x),
%% 	dbg:p(all,[c]),
%% 	F()
%%     after
%% 	dbg:ctpl(kvdb_sqlite3),
%% 	dbg:ctp(kvdb_lib),
%% 	dbg:ctp(sqlite3),
%% 	dbg:stop()
%%     end.

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
    File = db_file(Name, Backend),
    case file:delete(File) of
	{error,enoent} -> ok;
	ok -> ok;
	{error,eperm} ->
	    delete_db_dir(File, Backend, {error,eperm});
	Result -> Result
    end.

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



db_file(Name, Backend) ->
    Dir = code:lib_dir(kvdb),
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
