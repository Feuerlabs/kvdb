%%% @author Tony Rogvall <tony@rogvall.se>
%%% @copyright (C) 2011, Tony Rogvall
%%% @doc
%%%    Some tests of kvdb
%%% @end
%%% Created : 30 Dec 2011 by Tony Rogvall <tony@rogvall.se>

-module(kvdb_test).

-include_lib("kernel/include/file.hrl").

-compile(export_all).

%% 
basic_test() ->
    basic_test(kvdb_ets),
    basic_test(kvdb_sqlite3),
    basic_test(kvdb_leveldb).
    
basic_test(Backend) ->
    File = db_file("test", Backend),
    {ok,Db} = kvdb:open(test, [{file,File},{backend,Backend}]),
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
			  io:format("put type ~p = ~p\n", [K,V]),
			  ok=kvdb:put(Db,type,K,V)
		  end, Types),

    Values = [
	      {<<"/config/interface/1/name">>, <<"eth0">>},
	      {<<"/config/interface/1/ether">>,<<"00:26:08:e6:ae:47">>},
	      {<<"/config/interface/1/media">>,<<"autoselect">>},
	      {<<"/config/interface/1/status">>,<<"active">>}
	     ],

    lists:foreach(fun({K,V}) ->
			  io:format("put value ~p = ~p\n", [K,V]),
			  ok=kvdb:put(Db,value,K,V)
		  end, Values),

    lists:foreach(fun({K,V}) ->
			  io:format("get value ~p\n", [K]),
			  {ok,V} = kvdb:get(Db,value,K)
		  end, Values),

    AllValues = get_all(Db, value),
    io:format("AllValues=~p\n", [AllValues]),
    AllValues = lists:reverse(lists:sort(Values)),

    AllTypes = get_all(Db, type),
    AllTypes = lists:reverse(lists:sort(Types)),

    kvdb:close(Db),
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
    kvdb:put(Db, speed, Key, Value),
    speed_put(Db, I-1, Value).

speed_get(_, 0, _Value) ->
    ok;
speed_get(Db, I, Value) ->
    Key = erlang:md5(integer_to_list(I)),
    case kvdb:get(Db, speed, Key) of
	{ok,Value} ->
	    speed_get(Db, I-1, Value);
	{ok,OtherValue} ->
	    io:format("FAIL: key=~w got value: ~w, expected value ~w\m",
		      [Key, OtherValue, Value]),
	    speed_get(Db, I-1, Value)
    end.
	    


get_all(Db, Table) ->
    {ok,Iter} = kvdb:iterator(Db, Table),
    case kvdb:first(Db, Iter) of
	done -> 
	    [];
	{ok,Key,Value} ->
	    get_iter_all(Db, Iter, [{Key,Value}])
    end.
	
get_iter_all(Db, Iter, Acc) ->
    case kvdb:next(Db, Iter) of
	done -> 
	    Acc;
	{ok,Key,Value} ->
	    get_iter_all(Db, Iter, [{Key,Value}|Acc])
    end.    

delete_db_file(Name, Backend) ->
    File = db_file(Name, Backend),
    case file:delete(File) of
	{error,enoent} -> ok;
	ok -> ok;
	{error,eperm} ->
	    delete_db_dir(File, Backend, {error,eperm});
	Result -> Result
    end.
	    
delete_db_dir(Dir, kvdb_leveldb, _Error) ->
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

