-module(kvdb_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1,
	 start_phase/3]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    io:fwrite("starting kvdb~n", []),
    %% dbg:tracer(),
    %% dbg:tpl(kvdb,x),
    %% dbg:tpl(kvdb_sup,x),
    %% dbg:tp(kvdb_sqlite,x),
    %% dbg:tp(kvdb_leveldb,x),
    %% dbg:p(all,[c]),
    kvdb_sup:start_link().

start_phase(open_dbs, _, []) ->
    Dbs = get_databases(),
    io:fwrite("KVDB Dbs = ~p~n", [Dbs]),
    [{ok, _} = kvdb_sup:start_child(Name, Opts) || {Name, Opts} <- Dbs],
    ok.

stop(_State) ->
    ok.



get_databases() ->
    %% If 'setup' is available, query for other databases
    OtherDBs =
	case lists:keymember(setup, 1, application:loaded_applications()) of
	    true ->
		[DB || {_, DB} <- setup:find_env_vars(kvdb_databases)];
	    false ->
		[]
	end,
    lists:flatten(
      case application:get_env(databases) of
	  {ok, DBs} when is_list(DBs) ->
	      [DBs | OtherDBs];
	  _ ->
	      OtherDBs
      end).
