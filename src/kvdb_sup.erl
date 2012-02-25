
-module(kvdb_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).
-export([childspec/1]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    Children = childspecs(DBs = get_databases()),
    io:fwrite("DBs = ~p~n", [DBs]),
    {ok, { {one_for_one, 5, 10}, Children} }.


childspecs(DBs) ->
    [childspec(DB) || DB <- lists:concat(DBs)].

childspec({Name, Opts}) ->
    {Name, {kvdb, start_link, [Name, Opts]},
     permanent, 5000, worker, [kvdb]}.

get_databases() ->
    OtherDBs = [DB || {_, DB} <- setup:find_env_vars(kvdb_databases)],
    case application:get_env(databases) of
	{ok, DBs} when is_list(DBs) ->
	    [DBs | OtherDBs];
	_ ->
	    OtherDBs
    end.
