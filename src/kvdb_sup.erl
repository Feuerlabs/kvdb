
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
    Children =
	case application:get_env(databases) of
	    {ok, DBs} when is_list(DBs) ->
		childspecs(DBs);
	    _ ->
		[]
	end,
    {ok, { {one_for_one, 5, 10}, Children} }.


childspecs(DBs) ->
    [childspec(DB) || DB <- DBs].

childspec({Name, Opts}) ->
    {Name, {kvdb, start_link, [Name, Opts]},
     permanent, 5000, worker, [kvdb]}.
