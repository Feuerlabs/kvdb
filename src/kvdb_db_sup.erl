%%%---- BEGIN COPYRIGHT -------------------------------------------------------
%%%
%%% Copyright (C) 2012 Feuerlabs, Inc. All rights reserved.
%%%
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at http://mozilla.org/MPL/2.0/.
%%%
%%%---- END COPYRIGHT ---------------------------------------------------------
%%% @author Ulf Wiger <ulf@wiger.net>
%%%
-module(kvdb_db_sup).

-behaviour(supervisor).

%% API
-export([start_link/2,
	stop_child/1]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link(Name, Backend) ->
    supervisor:start_link(?MODULE, {Name, Backend}).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init({Name, Backend}) ->
    gproc:reg({n,l,{?MODULE, Name}}),
    {ok, { {rest_for_one, 5, 10},
	   [{db, {kvdb_server, start_link, [Name, Backend]},
	     permanent, 5000, worker, [kvdb_server]},
	    {cron, {kvdb_cron, start_link, [Name]},
	     permanent, 5000, worker, [kvdb_cron]}]}}.

stop_child(Name) ->
    case gproc:where({n,l,{?MODULE,Name}}) of
	Pid when is_pid(Pid) ->
	    supervisor:terminate_child(kvdb_sup, Pid);
	_ ->
	    {error, unknown_db}
    end.
