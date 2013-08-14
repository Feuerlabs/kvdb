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
%%% @doc
%%% Database proxy supervisor.
%%%
%%% NOTE: This is work in progress, highly experimental.
%%% The general idea is that a backend can specify 'proxy processes'
%%% as a list of childspecs. An example of such a proxy is kvdb_riak_proxy.
%%%
%%% @end
-module(kvdb_proxy_sup).
-behaviour(supervisor).

-export([start_link/2]).

-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link(Name, Options) ->
    supervisor:start_link(?MODULE, {Name, Options}).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init({Name, Options}) ->
    gproc:reg({n,l,{?MODULE,Name}}),
    Backend = proplists:get_value(backend, Options, ets),
    Mod = kvdb_lib:backend_mod(Backend),
    Children = try Mod:proxy_childspecs(Name, Options)
	       catch
		   error:undef ->
		       []
	       end,
    {ok, { {one_for_one, 5, 10}, Children } }.
