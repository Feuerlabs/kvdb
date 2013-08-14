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
%%% Riak connectivity manager (proxy) for kvdb_riak backend
%%%
%%% NOTE: This is work in progress, highly experimental.
%%%
%%% @end
-module(kvdb_riak_proxy).
-behaviour(gen_server).

-export([start_link/2]).

-export([get_session/1]).

-export([init/1,
	 handle_call/3,
	 handle_cast/2,
	 handle_info/2,
	 terminate/2,
	 code_change/3]).

-record(st, {session}).

start_link(Name, Options) ->
    gen_server:start_link(?MODULE, {Name, Options}, []).

get_session(Name) ->
    case gproc:where({n,l,{?MODULE,Name}}) of
	undefined ->
	    undefined;
	Pid ->
	    gen_server:call(Pid, get_session)
    end.

init({Name, Options}) ->
    gproc:reg({n,l,{?MODULE, Name}}),
    Session = open_session(Options),
    {ok, #st{session = Session}}.

handle_call(get_session, _, #st{session = Session} = S) ->
    {reply, Session, S}.

handle_cast(_, S) ->
    {noreply, S}.

handle_info(_, S) ->
    {noreply, S}.

terminate(_, _) ->
    ok.

code_change(_, S, _) ->
    {ok, S}.


open_session(Options) ->
    case proplists:get_value(riak, Options, {client, node()}) of
	{client, Node} ->
	    case riak:client_connect(Node) of
		{ok, Pid} -> {client, Pid};
		_ -> undefined
	    end;
	{pb, Conf} ->
	    {Host, Port} =
		case Conf of
		    {_,_} -> Conf;
		    local ->
			case riak_api_pb_listener:get_listeners() of
			    [{_, P}|_] ->
				{"127.0.0.1", P};
			    _ -> error(cannot_get_pb_config)
			end
		end,
	    case riakc_pb_socket:start_link(Host, Port) of
		{ok, Pid} ->
		    {pb, Pid};
		_ ->
		    undefined
	    end
    end.
