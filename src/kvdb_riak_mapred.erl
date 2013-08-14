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
%%% Riak map-reduce hooks for kvdb_riak backend
%%%
%%% NOTE: This is work in progress, highly experimental.
%%%
%%% @end
-module(kvdb_riak_mapred).

-compile(export_all).



map_prefix_match(Pfx) ->
    {map, {modfun, ?MODULE, map_prefix_match}, Pfx, false}.

map_prefix_match(Obj, _, Pfx) ->
    Sz = byte_size(Pfx),
    Key = riak_object:key(Obj),
    case Key of
	<<Pfx:Sz/binary, _/binary>> ->
	    [binary_to_term(riak_object:get_value(Obj))];
	_ ->
	    []
    end.

