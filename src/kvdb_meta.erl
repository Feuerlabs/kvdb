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
%%% @hidden
%%% @doc
%%% KVDB meta-data management API
%%% @end
%%%
-module(kvdb_meta).

-export([write/3,
	 write_new/3,
	 read/3,
	 delete/2,
	 update_counter/3]).

-include("kvdb.hrl").

write(#db{metadata = Ets}, Key, Value) ->
    ets:insert(Ets, {key(Key), Value}).

write_new(#db{metadata = Ets}, Key, Value) ->
    ets:insert_new(Ets, {key(Key), Value}).

read(#db{metadata = Ets}, Key, Default) ->
    case ets:lookup(Ets, key(Key)) of
	[] ->
	    Default;
	[{_, Value}] ->
	    Value
    end.

delete(#db{metadata = Ets}, Key) ->
    %% io:fwrite("delete meta (~p) ~p~n", [Ets, Key]),
    ets:delete(Ets, key(Key)).

update_counter(#db{metadata = Ets}, K, Incr) ->
    Key = key(K),
    try ets:update_counter(Ets, Key, Incr)
    catch
	error:_ ->
	    ets:insert_new(Ets, {Key, 0}),
	    ets:update_counter(Ets, Key, Incr)
    end.

key(K) ->
    {'$kvdb_meta', K}.
