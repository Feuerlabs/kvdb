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
%%% Experimental logging API
%%% @end
-module(kvdb_log).

-export([create_table/2, create_table/3,
	 add_log/3, add_log/4]).
-export([log/3, adjust/3, list/3]).

-include("kvdb.hrl").
-record(i, {cur = 0, max = infinity}).

create_table(Db, Table) ->
    create_table(Db, Table, {sext, term, term}).

create_table(Db, Table, Enc) ->
    kvdb:add_table(Db, Table, [{type, fifo}, {encoding,Enc}]).

add_log(Db, Table, Log) ->
    add_log(Db, Table, Log, infinity).

add_log(Db, Table, Log, Max) when Max == infinity;
				  is_integer(Max), Max > 0 ->
    case kvdb:is_queue_empty(Db, Table, Log) of
	true ->
	    kvdb:queue_insert(Db, Table, meta_key(Log),
			      inactive, meta_obj(Db, Table, Max)),
	    ok;
	false ->
	    {error, log_exists}
    end.

log(_Db, _Table, _Obj) ->
    error(nyi).

adjust(_Db, _Table, _Info) ->
    error(nyi).

list(_Db, _Table, _Filter) ->
    error(nyi).

meta_key(Log) ->
    #q_key{queue = Log, ts = 0, key = <<>>}.

meta_obj(Db, Table, Max) ->
    case kvdb:info(Db, {Table, encoding}) of
	T when tuple_size(T) == 3 ->
	    {<<>>, [], #i{max = Max}};
	_ ->
	    {<<>>, #i{max = Max}}
    end.
