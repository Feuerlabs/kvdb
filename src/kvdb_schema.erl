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
-module(kvdb_schema).

-export([
	 validate/3, validate_attr/3, on_update/4
	]).
-export([write/2, read/1, read/2]).
-export([pre_commit/2, post_commit/2]).

-export([behaviour_info/1]).

-include("kvdb.hrl").

behaviour_info(callbacks) ->
    [{validate, 3},
     {validate_attr, 3},
     {on_update, 4}];
behaviour_info(_) ->
    undefined.


validate(_Db, _Type, Obj) ->
    Obj.

validate_attr(_Db, _Type, Attr) ->
    Attr.

on_update(_Op, _Db, _Table, _Obj) ->
    ok.

write(Db, Schema) ->
    [kvdb:put(Db, ?SCHEMA_TABLE, X) || X <- Schema],
    ok.

read(Db) ->
    match_(kvdb:select(Db, ?SCHEMA_TABLE, [{'_',[],['$_']}], 100), []).

read(Db, Item) ->
    case kvdb:get(Db, ?SCHEMA_TABLE, Item) of
	{ok, {_,_,V}} -> {ok, V};
	{ok, {_, V}}  -> {ok, V};
	Error ->
	    Error
    end.

match_({Objs, Cont}, Acc) ->
    match_(Cont(), acc_(Objs, Acc));
match_(done, Acc) ->
    lists:reverse(Acc).

acc_([{K,_,V}|T], Acc) ->
    acc_(T, [{K,V}|Acc]);
acc_([{_,_}|_] = L, Acc) ->
    lists:reverse(L) ++ Acc;
acc_([], Acc) ->
    Acc.

pre_commit(C, _) ->
    C.

post_commit(_, _) ->
    ok.
