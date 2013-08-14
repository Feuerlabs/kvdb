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
%%%   KVDB schema callback module behavior
%%% @end
-module(kvdb_schema).

-export([
	 validate/3, on_update/4
	]).
-export([write/2, read/1, read/2]).
-export([pre_commit/2, post_commit/2]).

-export([fold_schema/3, all_ok/3]).

-export([behaviour_info/1]).

-include("kvdb.hrl").

behaviour_info(callbacks) ->
    [{validate, 3},
     {on_update, 4},
     {pre_commit, 2},
     {post_commit, 2}];
behaviour_info(_) ->
    undefined.

fold_schema(Schema, F, Obj) when is_function(F, 2) ->
    if is_atom(Schema) ->
	    F(Schema, Obj);
       is_list(Schema) ->
	    lists:foldl(
	      fun(S, Acc) ->
		      fold_schema(S, F, Acc)
	      end, Schema, Obj);
       is_tuple(Schema), tuple_size(Schema) == 1 ->
	    {M} = Schema,
	    Expanded = M:expand_schema(),
	    fold_schema(Expanded, F, Obj)
    end.

all_ok(Schema, F, Obj) ->
    R = if is_atom(Schema) -> F(Schema, Obj);
	   is_list(Schema) -> catch all_ok_(Schema, F, Obj);
	   is_tuple(Schema), tuple_size(Schema) == 1 ->
		{M} = Schema,
		catch all_ok_([_|_] = M:expand_schema(), F, Obj)
	end,
    case R of
	{'EXIT', Reason} -> {error, Reason};
	Other -> Other
    end.

all_ok_([H|T], F, Obj) ->
    case F(H, Obj) of
	ok -> all_ok_(T, F, Obj);
	Other -> Other
    end;
all_ok_([], _, _) ->
    ok.


validate(_Db, _Type, Obj) ->
    Obj.

on_update(_Op, _Db, _Table, _Obj) ->
    ok.

write(Db, Schema) ->
    [kvdb:put(Db, ?META_TABLE, X) || X <- Schema],
    ok.

read(Db) ->
    match_(kvdb:select(Db, ?META_TABLE, [{'_',[],['$_']}], 100), []).

read(Db, Item) ->
    case kvdb:get(Db, ?META_TABLE, Item) of
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
