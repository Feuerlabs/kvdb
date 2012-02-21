-module(kvdb_schema).

-export([validate/3, on_update/3, encode/3, decode/3]).
-export([write/2, read/1, read/2]).

-include("kvdb.hrl").

validate(_Db, _Type, Obj) ->
    Obj.

on_update(_Db, _Op, Obj) ->
    ok.

encode(_Db, _Type, Obj) ->
    Obj.

decode(_Db, _Type, Obj) ->
    Obj.

write(Db, Schema) ->
    [kvdb:put(Db, ?SCHEMA_TABLE, X) || X <- Schema],
    ok.

read(Db) ->
    match_(kvdb:select(Db, ?SCHEMA_TABLE, [{'_',[],['$_']}], 100), []).

read(Db, Item) ->
    case kvdb:get(Db, Item) of
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


