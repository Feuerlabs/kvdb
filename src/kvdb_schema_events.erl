-module(kvdb_schema_events).
-behaviour(kvdb_schema).

-export([notify_when_not_empty/3,
	 notify_all_queues/2,
	 cancel_notify_all_queues/2]).

-include("kvdb.hrl").

%% kvdb_schema callbacks
-export([validate/3,
	 validate_attr/3,
	 on_update/4]).

notify_when_not_empty(#kvdb_ref{name = DBN} = Db, Table0, Q) ->
    Table = kvdb_lib:table_name(Table0),
    Evt = {kvdb, DBN, Table, Q, queue_status},
    gproc_ps:notify_single_if_true(
      l, Evt, fun() -> not kvdb:do_is_queue_empty(Db,Table,Q) end, not_empty).

notify_all_queues(#kvdb_ref{name = DBN}, Table0) ->
    Table = kvdb_lib:table_name(Table0),
    Evt = {kvdb, DBN, Table, queue_status},
    gproc_ps:subscribe(l, Evt).

cancel_notify_all_queues(#kvdb_ref{name = DBN}, Table0) ->
    gproc_ps:unsubscribe(l, {kvdb, DBN, kvdb_lib:table_name(Table0),
			     queue_status}).

validate(_, _, Obj) ->
    Obj.

validate_attr(_, _, Attr) ->
    Attr.

on_update({q_op,_,Q,true}, DB, Table, _) ->
    notify_queue_status(DB, Table, Q, empty),
    ok;
on_update({q_op,_,Q,false}, DB, Table, _) ->
    notify_queue_status(DB, Table, Q, not_empty),
    ok;
on_update(_, _, _, _) ->
    ok.

notify_queue_status(#kvdb_ref{name = DBN, db = #db{metadata = Ets}},
		    Table, Q, Status) ->
    case set_status(Ets, Table, Q, Status) of
	changed ->
	    _ = gproc_ps:publish(l, {kvdb, DBN, Table, queue_status},
				 {Q, Status}),
	    _ = gproc_ps:tell_singles(l, {kvdb, DBN, Table, Q, queue_status},
				      Status),
	    ok;
	same ->
	    ok
    end.

set_status(Ets, Tab, Q, Status) ->
    Key = {q_status,Tab,Q},
    Change = case Status of
		 empty -> [{2,0},{2,-1,0,0}];
		 not_empty -> [{2,0},{2,1,1,1}]
	     end,
    try ets:update_counter(Ets, Key, Change) of
	[X,X] ->
	    same;
	[_,_] ->
	    changed
    catch
	error:_ ->
	    ets:insert(Ets, {Key, case Status of
				      empty     -> 0;
				      not_empty -> 1
				  end}),
	    changed
    end.
