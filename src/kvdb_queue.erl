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
%%%    Useful queue inspection and management functions
%%% @end
%%%
-module(kvdb_queue).

-export([push/3, push/4,
	 pop/2, pop/3,
	 prel_pop/2, prel_pop/3,
	 delete/3, extract/3,
	 is_empty/3,
	 list_queues/2, list_queues/3,
	 list/3, list/6, list_full/3,
	 first/2,
	 next/3,
	 clear_queue/3]).

push(Db, Table, Obj)    -> push(Db, Table, Obj).
push(Db, Table, Q, Obj) -> kvdb:push(Db, Table, Q, Obj).
pop(Db, Table)          -> pop(Db, Table, <<>>).
pop(Db, Table, Q)       -> kvdb:pop(Db, Table, Q).
prel_pop(Db, Table)     -> prel_pop(Db, Table, <<>>).
prel_pop(Db, Table, Q)  -> kvdb:prel_pop(Db, Table, Q).
delete(Db, Table, Key)  -> kvdb:delete(Db, Table, Key).
extract(Db, Table, Key) -> kvdb:extract(Db, Table, Key).
is_empty(Db, Table, Q)  -> kvdb:is_queue_empty(Db, Table, Q).
list(Db, Table, Q)      -> kvdb:list_queue(Db, Table, Q).
list(Db,Tab,Q,Fltr,HeedBlock,Limit) ->
    kvdb:list_queue(Db, Tab, Q, Fltr, HeedBlock, Limit).
list_full(Db,Tab,Q) ->
    list(Db, Tab, Q, fun(S,K,O) -> {keep,{S,K,O}} end, false, infinity).
first(Db, Table)        -> kvdb:first_queue(Db, Table).
next(Db, Table, PrevQ)  -> kvdb:next_queue(Db, Table, PrevQ).

list_queues(Db, Table) ->
    list_queues(Db, Table, 30).

list_queues(Db, Table, Limit) when
      Limit == infinity; is_integer(Limit), Limit >= 0 ->
    list_queues_(first(Db, Table), Db, Table, Limit, Limit, []).

clear_queue(Db, Table, Q) ->
    clear_queue_(kvdb:list_queue(Db, Table, Q,
				 fun(_,K,_) -> {keep,K} end,
				 false, infinity), Db, Table, Q).

clear_queue_(done, _, _, _) ->
    done;
clear_queue_({Keys, Cont}, Db, Table, Q) ->
    lists:foreach(
      fun(Key) ->
	      kvdb:delete(Db, Table, Key)
      end, Keys),
    clear_queue_(Cont(), Db, Table, Q).

list_queues_({ok, Q}, Db, Table, Limit0, Limit, Acc) ->
    case decr(Limit) of
	0 ->
	    {lists:reverse([Q|Acc]),
	     fun() ->
		     Next = next(Db, Table, Q),
		     list_queues_(Next, Db, Table, Limit0, Limit0, [])
	     end};
	Limit1 ->
	    list_queues_(next(Db, Table, Q), Db, Table, Limit0, Limit1, [Q|Acc])
    end;
list_queues_(done, _Db, _Table, _Limit0, _Limit, Acc) ->
    if Acc == [] -> done;
       true -> {lists:reverse(Acc), fun() -> done end}
    end.

decr(infinity) ->
    infinity;
decr(I) when is_integer(I) ->
    I-1.

