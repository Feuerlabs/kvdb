-module(kvdb_queue).

-export([push/2, push/3,
	 pop/2, pop/3,
	 prel_pop/2, prel_pop/3,
	 delete/3, extract/3,
	 is_empty/3,
	 list/3, list/6,
	 first/2,
	 next/3,
	 clear_queue/3]).

push(Db, Table)         -> push(Db, Table, <<>>).
push(Db, Table, Q)      -> kvdb:push(Db, Table, Q).
pop(Db, Table)          -> pop(Db, Table, <<>>).
pop(Db, Table, Q)       -> kvdb:pop(Db, Table, Q).
prel_pop(Db, Table)     -> prel_pop(Db, Table, <<>>).
prel_pop(Db, Table, Q)  -> kvdb:prel_pop(Db, Table, Q).
delete(Db, Table, Key)  -> kvdb:delete(Db, Table, Key).
extract(Db, Table, Key) -> kvdb:extract(Db, Table, Key).
is_empty(Db, Table, Q)  -> kvdb:is_queue_empty(Db, Table, Q).
list(Db, Table, Q)      -> kvdb:list_queue(Db, Table, Q).
list(Db,Tab,Q,Fltr,Inactive,Limit) ->
    kvdb:list_queue(Db, Tab, Q, Fltr, Inactive, Limit).
first(Db, Table)        -> kvdb:first_queue(Db, Table).
next(Db, Table, PrevQ)  -> kvdb:next_queue(Db, Table, PrevQ).

clear_queue(Db, Table, Q) ->
    lists:foreach(
      fun(Key) ->
	      kvdb:delete(Db, Table, Key)
      end, kvdb:list_queue(Db, Table, Q,
			   fun(_,K,_) -> {keep,K} end, true, infinity)).

