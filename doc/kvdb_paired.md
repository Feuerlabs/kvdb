

# Module kvdb_paired #
* [Description](#description)
* [Function Index](#index)
* [Function Details](#functions)



Paired backend for kvdb.
__Behaviours:__ [`kvdb`](kvdb.md).

__Authors:__ Ulf Wiger ([`ulf@feuerlabs.com`](mailto:ulf@feuerlabs.com)).
<a name="description"></a>

## Description ##



NOTE: This is work in progress. Several things still do not work



The idea with this backend is to combine two backends as a write-through
pair. That is: all reads are done on 'backend 1', but writes are served
in both 'backend 1' and 'backend 2'. An example of how to use this would
be e.g. a kvdb_ets backend in front of a kvdb_riak backend:



```erlang

   kvdb:open(p, [{backend, kvdb_paired},
                 {module1, kvdb_ets},
                 {module2, kvdb_riak},
                 {options2, [{update_index, false}]}]).
```


The 'options2' list applies only to the 'backend 2' (riak in this case).
With `{update_index, false}` on 'backend 2', we will not maintain indexes
on the riak side, but rebuild them when the ets backend is populated.<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#add_table-3">add_table/3</a></td><td></td></tr><tr><td valign="top"><a href="#close-1">close/1</a></td><td></td></tr><tr><td valign="top"><a href="#delete-3">delete/3</a></td><td></td></tr><tr><td valign="top"><a href="#delete_table-2">delete_table/2</a></td><td></td></tr><tr><td valign="top"><a href="#dump_tables-1">dump_tables/1</a></td><td></td></tr><tr><td valign="top"><a href="#extract-3">extract/3</a></td><td></td></tr><tr><td valign="top"><a href="#first-2">first/2</a></td><td></td></tr><tr><td valign="top"><a href="#first_queue-2">first_queue/2</a></td><td></td></tr><tr><td valign="top"><a href="#get-3">get/3</a></td><td></td></tr><tr><td valign="top"><a href="#get_attrs-4">get_attrs/4</a></td><td></td></tr><tr><td valign="top"><a href="#get_schema_mod-2">get_schema_mod/2</a></td><td></td></tr><tr><td valign="top"><a href="#index_get-4">index_get/4</a></td><td></td></tr><tr><td valign="top"><a href="#index_keys-4">index_keys/4</a></td><td></td></tr><tr><td valign="top"><a href="#info-2">info/2</a></td><td></td></tr><tr><td valign="top"><a href="#is_queue_empty-3">is_queue_empty/3</a></td><td></td></tr><tr><td valign="top"><a href="#is_table-2">is_table/2</a></td><td></td></tr><tr><td valign="top"><a href="#last-2">last/2</a></td><td></td></tr><tr><td valign="top"><a href="#list_queue-3">list_queue/3</a></td><td></td></tr><tr><td valign="top"><a href="#list_queue-6">list_queue/6</a></td><td></td></tr><tr><td valign="top"><a href="#list_queue-7">list_queue/7</a></td><td></td></tr><tr><td valign="top"><a href="#list_tables-1">list_tables/1</a></td><td></td></tr><tr><td valign="top"><a href="#mark_queue_object-4">mark_queue_object/4</a></td><td></td></tr><tr><td valign="top"><a href="#next-3">next/3</a></td><td></td></tr><tr><td valign="top"><a href="#next_queue-3">next_queue/3</a></td><td></td></tr><tr><td valign="top"><a href="#open-2">open/2</a></td><td></td></tr><tr><td valign="top"><a href="#pop-3">pop/3</a></td><td></td></tr><tr><td valign="top"><a href="#prefix_match-3">prefix_match/3</a></td><td></td></tr><tr><td valign="top"><a href="#prefix_match-4">prefix_match/4</a></td><td></td></tr><tr><td valign="top"><a href="#prefix_match_rel-5">prefix_match_rel/5</a></td><td></td></tr><tr><td valign="top"><a href="#prel_pop-3">prel_pop/3</a></td><td></td></tr><tr><td valign="top"><a href="#prev-3">prev/3</a></td><td></td></tr><tr><td valign="top"><a href="#proxy_childspecs-2">proxy_childspecs/2</a></td><td></td></tr><tr><td valign="top"><a href="#push-4">push/4</a></td><td></td></tr><tr><td valign="top"><a href="#put-3">put/3</a></td><td></td></tr><tr><td valign="top"><a href="#queue_delete-3">queue_delete/3</a></td><td></td></tr><tr><td valign="top"><a href="#queue_head_delete-3">queue_head_delete/3</a></td><td></td></tr><tr><td valign="top"><a href="#queue_head_read-3">queue_head_read/3</a></td><td></td></tr><tr><td valign="top"><a href="#queue_head_write-4">queue_head_write/4</a></td><td></td></tr><tr><td valign="top"><a href="#queue_insert-5">queue_insert/5</a></td><td></td></tr><tr><td valign="top"><a href="#queue_read-3">queue_read/3</a></td><td></td></tr><tr><td valign="top"><a href="#schema_delete-3">schema_delete/3</a></td><td></td></tr><tr><td valign="top"><a href="#schema_fold-3">schema_fold/3</a></td><td></td></tr><tr><td valign="top"><a href="#schema_read-3">schema_read/3</a></td><td></td></tr><tr><td valign="top"><a href="#schema_write-4">schema_write/4</a></td><td></td></tr><tr><td valign="top"><a href="#update_counter-4">update_counter/4</a></td><td></td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="add_table-3"></a>

### add_table/3 ###

`add_table(Db, Table, Opts) -> any()`


<a name="close-1"></a>

### close/1 ###

`close(Db) -> any()`


<a name="delete-3"></a>

### delete/3 ###

`delete(Db, T, K) -> any()`


<a name="delete_table-2"></a>

### delete_table/2 ###

`delete_table(Db, Table) -> any()`


<a name="dump_tables-1"></a>

### dump_tables/1 ###

`dump_tables(Db) -> any()`


<a name="extract-3"></a>

### extract/3 ###

`extract(Db, T, K) -> any()`


<a name="first-2"></a>

### first/2 ###

`first(Db, Tab) -> any()`


<a name="first_queue-2"></a>

### first_queue/2 ###

`first_queue(Db, Tab) -> any()`


<a name="get-3"></a>

### get/3 ###

`get(Db, Tab, K) -> any()`


<a name="get_attrs-4"></a>

### get_attrs/4 ###

`get_attrs(Db, Tab, K, As) -> any()`


<a name="get_schema_mod-2"></a>

### get_schema_mod/2 ###

`get_schema_mod(Db, Default) -> any()`


<a name="index_get-4"></a>

### index_get/4 ###

`index_get(Db, Tab, K, V) -> any()`


<a name="index_keys-4"></a>

### index_keys/4 ###

`index_keys(Db, T, K, V) -> any()`


<a name="info-2"></a>

### info/2 ###

`info(Db, What) -> any()`


<a name="is_queue_empty-3"></a>

### is_queue_empty/3 ###

`is_queue_empty(Db, T, Q) -> any()`


<a name="is_table-2"></a>

### is_table/2 ###

`is_table(Db, Tab) -> any()`


<a name="last-2"></a>

### last/2 ###

`last(Db, Tab) -> any()`


<a name="list_queue-3"></a>

### list_queue/3 ###

`list_queue(Db, T, Q) -> any()`


<a name="list_queue-6"></a>

### list_queue/6 ###

`list_queue(Db, T, Q, Fltr, HeedBlock, Limit) -> any()`


<a name="list_queue-7"></a>

### list_queue/7 ###

`list_queue(Db, T, Q, Fltr, HeedBlock, Limit, Reverse) -> any()`


<a name="list_tables-1"></a>

### list_tables/1 ###

`list_tables(Db) -> any()`


<a name="mark_queue_object-4"></a>

### mark_queue_object/4 ###

`mark_queue_object(Db, Tab, K, St) -> any()`


<a name="next-3"></a>

### next/3 ###

`next(Db, Tab, K) -> any()`


<a name="next_queue-3"></a>

### next_queue/3 ###

`next_queue(Db, Tab, Q) -> any()`


<a name="open-2"></a>

### open/2 ###

`open(DbName, Options) -> any()`


<a name="pop-3"></a>

### pop/3 ###

`pop(Db, T, Q) -> any()`


<a name="prefix_match-3"></a>

### prefix_match/3 ###

`prefix_match(Db, Tab, Pfx) -> any()`


<a name="prefix_match-4"></a>

### prefix_match/4 ###

`prefix_match(Db, Tab, Pfx, Limit) -> any()`


<a name="prefix_match_rel-5"></a>

### prefix_match_rel/5 ###

`prefix_match_rel(Db, Tab, Prefix, Start, Limit) -> any()`


<a name="prel_pop-3"></a>

### prel_pop/3 ###

`prel_pop(Db, T, Q) -> any()`


<a name="prev-3"></a>

### prev/3 ###

`prev(Db, Tab, K) -> any()`


<a name="proxy_childspecs-2"></a>

### proxy_childspecs/2 ###

`proxy_childspecs(Name, Options) -> any()`


<a name="push-4"></a>

### push/4 ###

`push(Db, Table, Q, Obj) -> any()`


<a name="put-3"></a>

### put/3 ###

`put(Db, Table, Obj) -> any()`


<a name="queue_delete-3"></a>

### queue_delete/3 ###

`queue_delete(Db, T, K) -> any()`


<a name="queue_head_delete-3"></a>

### queue_head_delete/3 ###

`queue_head_delete(Db, Tab, Q) -> any()`


<a name="queue_head_read-3"></a>

### queue_head_read/3 ###

`queue_head_read(Db, Tab, Q) -> any()`


<a name="queue_head_write-4"></a>

### queue_head_write/4 ###

`queue_head_write(Db, Tab, Q, Obj) -> any()`


<a name="queue_insert-5"></a>

### queue_insert/5 ###

`queue_insert(Db, T, K, St, Obj) -> any()`


<a name="queue_read-3"></a>

### queue_read/3 ###

`queue_read(Db, T, K) -> any()`


<a name="schema_delete-3"></a>

### schema_delete/3 ###

`schema_delete(Db, Cat, K) -> any()`


<a name="schema_fold-3"></a>

### schema_fold/3 ###

`schema_fold(Db, F, A) -> any()`


<a name="schema_read-3"></a>

### schema_read/3 ###

`schema_read(Db, Cat, K) -> any()`


<a name="schema_write-4"></a>

### schema_write/4 ###

`schema_write(Db, Cat, K, V) -> any()`


<a name="update_counter-4"></a>

### update_counter/4 ###

`update_counter(Db, Table, Key, Incr) -> any()`


