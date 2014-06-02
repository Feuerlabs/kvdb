

# Module kvdb_riak #
* [Description](#description)
* [Function Index](#index)
* [Function Details](#functions)



Highly experimental riak backend for kvdb.
__Behaviours:__ [`kvdb`](kvdb.md).

__Authors:__ Ulf Wiger ([`ulf@feuerlabs.com`](mailto:ulf@feuerlabs.com)).
<a name="description"></a>

## Description ##



NOTE: This is work in progress. Several things still do not work


The best way to use this for now is via the kvdb_paired backend.<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#add_table-3">add_table/3</a></td><td></td></tr><tr><td valign="top"><a href="#close-1">close/1</a></td><td></td></tr><tr><td valign="top"><a href="#delete-3">delete/3</a></td><td></td></tr><tr><td valign="top"><a href="#delete_table-2">delete_table/2</a></td><td></td></tr><tr><td valign="top"><a href="#dump_tables-1">dump_tables/1</a></td><td></td></tr><tr><td valign="top"><a href="#extract-3">extract/3</a></td><td></td></tr><tr><td valign="top"><a href="#first-2">first/2</a></td><td></td></tr><tr><td valign="top"><a href="#first_queue-2">first_queue/2</a></td><td></td></tr><tr><td valign="top"><a href="#get-3">get/3</a></td><td></td></tr><tr><td valign="top"><a href="#get_attrs-4">get_attrs/4</a></td><td></td></tr><tr><td valign="top"><a href="#get_schema_mod-2">get_schema_mod/2</a></td><td></td></tr><tr><td valign="top"><a href="#index_get-4">index_get/4</a></td><td></td></tr><tr><td valign="top"><a href="#index_keys-4">index_keys/4</a></td><td></td></tr><tr><td valign="top"><a href="#info-2">info/2</a></td><td></td></tr><tr><td valign="top"><a href="#is_queue_empty-3">is_queue_empty/3</a></td><td></td></tr><tr><td valign="top"><a href="#is_table-2">is_table/2</a></td><td></td></tr><tr><td valign="top"><a href="#last-2">last/2</a></td><td></td></tr><tr><td valign="top"><a href="#list_queue-3">list_queue/3</a></td><td></td></tr><tr><td valign="top"><a href="#list_queue-6">list_queue/6</a></td><td></td></tr><tr><td valign="top"><a href="#list_tables-1">list_tables/1</a></td><td></td></tr><tr><td valign="top"><a href="#mark_queue_object-4">mark_queue_object/4</a></td><td></td></tr><tr><td valign="top"><a href="#next-3">next/3</a></td><td></td></tr><tr><td valign="top"><a href="#next_queue-3">next_queue/3</a></td><td></td></tr><tr><td valign="top"><a href="#open-2">open/2</a></td><td></td></tr><tr><td valign="top"><a href="#pop-3">pop/3</a></td><td></td></tr><tr><td valign="top"><a href="#prefix_match-3">prefix_match/3</a></td><td></td></tr><tr><td valign="top"><a href="#prefix_match-4">prefix_match/4</a></td><td></td></tr><tr><td valign="top"><a href="#prefix_match_rel-5">prefix_match_rel/5</a></td><td></td></tr><tr><td valign="top"><a href="#prel_pop-3">prel_pop/3</a></td><td></td></tr><tr><td valign="top"><a href="#prev-3">prev/3</a></td><td></td></tr><tr><td valign="top"><a href="#proxy_childspecs-2">proxy_childspecs/2</a></td><td></td></tr><tr><td valign="top"><a href="#push-4">push/4</a></td><td></td></tr><tr><td valign="top"><a href="#put-3">put/3</a></td><td></td></tr><tr><td valign="top"><a href="#queue_delete-3">queue_delete/3</a></td><td></td></tr><tr><td valign="top"><a href="#queue_head_delete-3">queue_head_delete/3</a></td><td></td></tr><tr><td valign="top"><a href="#queue_head_read-3">queue_head_read/3</a></td><td></td></tr><tr><td valign="top"><a href="#queue_head_write-4">queue_head_write/4</a></td><td></td></tr><tr><td valign="top"><a href="#queue_insert-5">queue_insert/5</a></td><td></td></tr><tr><td valign="top"><a href="#queue_read-3">queue_read/3</a></td><td></td></tr><tr><td valign="top"><a href="#update_counter-4">update_counter/4</a></td><td></td></tr></table>


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

`delete(Db, Tab, K) -> any()`


<a name="delete_table-2"></a>

### delete_table/2 ###

`delete_table(Db, Table) -> any()`


<a name="dump_tables-1"></a>

### dump_tables/1 ###

`dump_tables(X1) -> any()`


<a name="extract-3"></a>

### extract/3 ###

`extract(X1, X2, X3) -> any()`


<a name="first-2"></a>

### first/2 ###

`first(X1, X2) -> any()`


<a name="first_queue-2"></a>

### first_queue/2 ###

`first_queue(X1, X2) -> any()`


<a name="get-3"></a>

### get/3 ###

`get(Db, Tab, K) -> any()`


<a name="get_attrs-4"></a>

### get_attrs/4 ###

`get_attrs(X1, X2, X3, X4) -> any()`


<a name="get_schema_mod-2"></a>

### get_schema_mod/2 ###

`get_schema_mod(Db, Default) -> any()`


<a name="index_get-4"></a>

### index_get/4 ###

`index_get(X1, X2, X3, X4) -> any()`


<a name="index_keys-4"></a>

### index_keys/4 ###

`index_keys(X1, X2, X3, X4) -> any()`


<a name="info-2"></a>

### info/2 ###

`info(Db, What) -> any()`


<a name="is_queue_empty-3"></a>

### is_queue_empty/3 ###

`is_queue_empty(X1, X2, X3) -> any()`


<a name="is_table-2"></a>

### is_table/2 ###

`is_table(Db, Tab) -> any()`


<a name="last-2"></a>

### last/2 ###

`last(X1, X2) -> any()`


<a name="list_queue-3"></a>

### list_queue/3 ###

`list_queue(X1, X2, X3) -> any()`


<a name="list_queue-6"></a>

### list_queue/6 ###

`list_queue(X1, X2, X3, X4, X5, X6) -> any()`


<a name="list_tables-1"></a>

### list_tables/1 ###

`list_tables(Db) -> any()`


<a name="mark_queue_object-4"></a>

### mark_queue_object/4 ###

`mark_queue_object(X1, X2, X3, X4) -> any()`


<a name="next-3"></a>

### next/3 ###

`next(X1, X2, X3) -> any()`


<a name="next_queue-3"></a>

### next_queue/3 ###

`next_queue(X1, X2, X3) -> any()`


<a name="open-2"></a>

### open/2 ###

`open(DbName, Options) -> any()`


<a name="pop-3"></a>

### pop/3 ###

`pop(X1, X2, X3) -> any()`


<a name="prefix_match-3"></a>

### prefix_match/3 ###

`prefix_match(Db, Table, Pfx) -> any()`


<a name="prefix_match-4"></a>

### prefix_match/4 ###

`prefix_match(Db, Table, Pfx, Limit) -> any()`


<a name="prefix_match_rel-5"></a>

### prefix_match_rel/5 ###

`prefix_match_rel(X1, X2, X3, X4, X5) -> any()`


<a name="prel_pop-3"></a>

### prel_pop/3 ###

`prel_pop(X1, X2, X3) -> any()`


<a name="prev-3"></a>

### prev/3 ###

`prev(X1, X2, X3) -> any()`


<a name="proxy_childspecs-2"></a>

### proxy_childspecs/2 ###

`proxy_childspecs(Name, Options) -> any()`


<a name="push-4"></a>

### push/4 ###

`push(X1, X2, X3, X4) -> any()`


<a name="put-3"></a>

### put/3 ###

`put(Db, Table, Obj) -> any()`


<a name="queue_delete-3"></a>

### queue_delete/3 ###

`queue_delete(X1, X2, X3) -> any()`


<a name="queue_head_delete-3"></a>

### queue_head_delete/3 ###

`queue_head_delete(X1, X2, X3) -> any()`


<a name="queue_head_read-3"></a>

### queue_head_read/3 ###

`queue_head_read(X1, X2, X3) -> any()`


<a name="queue_head_write-4"></a>

### queue_head_write/4 ###

`queue_head_write(X1, X2, X3, X4) -> any()`


<a name="queue_insert-5"></a>

### queue_insert/5 ###

`queue_insert(X1, X2, X3, X4, X5) -> any()`


<a name="queue_read-3"></a>

### queue_read/3 ###

`queue_read(X1, X2, X3) -> any()`


<a name="update_counter-4"></a>

### update_counter/4 ###

`update_counter(Db, Table, Key, Incr) -> any()`


