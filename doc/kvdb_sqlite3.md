

#Module kvdb_sqlite3#
* [Description](#description)
* [Function Index](#index)
* [Function Details](#functions)



SQLITE3 backend to kvdb.



Copyright (c) (C) 2011, Tony Rogvall

__Behaviours:__ [`kvdb`](kvdb.md).

__Authors:__ Tony Rogvall ([`tony@rogvall.se`](mailto:tony@rogvall.se)).<a name="index"></a>

##Function Index##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#add_table-3">add_table/3</a></td><td></td></tr><tr><td valign="top"><a href="#close-1">close/1</a></td><td></td></tr><tr><td valign="top"><a href="#delete-3">delete/3</a></td><td></td></tr><tr><td valign="top"><a href="#delete_table-2">delete_table/2</a></td><td></td></tr><tr><td valign="top"><a href="#dump_tables-1">dump_tables/1</a></td><td></td></tr><tr><td valign="top"><a href="#extract-3">extract/3</a></td><td></td></tr><tr><td valign="top"><a href="#first-2">first/2</a></td><td></td></tr><tr><td valign="top"><a href="#first_queue-2">first_queue/2</a></td><td></td></tr><tr><td valign="top"><a href="#get-3">get/3</a></td><td></td></tr><tr><td valign="top"><a href="#get_attrs-4">get_attrs/4</a></td><td></td></tr><tr><td valign="top"><a href="#get_schema_mod-2">get_schema_mod/2</a></td><td></td></tr><tr><td valign="top"><a href="#index_get-4">index_get/4</a></td><td></td></tr><tr><td valign="top"><a href="#info-2">info/2</a></td><td></td></tr><tr><td valign="top"><a href="#is_queue_empty-3">is_queue_empty/3</a></td><td></td></tr><tr><td valign="top"><a href="#last-2">last/2</a></td><td></td></tr><tr><td valign="top"><a href="#list_queue-3">list_queue/3</a></td><td></td></tr><tr><td valign="top"><a href="#list_queue-6">list_queue/6</a></td><td></td></tr><tr><td valign="top"><a href="#list_tables-1">list_tables/1</a></td><td></td></tr><tr><td valign="top"><a href="#next-3">next/3</a></td><td></td></tr><tr><td valign="top"><a href="#next_queue-3">next_queue/3</a></td><td></td></tr><tr><td valign="top"><a href="#open-2">open/2</a></td><td></td></tr><tr><td valign="top"><a href="#pop-3">pop/3</a></td><td></td></tr><tr><td valign="top"><a href="#prefix_match-3">prefix_match/3</a></td><td></td></tr><tr><td valign="top"><a href="#prefix_match-4">prefix_match/4</a></td><td></td></tr><tr><td valign="top"><a href="#prefix_match-5">prefix_match/5</a></td><td></td></tr><tr><td valign="top"><a href="#prel_pop-3">prel_pop/3</a></td><td></td></tr><tr><td valign="top"><a href="#prev-3">prev/3</a></td><td></td></tr><tr><td valign="top"><a href="#push-4">push/4</a></td><td></td></tr><tr><td valign="top"><a href="#put-3">put/3</a></td><td></td></tr></table>


<a name="functions"></a>

##Function Details##

<a name="add_table-3"></a>

###add_table/3##




`add_table(Db, Table, Opts) -> any()`

<a name="close-1"></a>

###close/1##




`close(Db) -> any()`

<a name="delete-3"></a>

###delete/3##




`delete(Db, Table, Key) -> any()`

<a name="delete_table-2"></a>

###delete_table/2##




`delete_table(Db, Table) -> any()`

<a name="dump_tables-1"></a>

###dump_tables/1##




`dump_tables(Db) -> any()`

<a name="extract-3"></a>

###extract/3##




`extract(Db, Table, Key) -> any()`

<a name="first-2"></a>

###first/2##




`first(Db, Table) -> any()`

<a name="first_queue-2"></a>

###first_queue/2##




`first_queue(Db, Table) -> any()`

<a name="get-3"></a>

###get/3##




`get(Db, Table, Key) -> any()`

<a name="get_attrs-4"></a>

###get_attrs/4##




`get_attrs(Db, Table, Key, As) -> any()`

<a name="get_schema_mod-2"></a>

###get_schema_mod/2##




`get_schema_mod(X1, M) -> any()`

<a name="index_get-4"></a>

###index_get/4##




`index_get(Db, Table, IxName, IxVal) -> any()`

<a name="info-2"></a>

###info/2##




`info(Db, X2) -> any()`

<a name="is_queue_empty-3"></a>

###is_queue_empty/3##




`is_queue_empty(Db, Table, Q) -> any()`

<a name="last-2"></a>

###last/2##




`last(Db, Table) -> any()`

<a name="list_queue-3"></a>

###list_queue/3##




`list_queue(Db, Table, Q) -> any()`

<a name="list_queue-6"></a>

###list_queue/6##




`list_queue(Db, Table, Q, Fltr, HeedBlock, Limit) -> any()`

<a name="list_tables-1"></a>

###list_tables/1##




`list_tables(Db) -> any()`

<a name="next-3"></a>

###next/3##




`next(Db, Table, Key) -> any()`

<a name="next_queue-3"></a>

###next_queue/3##




`next_queue(Db, Table, Q) -> any()`

<a name="open-2"></a>

###open/2##




`open(Db0, Options) -> any()`

<a name="pop-3"></a>

###pop/3##




`pop(Db, Table, Q) -> any()`

<a name="prefix_match-3"></a>

###prefix_match/3##




`prefix_match(Db, Table, Prefix) -> any()`

<a name="prefix_match-4"></a>

###prefix_match/4##




`prefix_match(Db, Table, Prefix, Limit) -> any()`

<a name="prefix_match-5"></a>

###prefix_match/5##




`prefix_match(Db, Table, Prefix, Limit, Dir) -> any()`

<a name="prel_pop-3"></a>

###prel_pop/3##




`prel_pop(Db, Table, Q) -> any()`

<a name="prev-3"></a>

###prev/3##




`prev(Db, Table, Key) -> any()`

<a name="push-4"></a>

###push/4##




`push(Db, Table, Q, X4) -> any()`

<a name="put-3"></a>

###put/3##




`put(Db, Table, Obj) -> any()`

