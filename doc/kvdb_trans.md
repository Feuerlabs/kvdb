

#Module kvdb_trans#
* [Function Index](#index)
* [Function Details](#functions)


__Behaviours:__ [`kvdb`](kvdb.md).

__Authors:__ Ulf Wiger ([`ulf@feuerlabs.com`](mailto:ulf@feuerlabs.com)).<a name="index"></a>

##Function Index##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#add_table-3">add_table/3</a></td><td></td></tr><tr><td valign="top"><a href="#close-1">close/1</a></td><td></td></tr><tr><td valign="top"><a href="#delete-3">delete/3</a></td><td></td></tr><tr><td valign="top"><a href="#delete_table-2">delete_table/2</a></td><td></td></tr><tr><td valign="top"><a href="#dump_tables-1">dump_tables/1</a></td><td></td></tr><tr><td valign="top"><a href="#extract-3">extract/3</a></td><td></td></tr><tr><td valign="top"><a href="#first-2">first/2</a></td><td></td></tr><tr><td valign="top"><a href="#first_queue-2">first_queue/2</a></td><td></td></tr><tr><td valign="top"><a href="#get-3">get/3</a></td><td></td></tr><tr><td valign="top"><a href="#get_attrs-4">get_attrs/4</a></td><td></td></tr><tr><td valign="top"><a href="#get_schema_mod-2">get_schema_mod/2</a></td><td></td></tr><tr><td valign="top"><a href="#index_get-4">index_get/4</a></td><td></td></tr><tr><td valign="top"><a href="#index_keys-4">index_keys/4</a></td><td></td></tr><tr><td valign="top"><a href="#info-2">info/2</a></td><td></td></tr><tr><td valign="top"><a href="#is_queue_empty-3">is_queue_empty/3</a></td><td></td></tr><tr><td valign="top"><a href="#is_table-2">is_table/2</a></td><td></td></tr><tr><td valign="top"><a href="#is_transaction-1">is_transaction/1</a></td><td></td></tr><tr><td valign="top"><a href="#last-2">last/2</a></td><td></td></tr><tr><td valign="top"><a href="#list_queue-3">list_queue/3</a></td><td></td></tr><tr><td valign="top"><a href="#list_queue-6">list_queue/6</a></td><td></td></tr><tr><td valign="top"><a href="#mark_queue_object-4">mark_queue_object/4</a></td><td></td></tr><tr><td valign="top"><a href="#next-3">next/3</a></td><td></td></tr><tr><td valign="top"><a href="#next_queue-3">next_queue/3</a></td><td></td></tr><tr><td valign="top"><a href="#on_update-4">on_update/4</a></td><td></td></tr><tr><td valign="top"><a href="#open-2">open/2</a></td><td></td></tr><tr><td valign="top"><a href="#pop-3">pop/3</a></td><td></td></tr><tr><td valign="top"><a href="#prefix_match-4">prefix_match/4</a></td><td></td></tr><tr><td valign="top"><a href="#prefix_match_rel-5">prefix_match_rel/5</a></td><td></td></tr><tr><td valign="top"><a href="#prel_pop-3">prel_pop/3</a></td><td></td></tr><tr><td valign="top"><a href="#prev-3">prev/3</a></td><td></td></tr><tr><td valign="top"><a href="#push-4">push/4</a></td><td></td></tr><tr><td valign="top"><a href="#put-3">put/3</a></td><td></td></tr><tr><td valign="top"><a href="#queue_delete-3">queue_delete/3</a></td><td></td></tr><tr><td valign="top"><a href="#queue_insert-5">queue_insert/5</a></td><td></td></tr><tr><td valign="top"><a href="#queue_read-3">queue_read/3</a></td><td></td></tr><tr><td valign="top"><a href="#require-2">require/2</a></td><td>Ensures a transaction context, either reusing one, or creating one.</td></tr><tr><td valign="top"><a href="#run-2">run/2</a></td><td></td></tr><tr><td valign="top"><a href="#tstore_to_list-1">tstore_to_list/1</a></td><td></td></tr><tr><td valign="top"><a href="#update_counter-4">update_counter/4</a></td><td></td></tr></table>


<a name="functions"></a>

##Function Details##

<a name="add_table-3"></a>

###add_table/3##


`add_table(Db, Tab, Opts) -> any()`

<a name="close-1"></a>

###close/1##


`close(Db) -> any()`

<a name="delete-3"></a>

###delete/3##


`delete(Db, Tab, Key) -> any()`

<a name="delete_table-2"></a>

###delete_table/2##


`delete_table(Db, Tab) -> any()`

<a name="dump_tables-1"></a>

###dump_tables/1##


`dump_tables(Db) -> any()`

<a name="extract-3"></a>

###extract/3##


`extract(Db, Tab, QKey) -> any()`

<a name="first-2"></a>

###first/2##


`first(Db, Tab) -> any()`

<a name="first_queue-2"></a>

###first_queue/2##


`first_queue(Db, Tab) -> any()`

<a name="get-3"></a>

###get/3##


`get(Db, Tab, Key) -> any()`

<a name="get_attrs-4"></a>

###get_attrs/4##


`get_attrs(Db, Tab, K, Attrs) -> any()`

<a name="get_schema_mod-2"></a>

###get_schema_mod/2##


`get_schema_mod(Db, Tab) -> any()`

<a name="index_get-4"></a>

###index_get/4##


`index_get(Db, Tab, Ix, IxK) -> any()`

<a name="index_keys-4"></a>

###index_keys/4##


`index_keys(Db, Tab, Ix, IxK) -> any()`

<a name="info-2"></a>

###info/2##


`info(Db, Item) -> any()`

<a name="is_queue_empty-3"></a>

###is_queue_empty/3##


`is_queue_empty(Db, Tab, Q) -> any()`

<a name="is_table-2"></a>

###is_table/2##


`is_table(Db, Tab) -> any()`

<a name="is_transaction-1"></a>

###is_transaction/1##


<pre>is_transaction(Kvdb_ref::<a href="kvdb.md#type-db_ref">kvdb:db_ref()</a>) -> {true, <a href="kvdb.md#type-db_ref">kvdb:db_ref()</a>} | false</pre>
<br></br>


<a name="last-2"></a>

###last/2##


`last(Db, Tab) -> any()`

<a name="list_queue-3"></a>

###list_queue/3##


`list_queue(Db, Tab, Q) -> any()`

<a name="list_queue-6"></a>

###list_queue/6##


`list_queue(Db, Tab, Q, Filter, HeedBlock, Limit) -> any()`

<a name="mark_queue_object-4"></a>

###mark_queue_object/4##


`mark_queue_object(Db, Tab, Q_key, St) -> any()`

<a name="next-3"></a>

###next/3##


`next(Db, Tab, K) -> any()`

<a name="next_queue-3"></a>

###next_queue/3##


`next_queue(Db, Tab, Q) -> any()`

<a name="on_update-4"></a>

###on_update/4##


`on_update(Event, Kvdb_ref, Tab, Info) -> any()`

<a name="open-2"></a>

###open/2##


`open(Db, Opts) -> any()`

<a name="pop-3"></a>

###pop/3##


`pop(Db, Tab, Q) -> any()`

<a name="prefix_match-4"></a>

###prefix_match/4##


`prefix_match(Db, Tab, Prefix, Limit) -> any()`

<a name="prefix_match_rel-5"></a>

###prefix_match_rel/5##


`prefix_match_rel(Db, Tab, Prefix, Start, Limit) -> any()`

<a name="prel_pop-3"></a>

###prel_pop/3##


`prel_pop(Db, Tab, Q) -> any()`

<a name="prev-3"></a>

###prev/3##


`prev(Db, Tab, K) -> any()`

<a name="push-4"></a>

###push/4##


`push(Db, Tab, Q, Obj) -> any()`

<a name="put-3"></a>

###put/3##


`put(Db, Tab, Obj) -> any()`

<a name="queue_delete-3"></a>

###queue_delete/3##


`queue_delete(Db, Tab, QKey) -> any()`

<a name="queue_insert-5"></a>

###queue_insert/5##


`queue_insert(Db, Tab, Q_key, St, Obj) -> any()`

<a name="queue_read-3"></a>

###queue_read/3##


`queue_read(Db, Tab, Q_key) -> any()`

<a name="require-2"></a>

###require/2##


<pre>require(Kvdb_ref::#kvdb_ref{}, F::fun((#kvdb_ref{}) -&gt; T)) -&gt; T</pre>
<br></br>




Ensures a transaction context, either reusing one, or creating one.

This function allows for a function `F` to run inside a transaction context.
If no such context exists, a new transaction is started, just as if
[`run/2`](#run-2) had been called from the beginning. If there is an existing
context, `F` is run inside that context. Note that the transaction is not
committed when `F` returns - this is the responsibility of the topmost
function. Also, if the provided reference has a valid `tref` (i.e. it is a
transaction context, that context is reused explicitly. This could be used
to participate in a transaction started by another process.<a name="run-2"></a>

###run/2##


`run(Kvdb_ref, F) -> any()`

<a name="tstore_to_list-1"></a>

###tstore_to_list/1##


`tstore_to_list(Kvdb_ref) -> any()`

<a name="update_counter-4"></a>

###update_counter/4##


`update_counter(Db, Tab, K, Incr) -> any()`

