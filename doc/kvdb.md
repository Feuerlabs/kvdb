

#Module kvdb#
* [Description](#description)
* [Function Index](#index)
* [Function Details](#functions)


  
Key-value database frontend.



Copyright (c) 2011-2012, Feuerlabs Inc

__Behaviours:__ [`gen_server`](gen_server.md).

__Authors:__ Ulf Wiger ([`ulf@feuerlabs.com`](mailto:ulf@feuerlabs.com)), Tony Rogvall ([`tony@rogvall.se`](mailto:tony@rogvall.se)).<a name="description"></a>

##Description##




Kvdb is a key-value database library, supporting different backends  
(currently: sqlite3 and leveldb), and a number of different table types.



Feature overview:



- Multiple logical tables per database



- Persistent ordered-set semantics



- `{Key, Value}` or `{Key, Attributes, Value}` structure (per-table)



- Table types: set (normal) or queue (FIFO, LIFO or keyed FIFO or LIFO)



- Attributes can be indexed



- Schema-based validation (per-database) with update triggers



- Prefix matching



- ETS-style select() operations

- Configurable encoding schemes (raw, sext or term_to_binary)
<a name="index"></a>

##Function Index##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#add_table-2">add_table/2</a></td><td>Equivalent to <a href="#add_table-3"><tt>add_table(Name, Table, [{type, set}])</tt></a>.</td></tr><tr><td valign="top"><a href="#add_table-3">add_table/3</a></td><td>Add a table to the database.</td></tr><tr><td valign="top"><a href="#close-1">close/1</a></td><td></td></tr><tr><td valign="top"><a href="#db-1">db/1</a></td><td>Returns a low-level handle for accessing the data via do_* functions.</td></tr><tr><td valign="top"><a href="#delete-3">delete/3</a></td><td></td></tr><tr><td valign="top"><a href="#delete_table-2">delete_table/2</a></td><td>Delete <code>Table</code> from the database.</td></tr><tr><td valign="top"><a href="#do_add_table-3">do_add_table/3</a></td><td>Low-level equivalent to <a href="#add_table-3"><code>add_table/3</code></a></td></tr><tr><td valign="top"><a href="#do_delete-3">do_delete/3</a></td><td></td></tr><tr><td valign="top"><a href="#do_delete_table-2">do_delete_table/2</a></td><td>low-level equivalent to <a href="#delete_table-2"><code>delete_table/2</code></a></td></tr><tr><td valign="top"><a href="#do_dump_tables-1">do_dump_tables/1</a></td><td>Low-level equivalent to <a href="#dump_tables-1"><code>dump_tables/1</code></a></td></tr><tr><td valign="top"><a href="#do_extract-3">do_extract/3</a></td><td></td></tr><tr><td valign="top"><a href="#do_first-2">do_first/2</a></td><td></td></tr><tr><td valign="top"><a href="#do_first_queue-2">do_first_queue/2</a></td><td></td></tr><tr><td valign="top"><a href="#do_get-3">do_get/3</a></td><td>Low-level equivalent of <a href="#get-3"><code>get/3</code></a></td></tr><tr><td valign="top"><a href="#do_get_attr-4">do_get_attr/4</a></td><td></td></tr><tr><td valign="top"><a href="#do_get_attrs-3">do_get_attrs/3</a></td><td></td></tr><tr><td valign="top"><a href="#do_index_get-4">do_index_get/4</a></td><td>Low-level equivalent of <a href="#index_get-4"><code>index_get/4</code></a></td></tr><tr><td valign="top"><a href="#do_info-2">do_info/2</a></td><td></td></tr><tr><td valign="top"><a href="#do_is_queue_empty-3">do_is_queue_empty/3</a></td><td></td></tr><tr><td valign="top"><a href="#do_last-2">do_last/2</a></td><td></td></tr><tr><td valign="top"><a href="#do_list_queue-3">do_list_queue/3</a></td><td></td></tr><tr><td valign="top"><a href="#do_list_queue-6">do_list_queue/6</a></td><td></td></tr><tr><td valign="top"><a href="#do_next-3">do_next/3</a></td><td></td></tr><tr><td valign="top"><a href="#do_next_queue-3">do_next_queue/3</a></td><td></td></tr><tr><td valign="top"><a href="#do_pop-2">do_pop/2</a></td><td>Equivalent to <a href="#do_pop-3"><tt>do_pop(Db, Table, &lt;&lt;&gt;&gt;)</tt></a>.</td></tr><tr><td valign="top"><a href="#do_pop-3">do_pop/3</a></td><td>Low-level equivalent of <a href="#pop-3"><code>pop/3</code></a></td></tr><tr><td valign="top"><a href="#do_prefix_match-4">do_prefix_match/4</a></td><td></td></tr><tr><td valign="top"><a href="#do_prel_pop-2">do_prel_pop/2</a></td><td></td></tr><tr><td valign="top"><a href="#do_prel_pop-3">do_prel_pop/3</a></td><td></td></tr><tr><td valign="top"><a href="#do_prev-3">do_prev/3</a></td><td></td></tr><tr><td valign="top"><a href="#do_push-3">do_push/3</a></td><td>Equivalent to <a href="#do_push-4"><tt>do_push(Db, Table, &lt;&lt;&gt;&gt;, Obj)</tt></a>.</td></tr><tr><td valign="top"><a href="#do_push-4">do_push/4</a></td><td>Low-level equivalent of <a href="#push-4"><code>push/4</code></a></td></tr><tr><td valign="top"><a href="#do_put-3">do_put/3</a></td><td>Low-level equivalent to <a href="#put-3"><code>put/3</code></a></td></tr><tr><td valign="top"><a href="#do_put_attr-5">do_put_attr/5</a></td><td></td></tr><tr><td valign="top"><a href="#do_put_attrs-4">do_put_attrs/4</a></td><td></td></tr><tr><td valign="top"><a href="#do_select-4">do_select/4</a></td><td></td></tr><tr><td valign="top"><a href="#dump_tables-1">dump_tables/1</a></td><td>Returns the contents of the database as a list of objects.</td></tr><tr><td valign="top"><a href="#extract-3">extract/3</a></td><td></td></tr><tr><td valign="top"><a href="#first-2">first/2</a></td><td></td></tr><tr><td valign="top"><a href="#first_queue-2">first_queue/2</a></td><td></td></tr><tr><td valign="top"><a href="#get-3">get/3</a></td><td>Perform a lookup on <code>Key</code> in <code>Table</code></td></tr><tr><td valign="top"><a href="#get_attr-4">get_attr/4</a></td><td></td></tr><tr><td valign="top"><a href="#get_attrs-3">get_attrs/3</a></td><td></td></tr><tr><td valign="top"><a href="#index_get-4">index_get/4</a></td><td>Perform an index lookup on the named index of Table.</td></tr><tr><td valign="top"><a href="#info-2">info/2</a></td><td></td></tr><tr><td valign="top"><a href="#is_queue_empty-3">is_queue_empty/3</a></td><td></td></tr><tr><td valign="top"><a href="#last-2">last/2</a></td><td></td></tr><tr><td valign="top"><a href="#list_queue-3">list_queue/3</a></td><td></td></tr><tr><td valign="top"><a href="#list_queue-6">list_queue/6</a></td><td></td></tr><tr><td valign="top"><a href="#list_tables-1">list_tables/1</a></td><td>Lists the tables defined in the database.</td></tr><tr><td valign="top"><a href="#next-3">next/3</a></td><td></td></tr><tr><td valign="top"><a href="#next_queue-3">next_queue/3</a></td><td></td></tr><tr><td valign="top"><a href="#open-2">open/2</a></td><td>Opens a database.</td></tr><tr><td valign="top"><a href="#open_db-2">open_db/2</a></td><td>Opens a kvdb database instance.</td></tr><tr><td valign="top"><a href="#pop-2">pop/2</a></td><td>Equivalent to <a href="#pop-3"><tt>pop(Name, Table, &lt;&lt;&gt;&gt;)</tt></a>.</td></tr><tr><td valign="top"><a href="#pop-3">pop/3</a></td><td>Fetches and deletes the 'first' object in the given queue.</td></tr><tr><td valign="top"><a href="#prefix_match-3">prefix_match/3</a></td><td></td></tr><tr><td valign="top"><a href="#prefix_match-4">prefix_match/4</a></td><td></td></tr><tr><td valign="top"><a href="#prel_pop-2">prel_pop/2</a></td><td></td></tr><tr><td valign="top"><a href="#prel_pop-3">prel_pop/3</a></td><td></td></tr><tr><td valign="top"><a href="#prev-3">prev/3</a></td><td></td></tr><tr><td valign="top"><a href="#push-3">push/3</a></td><td>Equivalent to <a href="#push-4"><tt>push(Name, Table, &lt;&lt;&gt;&gt;, Obj)</tt></a>.</td></tr><tr><td valign="top"><a href="#push-4">push/4</a></td><td>Push an object onto a persistent queue.</td></tr><tr><td valign="top"><a href="#put-3">put/3</a></td><td>Inserts an object into Table.</td></tr><tr><td valign="top"><a href="#put_attr-5">put_attr/5</a></td><td></td></tr><tr><td valign="top"><a href="#put_attrs-4">put_attrs/4</a></td><td></td></tr><tr><td valign="top"><a href="#select-3">select/3</a></td><td>Similar to ets:select/3.</td></tr><tr><td valign="top"><a href="#select-4">select/4</a></td><td></td></tr><tr><td valign="top"><a href="#start_link-2">start_link/2</a></td><td></td></tr><tr><td valign="top"><a href="#start_session-2">start_session/2</a></td><td></td></tr></table>


<a name="functions"></a>

##Function Details##

<a name="add_table-2"></a>

###add_table/2##




<pre>add_table(Name::<a href="#type-db_name">db_name()</a>, Table::<a href="#type-table">table()</a>) -> ok</pre>
<br></br>




Equivalent to [`add_table(Name, Table, [{type, set}])`](#add_table-3).<a name="add_table-3"></a>

###add_table/3##




<pre>add_table(Name::<a href="#type-db_name">db_name()</a>, Table::<a href="#type-table">table()</a>, Opts::<a href="#type-options">options()</a>) -> ok</pre>
<br></br>






Add a table to the database



This function assumes that the table doesn't already exist in the database.  
Valid options are:



- `{type, set | fifo | lifo | {keyed, fifo | lifo}`
<br></br>

This defines the type of the table. `set` signifies an ordered-set table.
`fifo` and `lifo` are queue table types, accessed using the functions
[`push/3`](#push-3), [`pop/2`](#pop-2), [`prel_pop/2`](#prel_pop-2), [`extract/3`](#extract-3),
[`delete/3`](#delete-3).



`{keyed, fifo | lifo}` are also a form of queued table type, where items  
are sorted by object key first, and then in FIFO or LIFO insertion order.  
This can be used for e.g. priority- or timer queues.



- `{encoding, encoding()}`
<br></br>

`encoding() :: enc() | {enc(), enc()} | {enc(), enc(), enc()}`
<br></br>

`enc() :: raw | sext | term`
<br></br>
  
Specifies how the object, or parts of the object, should be encoded.


* `raw` assumes that the data is of type binary; no extra encoding is
performed.

* `term` uses `term_to_binary/1` encoding. This is generally not useful
for the key component, as sort order is not preserved, but is a good
generic choice for the value component.

* `sext` uses sext-encoding
([github.com/uwiger/sext](http://github.com/uwiger/sext)), which
preserves the inherent sort order of erlang terms. Note that `sext`-encoding
is a bit more costly than term-encoding, both in time and space.




When the short forms, `sext`, `raw` or `term` are used, they imply a
`{Key, Value}` structure. For a `{Key, Attrs, Value}` structure, use the
3-tuple form, e.g. `{sext, sext, term}`. (The leveldb backend ignores the
encoding instruction for attrs, and encodes each attribute key with `sext`
encoding and each attribute value with `term` encoding).

- `{index, [index_expr()]}`
<br></br>

`index_expr() :: atom() | {_Name::any(), each|words, _Attr::atom()}`
<br></br>

Attributes can be indexed, by naming the attribute names to include.
If only the attribute name is given, the attribute value is used as the index
value. If a tuple {IxName, Op, Attr} is given, the attribute value is
processed to yield a list of index values. Supported operations are:
<br></br>


* `each` - the attribute value is a list; each list item becomes and index
value.

* `words` - the attribute value is a string (list) or binary; each word
in the text becomes an index value.

<a name="close-1"></a>

###close/1##




`close(Kvdb_ref) -> any()`

<a name="db-1"></a>

###db/1##




<pre>db(Kvdb_ref::<a href="#type-db_name">db_name()</a> | <a href="#type-db_ref">db_ref()</a>) -> <a href="#type-db_ref">db_ref()</a></pre>
<br></br>






Returns a low-level handle for accessing the data via do_* functions.

Note that not all functions are safe to use concurrently from different
processes. When accessing a database via Name, update functions are
serialized so that database corruption won't occur.<a name="delete-3"></a>

###delete/3##




`delete(Name, Table, Key) -> any()`

<a name="delete_table-2"></a>

###delete_table/2##




`delete_table(Name, Table) -> any()`



Delete `Table` from the database<a name="do_add_table-3"></a>

###do_add_table/3##




<pre>do_add_table(Db::<a href="#type-db_ref">db_ref()</a>, Table::<a href="#type-table">table()</a>, Opts::list()) -> ok | {error, any()}</pre>
<br></br>




Low-level equivalent to [`add_table/3`](#add_table-3)<a name="do_delete-3"></a>

###do_delete/3##




<pre>do_delete(Db::<a href="#type-db_ref">db_ref()</a>, Table::<a href="#type-table">table()</a>, Key::binary()) -> ok | {error, any()}</pre>
<br></br>


<a name="do_delete_table-2"></a>

###do_delete_table/2##




<pre>do_delete_table(Db::<a href="#type-db_ref">db_ref()</a>, Table::<a href="#type-table">table()</a>) -> ok | {error, any()}</pre>
<br></br>




low-level equivalent to [`delete_table/2`](#delete_table-2)<a name="do_dump_tables-1"></a>

###do_dump_tables/1##




<pre>do_dump_tables(Kvdb_ref::<a href="#type-db_ref">db_ref()</a>) -> list()</pre>
<br></br>




Low-level equivalent to [`dump_tables/1`](#dump_tables-1)<a name="do_extract-3"></a>

###do_extract/3##




<pre>do_extract(Kvdb_ref::#kvdb_ref{}, Table::<a href="#type-table">table()</a>, Key::binary()) -> {ok, <a href="#type-object">object()</a>} | {error, any()}</pre>
<br></br>


<a name="do_first-2"></a>

###do_first/2##




<pre>do_first(Db::<a href="#type-db_ref">db_ref()</a>, Table::<a href="#type-table">table()</a>) -> {ok, Key::binary()} | {ok, Key::binary(), Value::binary()} | done | {error, any()}</pre>
<br></br>


<a name="do_first_queue-2"></a>

###do_first_queue/2##




<pre>do_first_queue(Kvdb_ref::#kvdb_ref{}, Table0::<a href="#type-table">table()</a>) -> {ok, <a href="#type-queue_name">queue_name()</a>} | done</pre>
<br></br>


<a name="do_get-3"></a>

###do_get/3##




<pre>do_get(Db::<a href="#type-db_ref">db_ref()</a>, Table::<a href="#type-table">table()</a>, Key::binary()) -> {ok, binary()} | {error, any()}</pre>
<br></br>




Low-level equivalent of [`get/3`](#get-3)<a name="do_get_attr-4"></a>

###do_get_attr/4##




`do_get_attr(Kvdb_ref, Table0, Key, Attr) -> any()`

<a name="do_get_attrs-3"></a>

###do_get_attrs/3##




`do_get_attrs(Kvdb_ref, Table0, Key) -> any()`

<a name="do_index_get-4"></a>

###do_index_get/4##




<pre>do_index_get(Kvdb_ref::<a href="#type-db_ref">db_ref()</a>, Table0::<a href="#type-table">table()</a>, _IxName::any(), _IxVal::any()) -> [<a href="#type-object">object()</a>] | {error, any()}</pre>
<br></br>




Low-level equivalent of [`index_get/4`](#index_get-4)<a name="do_info-2"></a>

###do_info/2##




<pre>do_info(Kvdb_ref::<a href="#type-db_ref">db_ref()</a>, Item::<a href="#type-attr_name">attr_name()</a>) -> undefined | <a href="#type-attr_value">attr_value()</a></pre>
<br></br>


<a name="do_is_queue_empty-3"></a>

###do_is_queue_empty/3##




<pre>do_is_queue_empty(Kvdb_ref::#kvdb_ref{}, Table0::<a href="#type-table">table()</a>, _Q::<a href="#type-queue_name">queue_name()</a>) -> boolean()</pre>
<br></br>


<a name="do_last-2"></a>

###do_last/2##




<pre>do_last(Db::<a href="#type-db_ref">db_ref()</a>, Table::<a href="#type-table">table()</a>) -> {ok, Key::binary()} | {ok, Key::binary(), Value::binary()} | done | {error, any()}</pre>
<br></br>


<a name="do_list_queue-3"></a>

###do_list_queue/3##




<pre>do_list_queue(Kvdb_ref::#kvdb_ref{}, Table::<a href="#type-table">table()</a>, Q::<a href="#type-queue_name">queue_name()</a>) -> [<a href="#type-object">object()</a>] | {error, any()}</pre>
<br></br>


<a name="do_list_queue-6"></a>

###do_list_queue/6##




<pre>do_list_queue(Kvdb_ref::#kvdb_ref{}, Table::<a href="#type-table">table()</a>, Q::<a href="#type-queue_name">queue_name()</a>, _Fltr::fun((active | inactive, tuple()) -> keep | keep_raw | skip | tuple()), _Inactive::boolean(), _Limit::integer() | infinity) -> [<a href="#type-object">object()</a>] | {error, any()}</pre>
<br></br>


<a name="do_next-3"></a>

###do_next/3##




<pre>do_next(Db::<a href="#type-db_ref">db_ref()</a>, Table::<a href="#type-table">table()</a>, FromKey::binary()) -> {ok, Key::binary()} | {ok, Key::binary(), Value::binary()} | done | {error, any()}</pre>
<br></br>


<a name="do_next_queue-3"></a>

###do_next_queue/3##




<pre>do_next_queue(Kvdb_ref::#kvdb_ref{}, Table0::<a href="#type-table">table()</a>, _Q::<a href="#type-queue_name">queue_name()</a>) -> {ok, any()} | done</pre>
<br></br>


<a name="do_pop-2"></a>

###do_pop/2##




<pre>do_pop(Db::<a href="#type-db_ref">db_ref()</a>, Table::<a href="#type-table">table()</a>) -> {ok, <a href="#type-object">object()</a>} | done | {error, any()}</pre>
<br></br>




Equivalent to [`do_pop(Db, Table, <<>>)`](#do_pop-3).<a name="do_pop-3"></a>

###do_pop/3##




<pre>do_pop(Db::<a href="#type-db_ref">db_ref()</a>, Table::<a href="#type-table">table()</a>, Q::<a href="#type-queue_name">queue_name()</a>) -> {ok, <a href="#type-object">object()</a>} | done | blocked | {error, any()}</pre>
<br></br>




Low-level equivalent of [`pop/3`](#pop-3)<a name="do_prefix_match-4"></a>

###do_prefix_match/4##




`do_prefix_match(Kvdb_ref, Table0, Prefix, Limit) -> any()`

<a name="do_prel_pop-2"></a>

###do_prel_pop/2##




<pre>do_prel_pop(Db::<a href="#type-db_ref">db_ref()</a>, Table::<a href="#type-table">table()</a>) -> {ok, <a href="#type-object">object()</a>, binary()} | done | blocked | {error, any()}</pre>
<br></br>


<a name="do_prel_pop-3"></a>

###do_prel_pop/3##




<pre>do_prel_pop(Db::<a href="#type-db_ref">db_ref()</a>, Table::<a href="#type-table">table()</a>, Q::<a href="#type-queue_name">queue_name()</a>) -> {ok, <a href="#type-object">object()</a>, binary()} | done | blocked | {error, any()}</pre>
<br></br>


<a name="do_prev-3"></a>

###do_prev/3##




<pre>do_prev(Db::<a href="#type-db_ref">db_ref()</a>, Table::<a href="#type-table">table()</a>, FromKey::binary()) -> {ok, Key::binary()} | {ok, Key::binary(), Value::binary()} | done | {error, any()}</pre>
<br></br>


<a name="do_push-3"></a>

###do_push/3##




<pre>do_push(Db::<a href="#type-db_ref">db_ref()</a>, Table::<a href="#type-table">table()</a>, Obj::<a href="#type-object">object()</a>) -> {ok, ActualKey::any()} | {error, any()}</pre>
<br></br>




Equivalent to [`do_push(Db, Table, <<>>, Obj)`](#do_push-4).<a name="do_push-4"></a>

###do_push/4##




<pre>do_push(Db::<a href="#type-db_ref">db_ref()</a>, Table::<a href="#type-table">table()</a>, Q::any(), Obj::<a href="#type-object">object()</a>) -> {ok, ActualKey::any()} | {error, any()}</pre>
<br></br>




Low-level equivalent of [`push/4`](#push-4)<a name="do_put-3"></a>

###do_put/3##




<pre>do_put(Db::<a href="#type-db_ref">db_ref()</a>, Table::<a href="#type-table">table()</a>, Obj::<a href="#type-object">object()</a>) -> ok | {error, any()}</pre>
<br></br>




Low-level equivalent to [`put/3`](#put-3)<a name="do_put_attr-5"></a>

###do_put_attr/5##




<pre>do_put_attr(Kvdb_ref::<a href="#type-db_ref">db_ref()</a>, Table::<a href="#type-table">table()</a>, Key::<a href="#type-key">key()</a>, AttrN::atom(), Value::any()) -> ok | {error, any()}</pre>
<br></br>


<a name="do_put_attrs-4"></a>

###do_put_attrs/4##




`do_put_attrs(Kvdb_ref, Table0, Key, As) -> any()`

<a name="do_select-4"></a>

###do_select/4##




`do_select(Kvdb_ref, Table0, MatchSpec, Limit) -> any()`

<a name="dump_tables-1"></a>

###dump_tables/1##




<pre>dump_tables(Name::<a href="#type-db_name">db_name()</a>) -> list()</pre>
<br></br>






Returns the contents of the database as a list of objects



This function is mainly for debugging, and should not be called on a  
large database.

The exact format of the list may vary from backend to backend.<a name="extract-3"></a>

###extract/3##




<pre>extract(Name::<a href="#type-db_name">db_name()</a>, Table::<a href="#type-table">table()</a>, Key::binary()) -> {ok, <a href="#type-object">object()</a>} | {error, any()}</pre>
<br></br>


<a name="first-2"></a>

###first/2##




`first(Name, Table) -> any()`

<a name="first_queue-2"></a>

###first_queue/2##




<pre>first_queue(Name::<a href="#type-db_name">db_name()</a>, Table::<a href="#type-table">table()</a>) -> {ok, <a href="#type-queue_name">queue_name()</a>} | done</pre>
<br></br>


<a name="get-3"></a>

###get/3##




<pre>get(Name::<a href="#type-db_name">db_name()</a>, Table::<a href="#type-table">table()</a>, Key::binary()) -> {ok, <a href="#type-object">object()</a>} | {error, any()}</pre>
<br></br>






Perform a lookup on `Key` in `Table`

Returns `{ok, Object}` or `{error, Reason}`, e.g. `{error, not_found}`
if the object could not be found.<a name="get_attr-4"></a>

###get_attr/4##




`get_attr(Name, Table, Key, Attr) -> any()`

<a name="get_attrs-3"></a>

###get_attrs/3##




`get_attrs(Name, Table, Key) -> any()`

<a name="index_get-4"></a>

###index_get/4##




<pre>index_get(Name::<a href="#type-db_name">db_name()</a>, Table::<a href="#type-table">table()</a>, _IxName::any(), _IxVal::any()) -> [<a href="#type-object">object()</a>] | {error, any()}</pre>
<br></br>






Perform an index lookup on the named index of Table

This function returns a list of objects referenced by the index value, or
an `{error, Reason}` tuple, if there is no such index for the Table.<a name="info-2"></a>

###info/2##




<pre>info(Name::<a href="#type-db_name">db_name()</a>, Item::<a href="#type-attr_name">attr_name()</a>) -> undefined | <a href="#type-attr_value">attr_value()</a></pre>
<br></br>


<a name="is_queue_empty-3"></a>

###is_queue_empty/3##




<pre>is_queue_empty(Name::<a href="#type-db_name">db_name()</a>, Table::<a href="#type-table">table()</a>, _Q::<a href="#type-queue_name">queue_name()</a>) -> boolean()</pre>
<br></br>


<a name="last-2"></a>

###last/2##




`last(Name, Table) -> any()`

<a name="list_queue-3"></a>

###list_queue/3##




<pre>list_queue(Name::<a href="#type-db_name">db_name()</a>, Table::<a href="#type-table">table()</a>, Q::<a href="#type-queue_name">queue_name()</a>) -> [<a href="#type-object">object()</a>] | {error, any()}</pre>
<br></br>


<a name="list_queue-6"></a>

###list_queue/6##




`list_queue(Name, Table, Q, Fltr, Inactive, Limit) -> any()`

<a name="list_tables-1"></a>

###list_tables/1##




<pre>list_tables(Kvdb_ref::<a href="#type-db_name">db_name()</a> | <a href="#type-db_ref">db_ref()</a>) -> [binary()]</pre>
<br></br>




Lists the tables defined in the database<a name="next-3"></a>

###next/3##




`next(Name, Table, Key) -> any()`

<a name="next_queue-3"></a>

###next_queue/3##




<pre>next_queue(Name::<a href="#type-db_name">db_name()</a>, Table::<a href="#type-table">table()</a>, _Q::<a href="#type-queue_name">queue_name()</a>) -> {ok, any()} | done</pre>
<br></br>


<a name="open-2"></a>

###open/2##




<pre>open(Name::<a href="#type-db_name">db_name()</a>, Options::[{atom(), term()}]) -> {ok, <a href="#type-db_ref">db_ref()</a>} | {error, term()}</pre>
<br></br>






Opens a database



Options:



- `{backend, Backend}` - select a backend
<br></br>

Supported backends are Sqlite3 (`sqlite` or `sqlite3`) and `leveldb`,
or any module that implements the `kvdb` behaviour.



- `{schema, SchemaMod}` - Callback module used for validation and triggers.
The module must implement the `kvdb_schema` behaviour.



- `{file, File}` - File name or directory name of the database.



- `{encoding, Encoding}` - Default encoding for tables.

- `{db_opts, DbOpts}` - Backend-specific options.<a name="open_db-2"></a>

###open_db/2##




<pre>open_db(Name, Options) -&gt; {ok, Pid} | {error, Reason}</pre>
<br></br>




Opens a kvdb database instance.
<a name="pop-2"></a>

###pop/2##




<pre>pop(Name::<a href="#type-db_name">db_name()</a>, Table::<a href="#type-table">table()</a>) -> {ok, <a href="#type-object">object()</a>} | done | blocked | {error, any()}</pre>
<br></br>




Equivalent to [`pop(Name, Table, <<>>)`](#pop-3).<a name="pop-3"></a>

###pop/3##




<pre>pop(Name::<a href="#type-db_name">db_name()</a>, Table::<a href="#type-table">table()</a>, Q::<a href="#type-queue_name">queue_name()</a>) -> {ok, <a href="#type-object">object()</a>} | done | blocked | {error, any()}</pre>
<br></br>




Fetches and deletes the 'first' object in the given queue<a name="prefix_match-3"></a>

###prefix_match/3##




`prefix_match(Db, Table, Prefix) -> any()`

<a name="prefix_match-4"></a>

###prefix_match/4##




`prefix_match(Db, Table, Prefix, Limit) -> any()`

<a name="prel_pop-2"></a>

###prel_pop/2##




<pre>prel_pop(Name::<a href="#type-db_name">db_name()</a>, Table::<a href="#type-table">table()</a>) -> {ok, <a href="#type-object">object()</a>, binary()} | done | {error, any()}</pre>
<br></br>


<a name="prel_pop-3"></a>

###prel_pop/3##




<pre>prel_pop(Name::<a href="#type-db_name">db_name()</a>, Table::<a href="#type-table">table()</a>, Q::<a href="#type-queue_name">queue_name()</a>) -> {ok, <a href="#type-object">object()</a>, binary()} | done | {error, any()}</pre>
<br></br>


<a name="prev-3"></a>

###prev/3##




`prev(Name, Table, Key) -> any()`

<a name="push-3"></a>

###push/3##




<pre>push(Name::<a href="#type-db_name">db_name()</a>, Table::<a href="#type-table">table()</a>, Obj::<a href="#type-object">object()</a>) -> {ok, _ActualKey::any()} | {error, any()}</pre>
<br></br>




Equivalent to [`push(Name, Table, <<>>, Obj)`](#push-4).<a name="push-4"></a>

###push/4##




<pre>push(Name::any(), Table::<a href="#type-table">table()</a>, Q::<a href="#type-queue_name">queue_name()</a>, Obj::<a href="#type-object">object()</a>) -> {ok, _ActualKey::any()} | {error, any()}</pre>
<br></br>






Push an object onto a persistent queue

`Table` must be of one of the queue types (see [`create_table/3`](#create_table-3)).
The queue identifier `Q` specifies a given queue instance inside the table
(there may be a large number of queue instances), and a special key is
created to uniquely identify the inserted object. The actual key must be
used to delete the object (unless it is automatically removed using the
[`pop/3`](#pop-3) function.<a name="put-3"></a>

###put/3##




<pre>put(Name::any(), Table::<a href="#type-table">table()</a>, Obj::<a href="#type-object">object()</a>) -> ok | {error, any()}</pre>
<br></br>




Inserts an object into Table<a name="put_attr-5"></a>

###put_attr/5##




<pre>put_attr(Name::<a href="#type-db_name">db_name()</a>, Table::<a href="#type-table">table()</a>, Key::<a href="#type-key">key()</a>, Attr::atom(), Value::any()) -> ok | {error, any()}</pre>
<br></br>


<a name="put_attrs-4"></a>

###put_attrs/4##




<pre>put_attrs(Name::any(), Table::<a href="#type-table">table()</a>, Key::<a href="#type-key">key()</a>, Attrs::<a href="#type-attrs">attrs()</a>) -> ok | {error, any()}</pre>
<br></br>


<a name="select-3"></a>

###select/3##




<pre>select(Db, Table, MatchSpec) -&gt; {Objects, Cont} | done</pre>
<br></br>






Similar to ets:select/3.

This function builds on prefix_match/3, and applies a match specification
on the results. If keys are using `raw` encoding, a partial key can be
given using string syntax, e.g. `"abc" ++ '_'`. Note that this
will necessitate some data conversion back and forth on the found objects.
If a prefix cannot be determined for the key, a full traversal of the table
will be performed. `sext`-encoded keys can be prefixed in the same way as
normal erlang terms in an ets:select().<a name="select-4"></a>

###select/4##




`select(Db, Table, MatchSpec, Limit) -> any()`

<a name="start_link-2"></a>

###start_link/2##




`start_link(Name, Backend) -> any()`

<a name="start_session-2"></a>

###start_session/2##




`start_session(Name, Id) -> any()`

