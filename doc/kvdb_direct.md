

#Module kvdb_direct#
* [Description](#description)
* [Function Index](#index)
* [Function Details](#functions)



Key-value database frontend.

Copyright (c) 2011-2012, Feuerlabs Inc

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


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#add_table-3">add_table/3</a></td><td>Low-level equivalent to <a href="#add_table-3"><code>add_table/3</code></a></td></tr><tr><td valign="top"><a href="#delete-3">delete/3</a></td><td></td></tr><tr><td valign="top"><a href="#delete_table-2">delete_table/2</a></td><td>low-level equivalent to <a href="#delete_table-2"><code>delete_table/2</code></a></td></tr><tr><td valign="top"><a href="#dump_tables-1">dump_tables/1</a></td><td>Low-level equivalent to <a href="#dump_tables-1"><code>dump_tables/1</code></a></td></tr><tr><td valign="top"><a href="#extract-3">extract/3</a></td><td></td></tr><tr><td valign="top"><a href="#first-2">first/2</a></td><td></td></tr><tr><td valign="top"><a href="#first_queue-2">first_queue/2</a></td><td></td></tr><tr><td valign="top"><a href="#get-3">get/3</a></td><td>Low-level equivalent of <a href="#get-3"><code>get/3</code></a></td></tr><tr><td valign="top"><a href="#get_attrs-4">get_attrs/4</a></td><td></td></tr><tr><td valign="top"><a href="#index_get-4">index_get/4</a></td><td>Low-level equivalent of <a href="#index_get-4"><code>index_get/4</code></a></td></tr><tr><td valign="top"><a href="#index_keys-4">index_keys/4</a></td><td>Low-level equivalent of <a href="#index_keys-4"><code>index_keys/4</code></a></td></tr><tr><td valign="top"><a href="#info-2">info/2</a></td><td></td></tr><tr><td valign="top"><a href="#is_queue_empty-3">is_queue_empty/3</a></td><td></td></tr><tr><td valign="top"><a href="#last-2">last/2</a></td><td></td></tr><tr><td valign="top"><a href="#list_queue-3">list_queue/3</a></td><td></td></tr><tr><td valign="top"><a href="#list_queue-6">list_queue/6</a></td><td></td></tr><tr><td valign="top"><a href="#list_tables-1">list_tables/1</a></td><td>Lists the tables defined in the database.</td></tr><tr><td valign="top"><a href="#next-3">next/3</a></td><td></td></tr><tr><td valign="top"><a href="#next_queue-3">next_queue/3</a></td><td></td></tr><tr><td valign="top"><a href="#pop-2">pop/2</a></td><td>Equivalent to <a href="#pop-3"><tt>pop(Db, Table, &lt;&lt;&gt;&gt;)</tt></a>.</td></tr><tr><td valign="top"><a href="#pop-3">pop/3</a></td><td>Low-level equivalent of <a href="#pop-3"><code>pop/3</code></a></td></tr><tr><td valign="top"><a href="#prefix_match-4">prefix_match/4</a></td><td></td></tr><tr><td valign="top"><a href="#prel_pop-2">prel_pop/2</a></td><td></td></tr><tr><td valign="top"><a href="#prel_pop-3">prel_pop/3</a></td><td></td></tr><tr><td valign="top"><a href="#prev-3">prev/3</a></td><td></td></tr><tr><td valign="top"><a href="#push-3">push/3</a></td><td>Equivalent to <a href="#push-4"><tt>push(Db, Table, &lt;&lt;&gt;&gt;, Obj)</tt></a>.</td></tr><tr><td valign="top"><a href="#push-4">push/4</a></td><td>Low-level equivalent of <a href="#push-4"><code>push/4</code></a></td></tr><tr><td valign="top"><a href="#put-3">put/3</a></td><td>Low-level equivalent to <a href="#put-3"><code>put/3</code></a></td></tr><tr><td valign="top"><a href="#select-4">select/4</a></td><td></td></tr><tr><td valign="top"><a href="#update_counter-4">update_counter/4</a></td><td></td></tr></table>


<a name="functions"></a>

##Function Details##

<a name="add_table-3"></a>

###add_table/3##


`add_table(Kvdb_ref, Table0, Opts) -> any()`

Low-level equivalent to [`add_table/3`](#add_table-3)<a name="delete-3"></a>

###delete/3##


<pre>delete(Db::<a href="#type-db_ref">db_ref()</a>, Table::<a href="#type-table">table()</a>, Key::binary()) -> ok | {error, any()}</pre>
<br></br>


<a name="delete_table-2"></a>

###delete_table/2##


<pre>delete_table(Db::<a href="#type-db_ref">db_ref()</a>, Table::<a href="#type-table">table()</a>) -> ok | {error, any()}</pre>
<br></br>


low-level equivalent to [`delete_table/2`](#delete_table-2)<a name="dump_tables-1"></a>

###dump_tables/1##


<pre>dump_tables(Kvdb_ref::<a href="#type-db_ref">db_ref()</a>) -> list()</pre>
<br></br>


Low-level equivalent to [`dump_tables/1`](#dump_tables-1)<a name="extract-3"></a>

###extract/3##


<pre>extract(Kvdb_ref::#kvdb_ref{}, Table::<a href="#type-table">table()</a>, Key::binary()) -> {ok, <a href="#type-object">object()</a>} | {error, any()}</pre>
<br></br>


<a name="first-2"></a>

###first/2##


<pre>first(Db::<a href="#type-db_ref">db_ref()</a>, Table::<a href="#type-table">table()</a>) -> {ok, Key::binary()} | {ok, Key::binary(), Value::binary()} | done | {error, any()}</pre>
<br></br>


<a name="first_queue-2"></a>

###first_queue/2##


<pre>first_queue(Kvdb_ref::#kvdb_ref{}, Table0::<a href="#type-table">table()</a>) -> {ok, <a href="#type-queue_name">queue_name()</a>} | done</pre>
<br></br>


<a name="get-3"></a>

###get/3##


<pre>get(Db::<a href="#type-db_ref">db_ref()</a>, Table::<a href="#type-table">table()</a>, Key::any()) -> {ok, <a href="#type-object">object()</a>} | {error, any()}</pre>
<br></br>


Low-level equivalent of [`get/3`](#get-3)<a name="get_attrs-4"></a>

###get_attrs/4##


`get_attrs(Kvdb_ref, Table0, Key, As) -> any()`

<a name="index_get-4"></a>

###index_get/4##


<pre>index_get(Kvdb_ref::<a href="#type-db_ref">db_ref()</a>, Table0::<a href="#type-table">table()</a>, _IxName::any(), _IxVal::any()) -> [<a href="#type-object">object()</a>] | {error, any()}</pre>
<br></br>


Low-level equivalent of [`index_get/4`](#index_get-4)<a name="index_keys-4"></a>

###index_keys/4##


`index_keys(Kvdb_ref, Table0, IxName, IxVal) -> any()`

Low-level equivalent of [`index_keys/4`](#index_keys-4)<a name="info-2"></a>

###info/2##


<pre>info(Kvdb_ref::<a href="#type-db_ref">db_ref()</a>, Item::<a href="#type-attr_name">attr_name()</a>) -> undefined | <a href="#type-attr_value">attr_value()</a></pre>
<br></br>


<a name="is_queue_empty-3"></a>

###is_queue_empty/3##


<pre>is_queue_empty(Kvdb_ref::#kvdb_ref{}, Table0::<a href="#type-table">table()</a>, _Q::<a href="#type-queue_name">queue_name()</a>) -> boolean()</pre>
<br></br>


<a name="last-2"></a>

###last/2##


<pre>last(Db::<a href="#type-db_ref">db_ref()</a>, Table::<a href="#type-table">table()</a>) -> {ok, Key::binary()} | {ok, Key::binary(), Value::binary()} | done | {error, any()}</pre>
<br></br>


<a name="list_queue-3"></a>

###list_queue/3##


<pre>list_queue(Kvdb_ref::#kvdb_ref{}, Table::<a href="#type-table">table()</a>, Q::<a href="#type-queue_name">queue_name()</a>) -> [<a href="#type-object">object()</a>] | {error, any()}</pre>
<br></br>


<a name="list_queue-6"></a>

###list_queue/6##


<pre>list_queue(Kvdb_ref::#kvdb_ref{}, Table::<a href="#type-table">table()</a>, Q::<a href="#type-queue_name">queue_name()</a>, _Fltr::fun((active | inactive, tuple()) -> keep | keep_raw | skip | tuple()), _Inactive::boolean(), _Limit::integer() | infinity) -> [<a href="#type-object">object()</a>] | {error, any()}</pre>
<br></br>


<a name="list_tables-1"></a>

###list_tables/1##


<pre>list_tables(Kvdb_ref::<a href="#type-db_ref">db_ref()</a>) -> [binary()]</pre>
<br></br>


Lists the tables defined in the database<a name="next-3"></a>

###next/3##


<pre>next(Db::<a href="#type-db_ref">db_ref()</a>, Table::<a href="#type-table">table()</a>, FromKey::binary()) -> {ok, Key::binary()} | {ok, Key::binary(), Value::binary()} | done | {error, any()}</pre>
<br></br>


<a name="next_queue-3"></a>

###next_queue/3##


<pre>next_queue(Kvdb_ref::#kvdb_ref{}, Table0::<a href="#type-table">table()</a>, _Q::<a href="#type-queue_name">queue_name()</a>) -> {ok, any()} | done</pre>
<br></br>


<a name="pop-2"></a>

###pop/2##


<pre>pop(Db::<a href="#type-db_ref">db_ref()</a>, Table::<a href="#type-table">table()</a>) -> {ok, <a href="#type-object">object()</a>} | done | {error, any()}</pre>
<br></br>


Equivalent to [`pop(Db, Table, <<>>)`](#pop-3).<a name="pop-3"></a>

###pop/3##


<pre>pop(Db::<a href="#type-db_ref">db_ref()</a>, Table::<a href="#type-table">table()</a>, Q::<a href="#type-queue_name">queue_name()</a>) -> {ok, <a href="#type-object">object()</a>} | done | blocked | {error, any()}</pre>
<br></br>


Low-level equivalent of [`pop/3`](#pop-3)<a name="prefix_match-4"></a>

###prefix_match/4##


`prefix_match(Kvdb_ref, Table0, Prefix, Limit) -> any()`

<a name="prel_pop-2"></a>

###prel_pop/2##


<pre>prel_pop(Db::<a href="#type-db_ref">db_ref()</a>, Table::<a href="#type-table">table()</a>) -> {ok, <a href="#type-object">object()</a>, binary()} | done | blocked | {error, any()}</pre>
<br></br>


<a name="prel_pop-3"></a>

###prel_pop/3##


<pre>prel_pop(Db::<a href="#type-db_ref">db_ref()</a>, Table::<a href="#type-table">table()</a>, Q::<a href="#type-queue_name">queue_name()</a>) -> {ok, <a href="#type-object">object()</a>, binary()} | done | blocked | {error, any()}</pre>
<br></br>


<a name="prev-3"></a>

###prev/3##


<pre>prev(Db::<a href="#type-db_ref">db_ref()</a>, Table::<a href="#type-table">table()</a>, FromKey::binary()) -> {ok, Key::binary()} | {ok, Key::binary(), Value::binary()} | done | {error, any()}</pre>
<br></br>


<a name="push-3"></a>

###push/3##


<pre>push(Db::<a href="#type-db_ref">db_ref()</a>, Table::<a href="#type-table">table()</a>, Obj::<a href="#type-object">object()</a>) -> {ok, ActualKey::any()} | {error, any()}</pre>
<br></br>


Equivalent to [`push(Db, Table, <<>>, Obj)`](#push-4).<a name="push-4"></a>

###push/4##


<pre>push(Db::<a href="#type-db_ref">db_ref()</a>, Table::<a href="#type-table">table()</a>, Q::any(), Obj::<a href="#type-object">object()</a>) -> {ok, ActualKey::any()} | {error, any()}</pre>
<br></br>


Low-level equivalent of [`push/4`](#push-4)<a name="put-3"></a>

###put/3##


<pre>put(Db::<a href="#type-db_ref">db_ref()</a>, Table::<a href="#type-table">table()</a>, Obj::<a href="#type-object">object()</a>) -> ok | {error, any()}</pre>
<br></br>


Low-level equivalent to [`put/3`](#put-3)<a name="select-4"></a>

###select/4##


`select(Kvdb_ref, Table0, MatchSpec, Limit) -> any()`

<a name="update_counter-4"></a>

###update_counter/4##


<pre>update_counter(Db::<a href="#type-db_ref">db_ref()</a>, Table::<a href="#type-table">table()</a>, Key::binary(), Incr::<a href="#type-increment">increment()</a>) -> integer()</pre>
<br></br>


