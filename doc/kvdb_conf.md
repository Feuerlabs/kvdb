

#Module kvdb_conf#
* [Description](#description)
* [Function Index](#index)
* [Function Details](#functions)



API to store NETCONF-style config data in kvdb.

__Authors:__ Ulf Wiger ([`ulf@feuerlabs.com`](mailto:ulf@feuerlabs.com)).<a name="description"></a>

##Description##




The `kvdb_conf` API offers a number of functions geared towards
management of a configuration database. Specifically, `kvdb_conf` is
designed to work well with configuration management à la
[NETCONF (RFC 6241)"](http://tools.ietf.org.md/rfc6241) and
the [YANG data modeling
language (RFC 6020)](http://tools.ietf.org.md/rfc6020). When used in the Feuerlabs Exosense(tm) platform,
`kvdb_conf` ties into the device lifecycle management concepts of the
Feuerlabs ExoDM Device Management Platform.



The database itself is a `kvdb` database, so the `kvdb` API also applies.



Specifically, `kvdb_conf` provides the following functionality on top
of `kvdb`:



* The object structure is `{Key, Attribues, Value}`
* Keys are always `raw`-encoded, and reflect a hierarchy
* A hierarchical key can be constructed using '*' as a join symbol.
* Keys can be constructed/deconstructed with
[`join_key/1`](#join_key-1)/[`split_key/1`](#split_key-1)
* Whole configuration subtrees can be read/written as one operation
* Subtrees can be efficiently skipped during traversal due to key encoding.
identifiers are stored as a structured key (binary)
delimited by:



Netconf identifiers can consist of alphanumerics, '-', '_' or '.'.
The '*' as delimiter is chosen so that a "wildcard" character can be
used that is greater than the delimiter, but smaller than any identifier
character.

For further generality, `kvdb_conf` keys are escaped, using an algorithm
that doesn't upset the sort order, or ability to skip past subtrees during
traversal. Two functions exist for construction/deconstruction of
composite keys: [`join_key/1`](#join_key-1) and [`split_key/1`](#split_key-1). These functions
also escape/unescape keys that are outside the Netconf alphabet. An escaped
key starts with '='. The escaping character is '@', followed by the hex
code of the escaped character. For efficiency, any key (or key part) that
starts with '=' will be considered escaped. Thus, no unescaped key may
begin with '=' (this is not enforced, but will lead to unpredictable
behavior).<a name="index"></a>

##Function Index##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#add_table-1">add_table/1</a></td><td>Equivalent to <a href="#add_table-2"><tt>add_table(T, [])</tt></a>.</td></tr><tr><td valign="top"><a href="#add_table-2">add_table/2</a></td><td>Adds a table to the <code>kvdb_conf</code> database.</td></tr><tr><td valign="top"><a href="#all-0">all/0</a></td><td>Equivalent to <a href="#all-1"><tt>all(&lt;&lt;"data"&gt;&gt;)</tt></a>.</td></tr><tr><td valign="top"><a href="#all-1">all/1</a></td><td></td></tr><tr><td valign="top"><a href="#close-0">close/0</a></td><td>Closes the current <code>kvdb_conf</code> database.</td></tr><tr><td valign="top"><a href="#decode_id-1">decode_id/1</a></td><td></td></tr><tr><td valign="top"><a href="#delete-1">delete/1</a></td><td>Equivalent to <a href="#delete-2"><tt>delete(&lt;&lt;"data"&gt;&gt;, K)</tt></a>.</td></tr><tr><td valign="top"><a href="#delete-2">delete/2</a></td><td>Deletes an object denoted by Key, returns <code>ok</code> even if object not found.</td></tr><tr><td valign="top"><a href="#delete_all-1">delete_all/1</a></td><td>Equivalent to <a href="#delete_all-2"><tt>delete_all(&lt;&lt;"data"&gt;&gt;, Prefix)</tt></a>.</td></tr><tr><td valign="top"><a href="#delete_all-2">delete_all/2</a></td><td>Deletes all objects with a key matching <code>Prefix</code>.</td></tr><tr><td valign="top"><a href="#encode_id-1">encode_id/1</a></td><td></td></tr><tr><td valign="top"><a href="#escape_key-1">escape_key/1</a></td><td></td></tr><tr><td valign="top"><a href="#escape_prefix-1">escape_prefix/1</a></td><td></td></tr><tr><td valign="top"><a href="#first-0">first/0</a></td><td></td></tr><tr><td valign="top"><a href="#first-1">first/1</a></td><td></td></tr><tr><td valign="top"><a href="#first_top_key-0">first_top_key/0</a></td><td></td></tr><tr><td valign="top"><a href="#first_top_key-1">first_top_key/1</a></td><td></td></tr><tr><td valign="top"><a href="#first_tree-0">first_tree/0</a></td><td></td></tr><tr><td valign="top"><a href="#first_tree-1">first_tree/1</a></td><td></td></tr><tr><td valign="top"><a href="#flatten_tree-1">flatten_tree/1</a></td><td>Converts a configuration tree into an ordered list of configuration objects.</td></tr><tr><td valign="top"><a href="#is_list_key-1">is_list_key/1</a></td><td></td></tr><tr><td valign="top"><a href="#join_key-1">join_key/1</a></td><td>Creates a kvdb_conf key out of a list of key parts.</td></tr><tr><td valign="top"><a href="#join_key-2">join_key/2</a></td><td></td></tr><tr><td valign="top"><a href="#last-0">last/0</a></td><td></td></tr><tr><td valign="top"><a href="#last-1">last/1</a></td><td></td></tr><tr><td valign="top"><a href="#last_tree-0">last_tree/0</a></td><td></td></tr><tr><td valign="top"><a href="#last_tree-1">last_tree/1</a></td><td></td></tr><tr><td valign="top"><a href="#list_key-2">list_key/2</a></td><td></td></tr><tr><td valign="top"><a href="#make_tree-1">make_tree/1</a></td><td>Converts an ordered list of configuration objects into a configuration tree.</td></tr><tr><td valign="top"><a href="#next-1">next/1</a></td><td></td></tr><tr><td valign="top"><a href="#next-2">next/2</a></td><td></td></tr><tr><td valign="top"><a href="#next_at_level-1">next_at_level/1</a></td><td></td></tr><tr><td valign="top"><a href="#next_at_level-2">next_at_level/2</a></td><td></td></tr><tr><td valign="top"><a href="#next_tree-1">next_tree/1</a></td><td></td></tr><tr><td valign="top"><a href="#next_tree-2">next_tree/2</a></td><td></td></tr><tr><td valign="top"><a href="#open-1">open/1</a></td><td>Equivalent to <a href="#open-2"><tt>open(File, [])</tt></a>.</td></tr><tr><td valign="top"><a href="#open-2">open/2</a></td><td>Opens a kvdb_conf-compliant kvdb database.</td></tr><tr><td valign="top"><a href="#options-1">options/1</a></td><td></td></tr><tr><td valign="top"><a href="#prefix_match-1">prefix_match/1</a></td><td>Equivalent to <a href="#prefix_matc-2"><tt>prefix_matc(&lt;&lt;"data"&gt;&gt;, Prefix)</tt></a>.</td></tr><tr><td valign="top"><a href="#prefix_match-2">prefix_match/2</a></td><td>Performs a prefix match, returning all matching objects.</td></tr><tr><td valign="top"><a href="#prev-1">prev/1</a></td><td></td></tr><tr><td valign="top"><a href="#prev-2">prev/2</a></td><td></td></tr><tr><td valign="top"><a href="#read-1">read/1</a></td><td>Equivalent to <a href="#read-2"><tt>read(&lt;&lt;"data"&gt;&gt;, Key)</tt></a>.</td></tr><tr><td valign="top"><a href="#read-2">read/2</a></td><td>Reads a configuration object from the database.</td></tr><tr><td valign="top"><a href="#read_tree-1">read_tree/1</a></td><td>Read a configuration (sub-)tree matching Prefix.</td></tr><tr><td valign="top"><a href="#read_tree-2">read_tree/2</a></td><td></td></tr><tr><td valign="top"><a href="#shift_root-2">shift_root/2</a></td><td></td></tr><tr><td valign="top"><a href="#split_key-1">split_key/1</a></td><td>Splits a <code>kvdb_conf` key into a list of key parts

Example: `split_key(<<"a*b*c">>) -> [<<"a">>,<<"b">>,<<"c">>].</code></td></tr><tr><td valign="top"><a href="#store_tree-1">store_tree/1</a></td><td>Store a configuration tree in the database.</td></tr><tr><td valign="top"><a href="#store_tree-2">store_tree/2</a></td><td></td></tr><tr><td valign="top"><a href="#unescape_key-1">unescape_key/1</a></td><td></td></tr><tr><td valign="top"><a href="#update_counter-2">update_counter/2</a></td><td>Equivalent to <a href="#update_counter-3"><tt>update_counter(&lt;&lt;"data"&gt;&gt;, Key, Incr)</tt></a>.</td></tr><tr><td valign="top"><a href="#update_counter-3">update_counter/3</a></td><td>Updates a counter with the given increment.</td></tr><tr><td valign="top"><a href="#write-1">write/1</a></td><td>Equivalent to <a href="#write-2"><tt>write(&lt;&lt;"data"&gt;&gt;, Obj)</tt></a>.</td></tr><tr><td valign="top"><a href="#write-2">write/2</a></td><td>Writes a configuration object into the database.</td></tr><tr><td valign="top"><a href="#write_tree-2">write_tree/2</a></td><td>Writes a configuration tree under the given parent.</td></tr><tr><td valign="top"><a href="#write_tree-3">write_tree/3</a></td><td></td></tr></table>


<a name="functions"></a>

##Function Details##

<a name="add_table-1"></a>

###add_table/1##


<pre>add_table(T::<a href="kvdb.md#type-table">kvdb:table()</a>) -> ok</pre>
<br></br>


Equivalent to [`add_table(T, [])`](#add_table-2).<a name="add_table-2"></a>

###add_table/2##


<pre>add_table(T::<a href="kvdb.md#type-table">kvdb:table()</a>, Opts::<a href="kvdb.md#type-options">kvdb:options()</a>) -> ok</pre>
<br></br>




Adds a table to the `kvdb_conf` database. By default, `kvdb_conf`
uses a table called `data`, which is created when the database is first
created. Additional tables can opt for different types of indexing, or
encoding of the `Value` part. The `Key` part must always be `raw`, since
the `kvdb_conf` API requires the keys to be of type `binary()`, and the
object structure must be `{Key, Attributes, Value}`. The default encoding
is `{raw, term, raw}`.



(While it is of course possible to store binary keys with any type of
encoding, enforcing `raw` encoding ensures that this restriction is not
subverted through the normal `kvdb` API).

All other table options supported by [`kvdb:add_table/3`](kvdb.md#add_table-3) are also
supported here.<a name="all-0"></a>

###all/0##


<pre>all() -> [<a href="#type-conf_obj">conf_obj()</a>]</pre>
<br></br>


Equivalent to [`all(<<"data">>)`](#all-1).<a name="all-1"></a>

###all/1##


`all(Tab) -> any()`

<a name="close-0"></a>

###close/0##


<pre>close() -&gt; ok</pre>
<br></br>


Closes the current `kvdb_conf` database.<a name="decode_id-1"></a>

###decode_id/1##


`decode_id(Bin) -> any()`

<a name="delete-1"></a>

###delete/1##


<pre>delete(K::<a href="kvdb.md#type-key">kvdb:key()</a>) -> ok</pre>
<br></br>


Equivalent to [`delete(<<"data">>, K)`](#delete-2).<a name="delete-2"></a>

###delete/2##


<pre>delete(Tab::<a href="kvdb.md#type-table">kvdb:table()</a>, K::<a href="kvdb.md#type-key">kvdb:key()</a>) -> ok</pre>
<br></br>


Deletes an object denoted by Key, returns `ok` even if object not found.<a name="delete_all-1"></a>

###delete_all/1##


<pre>delete_all(Prefix::<a href="kvdb.md#type-prefix">kvdb:prefix()</a>) -> ok</pre>
<br></br>


Equivalent to [`delete_all(<<"data">>, Prefix)`](#delete_all-2).<a name="delete_all-2"></a>

###delete_all/2##


<pre>delete_all(Tab::<a href="kvdb.md#type-table">kvdb:table()</a>, Prefix::<a href="kvdb.md#type-prefix">kvdb:prefix()</a>) -> ok</pre>
<br></br>


Deletes all objects with a key matching `Prefix`. Always returns `ok`.<a name="encode_id-1"></a>

###encode_id/1##


`encode_id(L) -> any()`

<a name="escape_key-1"></a>

###escape_key/1##


`escape_key(K) -> any()`

<a name="escape_prefix-1"></a>

###escape_prefix/1##


`escape_prefix(P) -> any()`

<a name="first-0"></a>

###first/0##


`first() -> any()`

<a name="first-1"></a>

###first/1##


`first(Tab) -> any()`

<a name="first_top_key-0"></a>

###first_top_key/0##


`first_top_key() -> any()`

<a name="first_top_key-1"></a>

###first_top_key/1##


`first_top_key(Tab) -> any()`

<a name="first_tree-0"></a>

###first_tree/0##


`first_tree() -> any()`

<a name="first_tree-1"></a>

###first_tree/1##


`first_tree(Tab) -> any()`

<a name="flatten_tree-1"></a>

###flatten_tree/1##


<pre>flatten_tree(Conf_tree::#conf_tree{} | <a href="#type-conf_tree">conf_tree()</a>) -> [<a href="#type-conf_obj">conf_obj()</a>]</pre>
<br></br>


Converts a configuration tree into an ordered list of configuration objects.<a name="is_list_key-1"></a>

###is_list_key/1##


`is_list_key(I) -> any()`

<a name="join_key-1"></a>

###join_key/1##


<pre>join_key(T::[binary()]) -&gt; binary()</pre>
<br></br>




Creates a kvdb_conf key out of a list of key parts



(See [`split_key/1`](#split_key-1)).

Example: `join_key([<<"a">>, <<"b">>, <<"c">>]) -> <<"a*b*c">>`<a name="join_key-2"></a>

###join_key/2##


`join_key(K, K2) -> any()`

<a name="last-0"></a>

###last/0##


`last() -> any()`

<a name="last-1"></a>

###last/1##


`last(Tab) -> any()`

<a name="last_tree-0"></a>

###last_tree/0##


`last_tree() -> any()`

<a name="last_tree-1"></a>

###last_tree/1##


`last_tree(Tab) -> any()`

<a name="list_key-2"></a>

###list_key/2##


`list_key(Name, Pos) -> any()`

<a name="make_tree-1"></a>

###make_tree/1##


<pre>make_tree(Objs::[<a href="#type-conf_obj">conf_obj()</a>]) -> <a href="#type-conf_tree">conf_tree()</a></pre>
<br></br>


Converts an ordered list of configuration objects into a configuration tree.<a name="next-1"></a>

###next/1##


`next(K) -> any()`

<a name="next-2"></a>

###next/2##


`next(Tab, K) -> any()`

<a name="next_at_level-1"></a>

###next_at_level/1##


`next_at_level(K) -> any()`

<a name="next_at_level-2"></a>

###next_at_level/2##


`next_at_level(Tab, K) -> any()`

<a name="next_tree-1"></a>

###next_tree/1##


`next_tree(K) -> any()`

<a name="next_tree-2"></a>

###next_tree/2##


`next_tree(Tab, K) -> any()`

<a name="open-1"></a>

###open/1##


`open(File) -> any()`

Equivalent to [`open(File, [])`](#open-2).<a name="open-2"></a>

###open/2##


<pre>open(_Filename::undefined | string(), Options::<a href="kvdb.md#type-options">kvdb:options()</a>) -> {ok, pid()} | {error, any()}</pre>
<br></br>




Opens a kvdb_conf-compliant kvdb database.



The kvdb_conf API offers a number of functions geared towards management of
a configuration database. The database itself is a `kvdb` database, so the
`kvdb` API also applies. Specifically, `kvdb_conf` provides the following
functionality on top of `kvdb`:



The default options provided by `kvdb_conf` may be overridden, except for
the key encoding, which must always be `raw`, and the object structure,
which must always include attributes.



If `File == undefined`, either a filename will be picked by the chosen
`kvdb` backend, or - e.g. in case of the `ets` backend - no file will be
used at all. Please refer to the documentation of each backend for
information about their respective strengths and weaknesses.



By default, `kvdb_conf` specifies one table: `<<"data">>`. This table is
mandatory, and used unless another table is specified. Other tables can
be created e.g. in order to use a different value encoding or indexes,
or to separate different data sets.

If a `name` option is given, the database instance can be called something
other than `kvdb_conf`. This is primarily intended for e.g. device
simulators. The database name is handled transparently in the `kvdb_conf`
API, and no facility exists to explicitly choose between different
`kvdb_conf` instances. The name of the current instance is stored in the
process dictionary of the current process, and automatically fetched by
the `kvdb_conf` functions.<a name="options-1"></a>

###options/1##


`options(File) -> any()`

<a name="prefix_match-1"></a>

###prefix_match/1##


<pre>prefix_match(Prefix::<a href="kvdb.md#type-prefix">kvdb:prefix()</a>) -> {[<a href="#type-conf_obj">conf_obj()</a>], <a href="kvdb.md#type-cont">kvdb:cont()</a>}</pre>
<br></br>


Equivalent to [`prefix_matc(<<"data">>, Prefix)`](#prefix_matc-2).<a name="prefix_match-2"></a>

###prefix_match/2##


<pre>prefix_match(Tab::<a href="kvdb.md#type-table">kvdb:table()</a>, Prefix::<a href="kvdb.md#type-prefix">kvdb:prefix()</a>) -> {[<a href="#type-conf_obj">conf_obj()</a>], <a href="kvdb.md#type-cont">kvdb:cont()</a>}</pre>
<br></br>




Performs a prefix match, returning all matching objects.

The difference between this function and [`kvdb:prefix_match/3`](kvdb.md#prefix_match-3) is that
this function automatically ensures that the prefix is escaped using the
`kvdb_conf` escaping rules.<a name="prev-1"></a>

###prev/1##


`prev(K) -> any()`

<a name="prev-2"></a>

###prev/2##


`prev(Tab, K) -> any()`

<a name="read-1"></a>

###read/1##


<pre>read(Key::<a href="#type-key">key()</a>) -> {ok, <a href="#type-conf_obj">conf_obj()</a>} | {error, any()}</pre>
<br></br>


Equivalent to [`read(<<"data">>, Key)`](#read-2).<a name="read-2"></a>

###read/2##


`read(Tab, Key) -> any()`



Reads a configuration object from the database

The returned item is always the single node. See read_tree(Prefix) on how to read an
entire tree structure.<a name="read_tree-1"></a>

###read_tree/1##


<pre>read_tree(Prefix::binary()) -> <a href="#type-conf_tree">conf_tree()</a></pre>
<br></br>




Read a configuration (sub-)tree matching Prefix.

This function does a prefix match on the configuration database, and builds a tree
from the result. The empty binary will result in the whole tree being built.<a name="read_tree-2"></a>

###read_tree/2##


`read_tree(Tab, Prefix) -> any()`

<a name="shift_root-2"></a>

###shift_root/2##


`shift_root(X1, Conf_tree) -> any()`

<a name="split_key-1"></a>

###split_key/1##


<pre>split_key(K::binary()) -&gt; [binary()]</pre>
<br></br>


Splits a `kvdb_conf` key into a list of key parts

Example: `split_key(<<"a*b*c">>) -> [<<"a">>,<<"b">>,<<"c">>].`<a name="store_tree-1"></a>

###store_tree/1##


<pre>store_tree(Tree::<a href="#type-conf_tree">conf_tree()</a>) -> ok</pre>
<br></br>




Store a configuration tree in the database.

Each node in the tree will be stored as a separate object in the database.
<a name="store_tree-2"></a>

###store_tree/2##


`store_tree(Tab, Tree) -> any()`

<a name="unescape_key-1"></a>

###unescape_key/1##


`unescape_key(K) -> any()`

<a name="update_counter-2"></a>

###update_counter/2##


<pre>update_counter(K::<a href="kvdb.md#type-key">kvdb:key()</a>, Incr::integer()) -> integer() | binary()</pre>
<br></br>


Equivalent to [`update_counter(<<"data">>, Key, Incr)`](#update_counter-3).<a name="update_counter-3"></a>

###update_counter/3##


<pre>update_counter(Tab::<a href="kvdb.md#type-table">kvdb:table()</a>, K::<a href="kvdb.md#type-key">kvdb:key()</a>, Incr::integer()) -> integer() | binary()</pre>
<br></br>




Updates a counter with the given increment.



This function can be used to update a counter object (the value part is
assumed to contain the counter value). The counter value can be either
a byte- or bitstring representation of an integer, or a regular integer.
The return value will be the new counter value, of the same type as the
counter itself.

In the case of a byte- or bitstring-encoded counter, the size of the
value is preserved. No overflow check is currently performed.<a name="write-1"></a>

###write/1##


<pre>write(Obj::<a href="#type-conf_obj">conf_obj()</a>) -> ok</pre>
<br></br>


Equivalent to [`write(<<"data">>, Obj)`](#write-2).<a name="write-2"></a>

###write/2##


<pre>write(Tab::<a href="kvdb.md#type-table">kvdb:table()</a>, Obj0::<a href="#type-conf_obj">conf_obj()</a>) -> ok</pre>
<br></br>




Writes a configuration object into the database.



Each node or leaf in the tree is stored as a separate object, so updating
or inserting a node or leaf in the tree is a very cheap operation.

Note that the `kvdb_conf` API only accepts keys of type `binary()`,
even though it allows kvdb_conf tables to select a different key encoding.<a name="write_tree-2"></a>

###write_tree/2##


<pre>write_tree(_Parent::<a href="#type-key">key()</a>, Conf_tree::#conf_tree{}) -> ok</pre>
<br></br>


Writes a configuration tree under the given parent.<a name="write_tree-3"></a>

###write_tree/3##


`write_tree(Table, Parent, Conf_tree) -> any()`

