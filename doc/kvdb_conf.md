

#Module kvdb_conf#
* [Description](#description)
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)


  
API to store NETCONF-style config data in kvdb.

<a name="description"></a>

##Description##




identifiers are stored as a structured key (binary)  
delimited by:



Example:
`{system,[{services, [{ssh,[...]}]}]}` would be stored as



{"system", []}  
{"system*services", []}  
{"system*services*ssh", []}

Netconf identifiers can consist of alphanumerics, '-', '_' or '.'. The '*' as delimiter
is chosen so that a "wildcard" character can be used that is greater than the delimiter,
but smaller than any identifier character.
<a name="types"></a>

##Data Types##




###<a name="type-attrs">attrs()</a>##



<pre>attrs() = [{atom(), any()}]</pre>



###<a name="type-conf_node">conf_node()</a>##



<pre>conf_node() = {<a href="#type-key">key()</a>, <a href="#type-attrs">attrs()</a>, <a href="#type-data">data()</a>, <a href="#type-conf_tree">conf_tree()</a>}</pre>



###<a name="type-conf_obj">conf_obj()</a>##



<pre>conf_obj() = {<a href="#type-key">key()</a>, <a href="#type-attrs">attrs()</a>, <a href="#type-data">data()</a>}</pre>



###<a name="type-conf_tree">conf_tree()</a>##



<pre>conf_tree() = [<a href="#type-conf_node">conf_node()</a> | <a href="#type-conf_obj">conf_obj()</a>]</pre>



###<a name="type-data">data()</a>##



<pre>data() = binary()</pre>



###<a name="type-key">key()</a>##



<pre>key() = binary()</pre>
<a name="index"></a>

##Function Index##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#all-0">all/0</a></td><td></td></tr><tr><td valign="top"><a href="#close-0">close/0</a></td><td></td></tr><tr><td valign="top"><a href="#delete-1">delete/1</a></td><td></td></tr><tr><td valign="top"><a href="#delete_all-1">delete_all/1</a></td><td></td></tr><tr><td valign="top"><a href="#first-0">first/0</a></td><td></td></tr><tr><td valign="top"><a href="#first_tree-0">first_tree/0</a></td><td></td></tr><tr><td valign="top"><a href="#flatten_tree-1">flatten_tree/1</a></td><td>Converts a configuration tree into an ordered list of configuration objects.</td></tr><tr><td valign="top"><a href="#last-0">last/0</a></td><td></td></tr><tr><td valign="top"><a href="#last_tree-0">last_tree/0</a></td><td></td></tr><tr><td valign="top"><a href="#make_tree-1">make_tree/1</a></td><td>Converts an ordered list of configuration objects into a configuration tree.</td></tr><tr><td valign="top"><a href="#next-1">next/1</a></td><td></td></tr><tr><td valign="top"><a href="#next_at_level-1">next_at_level/1</a></td><td></td></tr><tr><td valign="top"><a href="#next_tree-1">next_tree/1</a></td><td></td></tr><tr><td valign="top"><a href="#open-1">open/1</a></td><td></td></tr><tr><td valign="top"><a href="#open-2">open/2</a></td><td></td></tr><tr><td valign="top"><a href="#options-1">options/1</a></td><td></td></tr><tr><td valign="top"><a href="#prev-1">prev/1</a></td><td></td></tr><tr><td valign="top"><a href="#read-1">read/1</a></td><td>Reads a configuration object from the database.</td></tr><tr><td valign="top"><a href="#read_tree-1">read_tree/1</a></td><td>Read a configuration (sub-)tree matching Prefix.</td></tr><tr><td valign="top"><a href="#store_tree-1">store_tree/1</a></td><td>Store a configuration tree in the database.</td></tr><tr><td valign="top"><a href="#write-1">write/1</a></td><td>Writes a configuration object into the database.</td></tr></table>


<a name="functions"></a>

##Function Details##

<a name="all-0"></a>

###all/0##




`all() -> any()`

<a name="close-0"></a>

###close/0##




`close() -> any()`

<a name="delete-1"></a>

###delete/1##




`delete(K) -> any()`

<a name="delete_all-1"></a>

###delete_all/1##




`delete_all(Prefix) -> any()`

<a name="first-0"></a>

###first/0##




`first() -> any()`

<a name="first_tree-0"></a>

###first_tree/0##




`first_tree() -> any()`

<a name="flatten_tree-1"></a>

###flatten_tree/1##




<pre>flatten_tree(Tree::<a href="#type-conf_tree">conf_tree()</a>) -> [<a href="#type-conf_obj">conf_obj()</a>]</pre>
<br></br>




Converts a configuration tree into an ordered list of configuration objects.<a name="last-0"></a>

###last/0##




`last() -> any()`

<a name="last_tree-0"></a>

###last_tree/0##




`last_tree() -> any()`

<a name="make_tree-1"></a>

###make_tree/1##




<pre>make_tree(Objs::[<a href="#type-conf_obj">conf_obj()</a>]) -> <a href="#type-conf_tree">conf_tree()</a></pre>
<br></br>




Converts an ordered list of configuration objects into a configuration tree.<a name="next-1"></a>

###next/1##




`next(K) -> any()`

<a name="next_at_level-1"></a>

###next_at_level/1##




`next_at_level(K) -> any()`

<a name="next_tree-1"></a>

###next_tree/1##




`next_tree(K) -> any()`

<a name="open-1"></a>

###open/1##




`open(File) -> any()`

<a name="open-2"></a>

###open/2##




`open(File, Options) -> any()`

<a name="options-1"></a>

###options/1##




`options(File) -> any()`

<a name="prev-1"></a>

###prev/1##




`prev(K) -> any()`

<a name="read-1"></a>

###read/1##




<pre>read(Key::<a href="#type-key">key()</a>) -> {ok, <a href="#type-conf_obj">conf_obj()</a>} | {error, any()}</pre>
<br></br>






Reads a configuration object from the database

The returned item is always the single node. See read_tree(Prefix) on how to read an
entire tree structure.<a name="read_tree-1"></a>

###read_tree/1##




<pre>read_tree(Prefix::binary()) -> <a href="#type-conf_tree">conf_tree()</a></pre>
<br></br>






Read a configuration (sub-)tree matching Prefix.

This function does a prefix match on the configuration database, and builds a tree
from the result. The empty binary will result in the whole tree being built.<a name="store_tree-1"></a>

###store_tree/1##




<pre>store_tree(Tree::<a href="#type-conf_tree">conf_tree()</a>) -> ok</pre>
<br></br>






Store a configuration tree in the database.

Each node in the tree will be stored as a separate object in the database.
<a name="write-1"></a>

###write/1##




<pre>write(Obj::<a href="#type-conf_obj">conf_obj()</a>) -> ok</pre>
<br></br>






Writes a configuration object into the database.

Each node or leaf in the tree is stored as a separate object, so updating or inserting
a node or leaf in the tree is a very cheap operation.