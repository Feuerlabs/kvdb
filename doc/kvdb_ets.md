

#Module kvdb_ets#
* [Description](#description)
* [Function Index](#index)
* [Function Details](#functions)



ETS backend to kvdb.



Copyright (c) (C) 2011, Tony Rogvall

__Behaviours:__ [`kvdb`](kvdb.md).

__Authors:__ Tony Rogvall ([`tony@rogvall.se`](mailto:tony@rogvall.se)).<a name="index"></a>

##Function Index##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#add_table-2">add_table/2</a></td><td></td></tr><tr><td valign="top"><a href="#close-1">close/1</a></td><td></td></tr><tr><td valign="top"><a href="#delete-3">delete/3</a></td><td></td></tr><tr><td valign="top"><a href="#delete_table-2">delete_table/2</a></td><td></td></tr><tr><td valign="top"><a href="#first-2">first/2</a></td><td></td></tr><tr><td valign="top"><a href="#get-3">get/3</a></td><td></td></tr><tr><td valign="top"><a href="#iterator-2">iterator/2</a></td><td></td></tr><tr><td valign="top"><a href="#iterator_close-2">iterator_close/2</a></td><td></td></tr><tr><td valign="top"><a href="#last-2">last/2</a></td><td></td></tr><tr><td valign="top"><a href="#next-2">next/2</a></td><td></td></tr><tr><td valign="top"><a href="#open-2">open/2</a></td><td></td></tr><tr><td valign="top"><a href="#prev-2">prev/2</a></td><td></td></tr><tr><td valign="top"><a href="#put-4">put/4</a></td><td></td></tr></table>


<a name="functions"></a>

##Function Details##

<a name="add_table-2"></a>

###add_table/2##




`add_table(Db, Table) -> any()`

<a name="close-1"></a>

###close/1##




`close(Db) -> any()`

<a name="delete-3"></a>

###delete/3##




`delete(Db, Table, Key) -> any()`

<a name="delete_table-2"></a>

###delete_table/2##




`delete_table(Db, Table) -> any()`

<a name="first-2"></a>

###first/2##




`first(Db, Iter) -> any()`

<a name="get-3"></a>

###get/3##




`get(Db, Table, Key) -> any()`

<a name="iterator-2"></a>

###iterator/2##




`iterator(Db, Table) -> any()`

<a name="iterator_close-2"></a>

###iterator_close/2##




`iterator_close(Db, Iter) -> any()`

<a name="last-2"></a>

###last/2##




`last(Db, Iter) -> any()`

<a name="next-2"></a>

###next/2##




`next(Db, Iter) -> any()`

<a name="open-2"></a>

###open/2##




`open(Db, Options) -> any()`

<a name="prev-2"></a>

###prev/2##




`prev(Db, Iter) -> any()`

<a name="put-4"></a>

###put/4##




`put(Db, Table, Key, Value) -> any()`

