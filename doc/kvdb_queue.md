

#Module kvdb_queue#
* [Function Index](#index)
* [Function Details](#functions)


__Authors:__ Ulf Wiger ([`ulf@wiger.net`](mailto:ulf@wiger.net)).<a name="index"></a>

##Function Index##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#clear_queue-3">clear_queue/3</a></td><td></td></tr><tr><td valign="top"><a href="#delete-3">delete/3</a></td><td></td></tr><tr><td valign="top"><a href="#extract-3">extract/3</a></td><td></td></tr><tr><td valign="top"><a href="#first-2">first/2</a></td><td></td></tr><tr><td valign="top"><a href="#is_empty-3">is_empty/3</a></td><td></td></tr><tr><td valign="top"><a href="#list-3">list/3</a></td><td></td></tr><tr><td valign="top"><a href="#list-6">list/6</a></td><td></td></tr><tr><td valign="top"><a href="#list_full-3">list_full/3</a></td><td></td></tr><tr><td valign="top"><a href="#next-3">next/3</a></td><td></td></tr><tr><td valign="top"><a href="#pop-2">pop/2</a></td><td></td></tr><tr><td valign="top"><a href="#pop-3">pop/3</a></td><td></td></tr><tr><td valign="top"><a href="#prel_pop-2">prel_pop/2</a></td><td></td></tr><tr><td valign="top"><a href="#prel_pop-3">prel_pop/3</a></td><td></td></tr><tr><td valign="top"><a href="#push-3">push/3</a></td><td></td></tr><tr><td valign="top"><a href="#push-4">push/4</a></td><td></td></tr></table>


<a name="functions"></a>

##Function Details##

<a name="clear_queue-3"></a>

###clear_queue/3##


`clear_queue(Db, Table, Q) -> any()`

<a name="delete-3"></a>

###delete/3##


`delete(Db, Table, Key) -> any()`

<a name="extract-3"></a>

###extract/3##


`extract(Db, Table, Key) -> any()`

<a name="first-2"></a>

###first/2##


`first(Db, Table) -> any()`

<a name="is_empty-3"></a>

###is_empty/3##


`is_empty(Db, Table, Q) -> any()`

<a name="list-3"></a>

###list/3##


`list(Db, Table, Q) -> any()`

<a name="list-6"></a>

###list/6##


`list(Db, Tab, Q, Fltr, HeedBlock, Limit) -> any()`

<a name="list_full-3"></a>

###list_full/3##


`list_full(Db, Tab, Q) -> any()`

<a name="next-3"></a>

###next/3##


`next(Db, Table, PrevQ) -> any()`

<a name="pop-2"></a>

###pop/2##


`pop(Db, Table) -> any()`

<a name="pop-3"></a>

###pop/3##


`pop(Db, Table, Q) -> any()`

<a name="prel_pop-2"></a>

###prel_pop/2##


`prel_pop(Db, Table) -> any()`

<a name="prel_pop-3"></a>

###prel_pop/3##


`prel_pop(Db, Table, Q) -> any()`

<a name="push-3"></a>

###push/3##


`push(Db, Table, Obj) -> any()`

<a name="push-4"></a>

###push/4##


`push(Db, Table, Q, Obj) -> any()`

