

# Module kvdb_schema #
* [Description](#description)
* [Function Index](#index)
* [Function Details](#functions)



KVDB schema callback module behavior.
__This module defines the `kvdb_schema` behaviour.__
<br></br>
 Required callback functions: `validate/3`, `on_update/4`, `pre_commit/2`, `post_commit/2`.

__Authors:__ Ulf Wiger ([`ulf@feuerlabs.com`](mailto:ulf@feuerlabs.com)).
<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#behaviour_info-1">behaviour_info/1</a></td><td></td></tr><tr><td valign="top"><a href="#on_update-4">on_update/4</a></td><td></td></tr><tr><td valign="top"><a href="#post_commit-2">post_commit/2</a></td><td></td></tr><tr><td valign="top"><a href="#pre_commit-2">pre_commit/2</a></td><td></td></tr><tr><td valign="top"><a href="#read-1">read/1</a></td><td></td></tr><tr><td valign="top"><a href="#read-2">read/2</a></td><td></td></tr><tr><td valign="top"><a href="#validate-3">validate/3</a></td><td></td></tr><tr><td valign="top"><a href="#write-2">write/2</a></td><td></td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="behaviour_info-1"></a>

### behaviour_info/1 ###

`behaviour_info(X1) -> any()`


<a name="on_update-4"></a>

### on_update/4 ###

`on_update(Op, Db, Table, Obj) -> any()`


<a name="post_commit-2"></a>

### post_commit/2 ###

`post_commit(X1, X2) -> any()`


<a name="pre_commit-2"></a>

### pre_commit/2 ###

`pre_commit(C, X2) -> any()`


<a name="read-1"></a>

### read/1 ###

`read(Db) -> any()`


<a name="read-2"></a>

### read/2 ###

`read(Db, Item) -> any()`


<a name="validate-3"></a>

### validate/3 ###

`validate(Db, Type, Obj) -> any()`


<a name="write-2"></a>

### write/2 ###

`write(Db, Schema) -> any()`


