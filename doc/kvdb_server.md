

#Module kvdb_server#
* [Function Index](#index)
* [Function Details](#functions)


__Behaviours:__ [`gen_server`](gen_server.md).

__Authors:__ Ulf Wiger ([`ulf@wiger.net`](mailto:ulf@wiger.net)).<a name="index"></a>

##Function Index##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#await-1">await/1</a></td><td></td></tr><tr><td valign="top"><a href="#await-2">await/2</a></td><td></td></tr><tr><td valign="top"><a href="#begin_trans-3">begin_trans/3</a></td><td></td></tr><tr><td valign="top"><a href="#call-2">call/2</a></td><td></td></tr><tr><td valign="top"><a href="#cast-2">cast/2</a></td><td></td></tr><tr><td valign="top"><a href="#close-1">close/1</a></td><td></td></tr><tr><td valign="top"><a href="#db-1">db/1</a></td><td></td></tr><tr><td valign="top"><a href="#db-2">db/2</a></td><td></td></tr><tr><td valign="top"><a href="#end_trans-2">end_trans/2</a></td><td></td></tr><tr><td valign="top"><a href="#start_commit-2">start_commit/2</a></td><td></td></tr><tr><td valign="top"><a href="#start_link-2">start_link/2</a></td><td></td></tr><tr><td valign="top"><a href="#start_session-2">start_session/2</a></td><td></td></tr></table>


<a name="functions"></a>

##Function Details##

<a name="await-1"></a>

###await/1##


`await(Name) -> any()`

<a name="await-2"></a>

###await/2##


`await(Name, Timeout) -> any()`

<a name="begin_trans-3"></a>

###begin_trans/3##


`begin_trans(Name, TRef, Db) -> any()`

<a name="call-2"></a>

###call/2##


`call(Name, Req) -> any()`

<a name="cast-2"></a>

###cast/2##


`cast(Name, Msg) -> any()`

<a name="close-1"></a>

###close/1##


`close(Name) -> any()`

<a name="db-1"></a>

###db/1##


`db(Name) -> any()`

<a name="db-2"></a>

###db/2##


`db(Name, TRef) -> any()`

<a name="end_trans-2"></a>

###end_trans/2##


`end_trans(Name, Ref) -> any()`

<a name="start_commit-2"></a>

###start_commit/2##


`start_commit(Name, TRef) -> any()`

<a name="start_link-2"></a>

###start_link/2##


`start_link(Name, Backend) -> any()`

<a name="start_session-2"></a>

###start_session/2##


`start_session(Name, Id) -> any()`

