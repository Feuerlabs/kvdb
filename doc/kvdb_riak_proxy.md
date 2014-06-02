

# Module kvdb_riak_proxy #
* [Description](#description)
* [Function Index](#index)
* [Function Details](#functions)



Riak connectivity manager (proxy) for kvdb_riak backend.
__Behaviours:__ [`gen_server`](gen_server.md).

__Authors:__ Ulf Wiger ([`ulf@feuerlabs.com`](mailto:ulf@feuerlabs.com)).
<a name="description"></a>

## Description ##


NOTE: This is work in progress, highly experimental.
<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#code_change-3">code_change/3</a></td><td></td></tr><tr><td valign="top"><a href="#get_session-1">get_session/1</a></td><td></td></tr><tr><td valign="top"><a href="#handle_call-3">handle_call/3</a></td><td></td></tr><tr><td valign="top"><a href="#handle_cast-2">handle_cast/2</a></td><td></td></tr><tr><td valign="top"><a href="#handle_info-2">handle_info/2</a></td><td></td></tr><tr><td valign="top"><a href="#init-1">init/1</a></td><td></td></tr><tr><td valign="top"><a href="#start_link-2">start_link/2</a></td><td></td></tr><tr><td valign="top"><a href="#terminate-2">terminate/2</a></td><td></td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="code_change-3"></a>

### code_change/3 ###

`code_change(X1, S, X3) -> any()`


<a name="get_session-1"></a>

### get_session/1 ###

`get_session(Name) -> any()`


<a name="handle_call-3"></a>

### handle_call/3 ###

`handle_call(X1, X2, St) -> any()`


<a name="handle_cast-2"></a>

### handle_cast/2 ###

`handle_cast(X1, S) -> any()`


<a name="handle_info-2"></a>

### handle_info/2 ###

`handle_info(X1, S) -> any()`


<a name="init-1"></a>

### init/1 ###

`init(X1) -> any()`


<a name="start_link-2"></a>

### start_link/2 ###

`start_link(Name, Options) -> any()`


<a name="terminate-2"></a>

### terminate/2 ###

`terminate(X1, X2) -> any()`


