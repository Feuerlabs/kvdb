

# Module kvdb_proxy_sup #
* [Description](#description)
* [Function Index](#index)
* [Function Details](#functions)



Database proxy supervisor.
__Behaviours:__ [`supervisor`](supervisor.md).

__Authors:__ Ulf Wiger ([`ulf@feuerlabs.com`](mailto:ulf@feuerlabs.com)).
<a name="description"></a>

## Description ##


NOTE: This is work in progress, highly experimental.
The general idea is that a backend can specify 'proxy processes'
as a list of childspecs. An example of such a proxy is kvdb_riak_proxy.
<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#init-1">init/1</a></td><td></td></tr><tr><td valign="top"><a href="#start_link-2">start_link/2</a></td><td></td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="init-1"></a>

### init/1 ###

`init(X1) -> any()`


<a name="start_link-2"></a>

### start_link/2 ###

`start_link(Name, Options) -> any()`


