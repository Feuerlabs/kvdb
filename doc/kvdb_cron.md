

#Module kvdb_cron#
* [Function Index](#index)
* [Function Details](#functions)


__Behaviours:__ [`gen_server`](gen_server.md).<a name="index"></a>

##Function Index##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#add-7">add/7</a></td><td></td></tr><tr><td valign="top"><a href="#add-8">add/8</a></td><td></td></tr><tr><td valign="top"><a href="#code_change-3">code_change/3</a></td><td></td></tr><tr><td valign="top"><a href="#create_crontab-2">create_crontab/2</a></td><td></td></tr><tr><td valign="top"><a href="#delete-3">delete/3</a></td><td></td></tr><tr><td valign="top"><a href="#delete-4">delete/4</a></td><td></td></tr><tr><td valign="top"><a href="#delete_abs-3">delete_abs/3</a></td><td></td></tr><tr><td valign="top"><a href="#handle_call-3">handle_call/3</a></td><td></td></tr><tr><td valign="top"><a href="#handle_cast-2">handle_cast/2</a></td><td></td></tr><tr><td valign="top"><a href="#handle_info-2">handle_info/2</a></td><td></td></tr><tr><td valign="top"><a href="#init-1">init/1</a></td><td></td></tr><tr><td valign="top"><a href="#init_meta-1">init_meta/1</a></td><td></td></tr><tr><td valign="top"><a href="#set_timers-1">set_timers/1</a></td><td></td></tr><tr><td valign="top"><a href="#start_link-2">start_link/2</a></td><td></td></tr><tr><td valign="top"><a href="#terminate-2">terminate/2</a></td><td></td></tr><tr><td valign="top"><a href="#testf-0">testf/0</a></td><td></td></tr></table>


<a name="functions"></a>

##Function Details##

<a name="add-7"></a>

###add/7##


<pre>add(Db, Tab, When::<a href="#type-timespec">timespec()</a>, Key::any(), M::atom(), F::atom(), As::[any()]) -> ok | <a href="#type-error">error()</a></pre>
<br></br>


<a name="add-8"></a>

###add/8##


`add(Db, Tab, Q, When, Options, M, F, As) -> any()`

<a name="code_change-3"></a>

###code_change/3##


`code_change(FromVsn, St, Extra) -> any()`

<a name="create_crontab-2"></a>

###create_crontab/2##


`create_crontab(Db, Tab) -> any()`

<a name="delete-3"></a>

###delete/3##


`delete(Db, Tab, Key) -> any()`

<a name="delete-4"></a>

###delete/4##


`delete(Db, Tab, Q, Key) -> any()`

<a name="delete_abs-3"></a>

###delete_abs/3##


`delete_abs(Db, Tab, QK) -> any()`

<a name="handle_call-3"></a>

###handle_call/3##


`handle_call(Req, From, St) -> any()`

<a name="handle_cast-2"></a>

###handle_cast/2##


`handle_cast(Msg, St) -> any()`

<a name="handle_info-2"></a>

###handle_info/2##


`handle_info(Msg, St) -> any()`

<a name="init-1"></a>

###init/1##


`init(X1) -> any()`

<a name="init_meta-1"></a>

###init_meta/1##


`init_meta(Db) -> any()`

<a name="set_timers-1"></a>

###set_timers/1##


`set_timers(Db) -> any()`

<a name="start_link-2"></a>

###start_link/2##


`start_link(Db, Options) -> any()`

<a name="terminate-2"></a>

###terminate/2##


`terminate(Reason, St) -> any()`

<a name="testf-0"></a>

###testf/0##


`testf() -> any()`

