

# Module kvdb_ets_dumper #
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)



<a name="types"></a>

## Data Types ##




### <a name="type-tab">tab()</a> ###



<pre><code>
tab() = atom() | <a href="#type-tid">tid()</a>
</code></pre>



  a similar definition is also in erl_types



### <a name="type-tid">tid()</a> ###


__abstract datatype__: `tid()`

<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#file2tab-1">file2tab/1</a></td><td></td></tr><tr><td valign="top"><a href="#file2tab-2">file2tab/2</a></td><td></td></tr><tr><td valign="top"><a href="#tab2file-2">tab2file/2</a></td><td></td></tr><tr><td valign="top"><a href="#tab2file-3">tab2file/3</a></td><td></td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="file2tab-1"></a>

### file2tab/1 ###


<pre><code>
file2tab(Filename) -&gt; {ok, Tab} | {error, Reason}
</code></pre>

<ul class="definitions"><li><code>Filename = <a href="file.md#type-name">file:name()</a></code></li><li><code>Tab = <a href="#type-tab">tab()</a></code></li><li><code>Reason = term()</code></li></ul>


<a name="file2tab-2"></a>

### file2tab/2 ###


<pre><code>
file2tab(Filename, Options) -&gt; {ok, Tab} | {error, Reason}
</code></pre>

<ul class="definitions"><li><code>Filename = <a href="file.md#type-name">file:name()</a></code></li><li><code>Tab = <a href="#type-tab">tab()</a></code></li><li><code>Options = [Option]</code></li><li><code>Option = {verify, boolean()}</code></li><li><code>Reason = term()</code></li></ul>


<a name="tab2file-2"></a>

### tab2file/2 ###


<pre><code>
tab2file(Tab, Filename) -&gt; ok | {error, Reason}
</code></pre>

<ul class="definitions"><li><code>Tab = <a href="#type-tab">tab()</a></code></li><li><code>Filename = <a href="file.md#type-name">file:name()</a></code></li><li><code>Reason = term()</code></li></ul>


<a name="tab2file-3"></a>

### tab2file/3 ###


<pre><code>
tab2file(Tab, Filename, Options) -&gt; ok | {error, Reason}
</code></pre>

<ul class="definitions"><li><code>Tab = <a href="#type-tab">tab()</a></code></li><li><code>Filename = <a href="file.md#type-name">file:name()</a></code></li><li><code>Options = [Option]</code></li><li><code>Option = {extended_info, [ExtInfo]}</code></li><li><code>ExtInfo = md5sum | object_count</code></li><li><code>Reason = term()</code></li></ul>


