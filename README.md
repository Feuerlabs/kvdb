

# The kvdb application #

__Authors:__ Ulf Wiger ([`ulf@feuerlabs.com`](mailto:ulf@feuerlabs.com)), Tony Rogvall ([`tony@rogvall.se`](mailto:tony@rogvall.se)).

KVDB - Database Management System for Connected Device Management

KVDB was initially designed to support the requirements of the Exosense
system for managing Connected Devices, but is essentially a general-purpose
DBMS. A requirement was that it should be useable both in an embedded device
and on the device management server. To this end, KVDB supports a number of
different storage backends, each with different characteristics.

Features:

* Ordered-set semantics

* Transaction semantics

* Persistent queues

* CRON-like persistent timers

* Extensible indexing

* Storage backend plugins

* Schema callback behavior

See also the [Wiki](http://github.com/Feuerlabs/kvdb/wiki) for further description and examples.


## Modules ##


<table width="100%" border="0" summary="list of modules">
<tr><td><a href="http://github.com/Feuerlabs/kvdb/blob/master/doc/kvdb.md" class="module">kvdb</a></td></tr>
<tr><td><a href="http://github.com/Feuerlabs/kvdb/blob/master/doc/kvdb_conf.md" class="module">kvdb_conf</a></td></tr>
<tr><td><a href="http://github.com/Feuerlabs/kvdb/blob/master/doc/kvdb_cron.md" class="module">kvdb_cron</a></td></tr>
<tr><td><a href="http://github.com/Feuerlabs/kvdb/blob/master/doc/kvdb_cron_parse.md" class="module">kvdb_cron_parse</a></td></tr>
<tr><td><a href="http://github.com/Feuerlabs/kvdb/blob/master/doc/kvdb_cron_scan.md" class="module">kvdb_cron_scan</a></td></tr>
<tr><td><a href="http://github.com/Feuerlabs/kvdb/blob/master/doc/kvdb_ets_dumper.md" class="module">kvdb_ets_dumper</a></td></tr>
<tr><td><a href="http://github.com/Feuerlabs/kvdb/blob/master/doc/kvdb_export.md" class="module">kvdb_export</a></td></tr>
<tr><td><a href="http://github.com/Feuerlabs/kvdb/blob/master/doc/kvdb_paired.md" class="module">kvdb_paired</a></td></tr>
<tr><td><a href="http://github.com/Feuerlabs/kvdb/blob/master/doc/kvdb_proxy_sup.md" class="module">kvdb_proxy_sup</a></td></tr>
<tr><td><a href="http://github.com/Feuerlabs/kvdb/blob/master/doc/kvdb_queue.md" class="module">kvdb_queue</a></td></tr>
<tr><td><a href="http://github.com/Feuerlabs/kvdb/blob/master/doc/kvdb_riak.md" class="module">kvdb_riak</a></td></tr>
<tr><td><a href="http://github.com/Feuerlabs/kvdb/blob/master/doc/kvdb_riak_mapred.md" class="module">kvdb_riak_mapred</a></td></tr>
<tr><td><a href="http://github.com/Feuerlabs/kvdb/blob/master/doc/kvdb_riak_proxy.md" class="module">kvdb_riak_proxy</a></td></tr>
<tr><td><a href="http://github.com/Feuerlabs/kvdb/blob/master/doc/kvdb_schema.md" class="module">kvdb_schema</a></td></tr>
<tr><td><a href="http://github.com/Feuerlabs/kvdb/blob/master/doc/kvdb_schema_events.md" class="module">kvdb_schema_events</a></td></tr></table>

