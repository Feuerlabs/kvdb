

#Module kvdb_lib#
* [Function Index](#index)
* [Function Details](#functions)


__Authors:__ Ulf Wiger ([`ulf@wiger.net`](mailto:ulf@wiger.net)).<a name="index"></a>

##Function Index##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#actual_key-3">actual_key/3</a></td><td></td></tr><tr><td valign="top"><a href="#actual_key-4">actual_key/4</a></td><td></td></tr><tr><td valign="top"><a href="#actual_key-5">actual_key/5</a></td><td></td></tr><tr><td valign="top"><a href="#binary_match-2">binary_match/2</a></td><td></td></tr><tr><td valign="top"><a href="#check_valid_encoding-1">check_valid_encoding/1</a></td><td></td></tr><tr><td valign="top"><a href="#clear_log_thresholds-1">clear_log_thresholds/1</a></td><td></td></tr><tr><td valign="top"><a href="#commit-2">commit/2</a></td><td></td></tr><tr><td valign="top"><a href="#common_open-3">common_open/3</a></td><td></td></tr><tr><td valign="top"><a href="#datetime_to_timestamp-1">datetime_to_timestamp/1</a></td><td></td></tr><tr><td valign="top"><a href="#dec-3">dec/3</a></td><td></td></tr><tr><td valign="top"><a href="#enc-3">enc/3</a></td><td></td></tr><tr><td valign="top"><a href="#enc_prefix-3">enc_prefix/3</a></td><td></td></tr><tr><td valign="top"><a href="#good_string-1">good_string/1</a></td><td>Ensures that a database name doesn't contain weird characters.</td></tr><tr><td valign="top"><a href="#index_vals-4">index_vals/4</a></td><td></td></tr><tr><td valign="top"><a href="#is_prefix-3">is_prefix/3</a></td><td></td></tr><tr><td valign="top"><a href="#log-2">log/2</a></td><td></td></tr><tr><td valign="top"><a href="#log_filename-1">log_filename/1</a></td><td></td></tr><tr><td valign="top"><a href="#nodes_of-2">nodes_of/2</a></td><td></td></tr><tr><td valign="top"><a href="#on_update-4">on_update/4</a></td><td></td></tr><tr><td valign="top"><a href="#open_log-2">open_log/2</a></td><td></td></tr><tr><td valign="top"><a href="#process_log_event-3">process_log_event/3</a></td><td></td></tr><tr><td valign="top"><a href="#purge_logs-2">purge_logs/2</a></td><td></td></tr><tr><td valign="top"><a href="#q_key_to_actual-3">q_key_to_actual/3</a></td><td></td></tr><tr><td valign="top"><a href="#queue_prefix-2">queue_prefix/2</a></td><td></td></tr><tr><td valign="top"><a href="#queue_prefix-3">queue_prefix/3</a></td><td></td></tr><tr><td valign="top"><a href="#replay_logs-3">replay_logs/3</a></td><td></td></tr><tr><td valign="top"><a href="#split_queue_key-2">split_queue_key/2</a></td><td></td></tr><tr><td valign="top"><a href="#split_queue_key-3">split_queue_key/3</a></td><td></td></tr><tr><td valign="top"><a href="#table_name-1">table_name/1</a></td><td></td></tr><tr><td valign="top"><a href="#timestamp-0">timestamp/0</a></td><td></td></tr><tr><td valign="top"><a href="#timestamp-1">timestamp/1</a></td><td></td></tr><tr><td valign="top"><a href="#timestamp_to_datetime-1">timestamp_to_datetime/1</a></td><td></td></tr><tr><td valign="top"><a href="#try_decode-1">try_decode/1</a></td><td></td></tr><tr><td valign="top"><a href="#valid_indexes-1">valid_indexes/1</a></td><td></td></tr><tr><td valign="top"><a href="#valid_table_name-1">valid_table_name/1</a></td><td></td></tr></table>


<a name="functions"></a>

##Function Details##

<a name="actual_key-3"></a>

###actual_key/3##


`actual_key(Enc, Q, Key) -> any()`

<a name="actual_key-4"></a>

###actual_key/4##


`actual_key(Enc, T, Q, Key) -> any()`

<a name="actual_key-5"></a>

###actual_key/5##


`actual_key(Enc, T, Q, TS, Key) -> any()`

<a name="binary_match-2"></a>

###binary_match/2##


`binary_match(A, B) -> any()`

<a name="check_valid_encoding-1"></a>

###check_valid_encoding/1##


`check_valid_encoding(E) -> any()`

<a name="clear_log_thresholds-1"></a>

###clear_log_thresholds/1##


`clear_log_thresholds(Db) -> any()`

<a name="commit-2"></a>

###commit/2##


`commit(Commit, Kvdb_ref) -> any()`

<a name="common_open-3"></a>

###common_open/3##


`common_open(Module, Db, Options) -> any()`

<a name="datetime_to_timestamp-1"></a>

###datetime_to_timestamp/1##


`datetime_to_timestamp(X1) -> any()`

<a name="dec-3"></a>

###dec/3##


`dec(W, X, E) -> any()`

<a name="enc-3"></a>

###enc/3##


`enc(W, X, E) -> any()`

<a name="enc_prefix-3"></a>

###enc_prefix/3##


`enc_prefix(X1, X, X3) -> any()`

<a name="good_string-1"></a>

###good_string/1##


`good_string(Name) -> any()`



Ensures that a database name doesn't contain weird characters

Kvdb database names need to be mapped to filesystem names. This function
produces a formatting of the name that is filesystem-friendly.<a name="index_vals-4"></a>

###index_vals/4##


`index_vals(Ixs, K, Attrs, ValF) -> any()`

<a name="is_prefix-3"></a>

###is_prefix/3##


`is_prefix(Pfx, K, Enc) -> any()`

<a name="log-2"></a>

###log/2##


`log(Db, Data) -> any()`

<a name="log_filename-1"></a>

###log_filename/1##


`log_filename(D) -> any()`

<a name="nodes_of-2"></a>

###nodes_of/2##


`nodes_of(Evt, Db) -> any()`

<a name="on_update-4"></a>

###on_update/4##


`on_update(Event, Kvdb_ref, Table, Info) -> any()`

<a name="open_log-2"></a>

###open_log/2##


`open_log(F, Pid) -> any()`

<a name="process_log_event-3"></a>

###process_log_event/3##


`process_log_event(X1, Mod, Db) -> any()`

<a name="purge_logs-2"></a>

###purge_logs/2##


`purge_logs(Db, TS) -> any()`

<a name="q_key_to_actual-3"></a>

###q_key_to_actual/3##


`q_key_to_actual(Q_key, Enc, Type) -> any()`

<a name="queue_prefix-2"></a>

###queue_prefix/2##


`queue_prefix(Enc, Q) -> any()`

<a name="queue_prefix-3"></a>

###queue_prefix/3##


`queue_prefix(Enc, Q, End) -> any()`

<a name="replay_logs-3"></a>

###replay_logs/3##


`replay_logs(Dir, Module, Db) -> any()`

<a name="split_queue_key-2"></a>

###split_queue_key/2##


`split_queue_key(Enc, Key) -> any()`

<a name="split_queue_key-3"></a>

###split_queue_key/3##


`split_queue_key(Enc, T, Key) -> any()`

<a name="table_name-1"></a>

###table_name/1##


`table_name(Table) -> any()`

<a name="timestamp-0"></a>

###timestamp/0##


`timestamp() -> any()`

<a name="timestamp-1"></a>

###timestamp/1##


`timestamp(X1) -> any()`

<a name="timestamp_to_datetime-1"></a>

###timestamp_to_datetime/1##


`timestamp_to_datetime(TS) -> any()`

<a name="try_decode-1"></a>

###try_decode/1##


`try_decode(V) -> any()`

<a name="valid_indexes-1"></a>

###valid_indexes/1##


`valid_indexes(Ix) -> any()`

<a name="valid_table_name-1"></a>

###valid_table_name/1##


`valid_table_name(Table0) -> any()`

