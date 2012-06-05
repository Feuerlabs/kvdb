

#Module kvdb_lib#
* [Function Index](#index)
* [Function Details](#functions)


<a name="index"></a>

##Function Index##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#actual_key-3">actual_key/3</a></td><td></td></tr><tr><td valign="top"><a href="#actual_key-4">actual_key/4</a></td><td></td></tr><tr><td valign="top"><a href="#actual_key-5">actual_key/5</a></td><td></td></tr><tr><td valign="top"><a href="#check_valid_encoding-1">check_valid_encoding/1</a></td><td></td></tr><tr><td valign="top"><a href="#dec-3">dec/3</a></td><td></td></tr><tr><td valign="top"><a href="#enc-3">enc/3</a></td><td></td></tr><tr><td valign="top"><a href="#enc_prefix-3">enc_prefix/3</a></td><td></td></tr><tr><td valign="top"><a href="#good_string-1">good_string/1</a></td><td></td></tr><tr><td valign="top"><a href="#index_vals-4">index_vals/4</a></td><td></td></tr><tr><td valign="top"><a href="#is_prefix-3">is_prefix/3</a></td><td></td></tr><tr><td valign="top"><a href="#queue_prefix-2">queue_prefix/2</a></td><td></td></tr><tr><td valign="top"><a href="#queue_prefix-3">queue_prefix/3</a></td><td></td></tr><tr><td valign="top"><a href="#split_queue_key-2">split_queue_key/2</a></td><td></td></tr><tr><td valign="top"><a href="#split_queue_key-3">split_queue_key/3</a></td><td></td></tr><tr><td valign="top"><a href="#table_name-1">table_name/1</a></td><td></td></tr><tr><td valign="top"><a href="#timestamp-0">timestamp/0</a></td><td></td></tr><tr><td valign="top"><a href="#timestamp-1">timestamp/1</a></td><td></td></tr><tr><td valign="top"><a href="#timestamp_to_datetime-1">timestamp_to_datetime/1</a></td><td></td></tr><tr><td valign="top"><a href="#try_decode-1">try_decode/1</a></td><td></td></tr><tr><td valign="top"><a href="#valid_indexes-1">valid_indexes/1</a></td><td></td></tr><tr><td valign="top"><a href="#valid_table_name-1">valid_table_name/1</a></td><td></td></tr></table>


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

<a name="check_valid_encoding-1"></a>

###check_valid_encoding/1##


`check_valid_encoding(E) -> any()`

<a name="dec-3"></a>

###dec/3##


`dec(X1, X, X3) -> any()`

<a name="enc-3"></a>

###enc/3##


`enc(X1, X, X3) -> any()`

<a name="enc_prefix-3"></a>

###enc_prefix/3##


`enc_prefix(X1, X, X3) -> any()`

<a name="good_string-1"></a>

###good_string/1##


`good_string(Name) -> any()`

<a name="index_vals-4"></a>

###index_vals/4##


`index_vals(Ixs, K, Attrs, ValF) -> any()`

<a name="is_prefix-3"></a>

###is_prefix/3##


`is_prefix(Pfx, K, Enc) -> any()`

<a name="queue_prefix-2"></a>

###queue_prefix/2##


`queue_prefix(Enc, Q) -> any()`

<a name="queue_prefix-3"></a>

###queue_prefix/3##


`queue_prefix(Enc, Q, End) -> any()`

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

