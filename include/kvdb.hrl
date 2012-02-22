%% kvdb definitions

-type table() :: atom() | binary().

-record(kvdb_ref, {name, db, mod, schema = db}).
-opaque db_ref()  :: #kvdb_ref{}.

-record(db, {ref, encoding = sext, metadata}).
-record(table, {name, type = set, encoding = sext, columns, indexes = []}).

-opaque db() :: #db{}.

-define(SCHEMA_TABLE, <<"kvdb__SCHEMA">>).

-type name() :: any().
-type key() :: any().
-type value() :: any().
-type attr_name() :: atom().
-type attr_value() :: any().
-type attrs() :: [{atom(), any()}].
-type options() :: [{atom(), any()}].

-type object() :: {key(), value()} | {key(), attrs(), value()}.
