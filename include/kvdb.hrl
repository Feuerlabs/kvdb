%% kvdb definitions

-type table() :: atom() | binary().
-type int_table_name() :: binary().
-type queue_name() :: any().

-record(kvdb_ref, {name, tref, db, mod, schema = db}).
-opaque db_ref()  :: #kvdb_ref{}.

-record(db, {ref, encoding = sext, metadata, log = false, st}).
-record(table, {name,
		type = set,
		encoding = sext,
		columns,
		schema,
		index = []}).

-record(event, {event, tab, info}).

-record(dbst, {encoding,
	       type}).

-opaque db() :: #db{}.

-define(SCHEMA_TABLE, <<"kvdb__SCHEMA">>).

-type db_name() :: any().
-type key() :: any().
-type value() :: any().
-type attr_name() :: atom().
-type attr_value() :: any().
-type attrs() :: [{atom(), any()}].
-type options() :: [{atom(), any()}].
-type basic_encoding() :: sext | raw | term.
-type encoding() :: basic_encoding() |
		    {basic_encoding(), basic_encoding()} |
		    {basic_encoding(), basic_encoding(), basic_encoding()}.

-type object() :: {key(), value()} | {key(), attrs(), value()}.
-type increment() :: integer().
-type status() :: active | inactive | blocking.

-record(commit, {add_tables = [],
		 del_tables = [],
		 write = [],
		 delete = [],
		 events = []}).

-record(thr, {writes,
	      bytes,
	      time}).

-record(q_key, {queue,
		ts,
		key}).

-define(KVDB_CATCH(Expr, Args),
	try Expr
	catch
	    throw:{kvdb_return, __R} ->
		__R;
	    throw:{kvdb_throw, __E} ->
		%% error(__E, Args)
		error(__E, erlang:get_stacktrace())
	end).

-define(KVDB_THROW(E), throw({kvdb_throw, E})).
-define(KVDB_RETURN(R), throw({kvdb_return, R})).

-define(KVDB_LOG_INSERT(Tab, Obj), {insert, Tab, Obj}).
-define(KVDB_LOG_DELETE(Tab, Key), {delete, Tab, Key}).
-define(KVDB_LOG_Q_INSERT(Tab, QKey, St, Obj),
	{q_insert, Tab, QKey, St, Obj}).
-define(KVDB_LOG_Q_DELETE(Tab, QKey), {q_delete, Tab, QKey}).
-define(KVDB_LOG_ADD_TABLE(Tab, TabR), {add_table, Tab, TabR}).
-define(KVDB_LOG_DELETE_TABLE(Tab), {delete_table, Tab}).
-define(KVDB_LOG_COMMIT(CommitRec), {commit, CommitRec}).
