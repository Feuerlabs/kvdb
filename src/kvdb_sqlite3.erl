%%% @author Tony Rogvall <tony@rogvall.se>
%%% @copyright (C) 2011, Tony Rogvall
%%% @doc
%%%    SQLITE3 backend to kvdb
%%% @end
%%% Created : 29 Dec 2011 by Tony Rogvall <tony@rogvall.se>

-module(kvdb_sqlite3).

-behaviour(kvdb).

-export([open/2, close/1]).
-export([add_table/2, delete_table/2]).
-export([put/4, get/3, delete/3]).
-export([iterator/2, first/2, last/2, next/2, prev/2]).

open(Db, Options) ->
    case proplists:get_value(file, Options) of
	none ->
	    case sqlite3:open(Db, [{file,atom_to_list(Db)++".db"}|Options]) of
		{ok,DbRef} ->
		    {ok,{?MODULE,DbRef}};
		Error ->
		    Error
	    end;
	_ ->
	    case sqlite3:open(Db, Options) of
		{ok,DbRef} ->
		    {ok,{?MODULE,DbRef}};
		Error ->
		    Error
	    end		
    end.

close(Db) ->
    sqlite3:close(Db).

add_table(Db, Table) ->
    case lists:member(Table, sqlite3:list_tables(Db)) of
	true ->
	    ok;
	false ->
	    Columns = [{key, blob, primary_key}, {value, blob}],
	    sqlite3:create_table(Db, Table, Columns)
    end.

delete_table(Db, Table) ->
    sqlite3:drop_table(Db, Table).

-ifdef(__not_defined__).
put(Db, Table, Key, Value) ->
    %% FIXME: make this with SQL: INSERT OR REPLACE INTO <tab> VALUES ( ...)
    case sqlite3:read(Db, Table, {key,{blob,Key}}) of
	[{columns,_},{rows,[{_,{blob,Value}}]}] ->
	    ok;
	[{columns,_},{rows,[{_,{blob,_OldValue}}]}] ->
	    Res = sqlite3:update(Db, Table, {key,{blob,Key}},
				 [{value,{blob,Value}}]),
	    Res;

	[{columns,_},{rows,[]}] ->
	    Res = sqlite3:write(Db, Table, 
				[{key,{blob,Key}},{value,{blob,Value}}]),
	    {rowid,_} = Res,
	    ok;
	Other ->
	    error
    end.
-endif.

put(Db, Table, Key, Value) ->
    insert_or_replace(Db, Table, [{key,{blob,Key}},{value,{blob,Value}}]).

insert_or_replace(Db, Tbl, Data) ->
    SQL = insert_or_replace_sql(Tbl, Data),
    %% io:format("SQL = ~s\n", [SQL]),
    case sqlite3:sql_exec(Db, SQL) of
	{rowid,_} ->
	    ok;
	[{columns,_},{rows,[{_,{blob,_OldValue}}]}] ->
	    ok;
	Error ->
	    Error
    end.
	    
insert_or_replace_sql(Tbl, Data) ->
    {Cols, Values} = lists:unzip(Data),
    ["INSERT OR REPLACE INTO ", atom_to_list(Tbl), " (", 
     sqlite3_lib:write_col_sql(Cols), 
     ") values (",
     sqlite3_lib:write_value_sql(Values), ");"
    ].


get(Db, Table, Key) ->
    case sqlite3:read(Db, Table, {key,{blob,Key}}) of
	[{columns,_},{rows,[{_,{blob,Value}}]}] ->
	    {ok,Value};
	_ ->
	    {error,not_found}
    end.
	    
delete(Db, Table, Key) ->
    sqlite3:delete(Db, Table, {key,{blob,Key}}).
%%
%% iterator is currently first, next
%% but we may create an iterator last,prev by setting ORDER term to DESC
%% FIXME: add finalize!
%%
iterator(Db, Table) ->
    sqlite3:prepare(Db, "SELECT * FROM "++atom_to_list(Table) ++
			" ORDER BY key ASC").

first(Db, Iter) ->
    ok = sqlite3:reset(Db, Iter),
    case sqlite3:next(Db, Iter) of 
	{{blob,Key},{blob,Value}} ->
	    {ok,Key,Value};
	Error ->
	    Error
    end.

last(_Db, _Iter) ->
    error.

next(Db, Iter) ->
    case sqlite3:next(Db, Iter) of 
	{{blob,Key},{blob,Value}} ->
	    {ok,Key,Value};
	Error ->
	    Error
    end.


prev(_Db, _Iter) ->
    error.
