%%% @author Tony Rogvall <tony@rogvall.se>
%%% @copyright (C) 2011, Tony Rogvall
%%% @doc
%%%    SQLITE3 backend to kvdb
%%% @end
%%% Created : 29 Dec 2011 by Tony Rogvall <tony@rogvall.se>
%%% Hacked further by Ulf Wiger <ulf@feuerlabs.com>

-module(kvdb_sqlite3).

-behaviour(kvdb).

-export([open/2, close/1]).
-export([add_table/2, delete_table/2, list_tables/1]).
-export([put/3, get/3, delete/3]).
-export([iterator/2, first/2, last/2, next/3, prev/3]).
-export([prefix_match/3, prefix_match/4]).
-export([info/2]).

-import(kvdb_lib, [enc/3, dec/3, enc_prefix/3]).

-record(sqlite3_iter, {table, ref, db}).
-record(db, {ref, encoding}).


info(#db{ref = Db}, ref) -> Db;
info(#db{encoding = Enc}, encoding) -> Enc;
info(#db{}, _) -> undefined.

open(Db, Options) ->
    Res = case proplists:get_value(file, Options) of
	      none ->
		  sqlite3:open(Db, [{file,atom_to_list(Db)++".db"}|Options]);
	      _ ->
		  sqlite3:open(Db, Options)
	  end,
    case Res of
	{ok, Instance} ->
	    {ok, #db{ref = Instance,
		     encoding = proplists:get_value(encoding, Options, raw)}};
	{error,_} = Error ->
	    Error
    end.

close(#db{ref = Db}) ->
    sqlite3:close(Db).

add_table(#db{ref = Db, encoding = Enc}, Table) ->
    case lists:member(Table, sqlite3:list_tables(Db)) of
	true ->
	    ok;
	false ->
	    Columns = case Enc of
			  {_, _, _} ->
			      [{key, blob, primary_key}, {attrs, blob}, {value, blob}];
			  _ ->
			      [{key, blob, primary_key}, {value, blob}]
		      end,
	    sqlite3:create_table(Db, Table, Columns)
    end.

list_tables(#db{ref = Db}) ->
    sqlite3:list_tables(Db).

delete_table(#db{ref = Db}, Table) ->
    sqlite3:drop_table(Db, Table).

put(#db{ref = Db, encoding = Enc}, Table, {Key, Value}) ->
    insert_or_replace(Db, Table, [{key, {blob, enc(key, Key, Enc)}},
				  {value, {blob, enc(value, Value, Enc)}}]);
put(#db{ref = Db, encoding = Enc}, Table, {Key, Attrs, Value}) ->
    insert_or_replace(Db, Table, [{key, {blob, enc(key, Key, Enc)}},
				  {attrs, {blob, enc(attrs, Attrs, Enc)}},
				  {value, {blob, enc(value, Value, Enc)}}]).

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


get(#db{ref = Db, encoding = Enc}, Table, Key) ->
    case sqlite3:read(Db, Table, {key, {blob, enc(key, Key, Enc)}}) of
	[{columns,_},{rows,[{_,{blob,Value}}]}] ->
	    {ok, {Key, dec(value, Value, Enc)}};
	[{columns,_},{rows,[{_,{blob,Attrs},{blob,Value}}]}] ->
	    {ok, {Key, dec(attrs, Attrs, Enc), dec(value, Value, Enc)}};
	_ ->
	    {error,not_found}
    end.

delete(#db{ref = Db, encoding = Enc}, Table, Key) ->
    sqlite3:delete(Db, Table, {key,{blob, enc(key, Key, Enc)}}).


prefix_match(Db, Table, Prefix) ->
    prefix_match(Db, Table, Prefix, 100).

prefix_match(#db{ref = Db, encoding = Enc}, Table, Prefix, Limit)
  when (is_integer(Limit) orelse Limit==infinity)
       andalso is_atom(Table) ->
    {ok, Ref} = sqlite3:prepare(Db, ["SELECT * FROM ", atom_to_list(Table),
				     " WHERE key >= ?"
				     " ORDER BY key ASC"]),
    EncPrefix = enc_prefix(key, Prefix, Enc),
    ok = sqlite3:bind(Db, Ref, [{blob, EncPrefix}]),
    prefix_match_(Db, Ref, EncPrefix, Enc, Limit, Limit, []).

prefix_match_(Db, Ref, Pfx, Enc, 0, Limit0, Acc) ->
    {lists:reverse(Acc), fun() ->
				 prefix_match_(Db, Ref, Pfx, Enc, Limit0, Limit0, [])
			 end};
prefix_match_(Db, Ref, Pfx, Enc, Limit, Limit0, Acc) ->
    case sqlite3:next(Db, Ref) of
	done ->
	    sqlite3:finalize(Db, Ref),
	    {lists:reverse(Acc), fun() -> done end};
	Other ->
	    {blob,K} = element(1, Other),
	    case is_prefix(Pfx, K) of
		true ->
		    NewAcc = [decode_obj(Other, Enc) | Acc],
		    prefix_match_(Db, Ref, Pfx, Enc, decr(Limit), Limit0, NewAcc);
		false ->
		    {lists:reverse(Acc), fun() -> done end}
	    end
    end.

is_prefix(Pfx, Key) ->
    Sz = byte_size(Pfx),
    case Key of
	<< Pfx:Sz/binary, _/binary >> ->
	    true;
	_ ->
	    false
    end.

decode_obj({{blob,K},{blob,V}}, Enc) ->
    {dec(key,K,Enc), dec(value,V,Enc)};
decode_obj({{blob,K},{blob,As},{blob,V}}, Enc) ->
    {dec(key,K,Enc), dec(attrs, As, Enc), dec(value,V,Enc)}.

decr(infinity) ->
    infinity;
decr(N) when is_integer(N), N > 0 ->
    N - 1.

%%
%% iterator is currently first, next
%% but we may create an iterator last,prev by setting ORDER term to DESC
%% FIXME: add finalize!
%%
iterator(#db{ref = Db} = DbRec, Table) ->
    case sqlite3:prepare(Db, "SELECT * FROM "++atom_to_list(Table) ++
			     " ORDER BY key ASC") of
	{ok, Ref} ->
	    {ok, #sqlite3_iter{table = Table, ref = Ref, db = DbRec}};
	Other ->
	    Other
    end.

first(#db{ref = Db, encoding = Enc}, Table) ->
    select_one(Db, Enc, ["SELECT * FROM ", atom_to_list(Table),
			 " ORDER BY key ASC LIMIT 1"]).

last(#db{ref = Db, encoding = Enc}, Table) ->
    select_one(Db, Enc, ["SELECT * FROM ", atom_to_list(Table),
			 " ORDER BY key DESC LIMIT 1"]).

next(#db{ref = Db, encoding = Enc}, Table, Key) ->
    select_one(Db, Enc, ["SELECT * FROM ", atom_to_list(Table),
			 " WHERE key > ?"
			 " ORDER BY key ASC LIMIT 1"], [{blob, enc(key, Key, Enc)}]).

prev(#db{ref = Db, encoding = Enc}, Table, Key) ->
    select_one(Db, Enc, ["SELECT * FROM ", atom_to_list(Table),
			 " WHERE key < ?"
			 " ORDER BY key DESC LIMIT 1"], [{blob, enc(key, Key, Enc)}]).

select_one(Db, Enc, SQL) ->
    select_one(Db, Enc, SQL, []).

select_one(Db, Enc, SQL, Params) ->
    case sqlite3:sql_exec(Db, SQL, Params) of
	[{columns, _},
	 {rows, []}] ->
	    done;
	[{columns, [_,_]},
	 {rows, [{{blob,Key},{blob,Value}}]}]->
	    {ok, {dec(key, Key, Enc), dec(value, Value, Enc)}};
	[{columns, [_,_,_]},
	 {rows, [{{blob,Key},{blob,Attrs},{blob,Value}}]}]->
	    {ok, {dec(key, Key, Enc), dec(attrs, Attrs, Enc), dec(value, Value, Enc)}};
	[{columns, [_]},
	 {rows, [{{blob,Key}}]}]->
	    {ok, dec(key, Key, Enc)}
    end.
