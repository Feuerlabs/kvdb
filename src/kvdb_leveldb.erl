%%% @author Tony Rogvall <tony@rogvall.se>
%%% @copyright (C) 2011, Tony Rogvall
%%% @doc
%%%    LevelDB backend to kvdb
%%% @end
%%% Created : 29 Dec 2011 by Tony Rogvall <tony@rogvall.se>

-module(kvdb_leveldb).

-behaviour(kvdb).

-export([open/2, close/1]).
-export([add_table/2, delete_table/2]).
-export([put/3, get/3, delete/3]).
-export([iterator/2, first/2, last/2, next/2, prev/2]).

-import(kvdb_lib, [dec/3, enc/3]).

-record(db, {ref, encoding}).

open(Db, Options) ->
    DbOpts = [{create_if_missing,true}],
    Res = case proplists:get_value(file, Options) of
	      undefined ->
		  eleveldb:open(atom_to_list(Db)++".db", DbOpts);
	      Name ->
		  eleveldb:open(Name, DbOpts)
	  end,
    case Res of
	{ok, Ref} ->
	    {ok, #db{ref = Ref, encoding = proplists:get_value(encoding, Options, raw)}};
	Error ->
	    Error
    end.

close(_Db) ->
    %% leveldb is garbage collected
    ok.

add_table(#db{ref = Db}, Table) ->
    T = make_table_key(Table, <<>>),
    eleveldb:put(Db, T, <<>>, []).

delete_table(_Db, _Table)
  when is_atom(_Table) ->
    %% FIXME delete all elements (iterate!)
    ok.

put(Db, Table, {Key, Value}) ->
    eleveldb:put(Db, make_table_key(Table,Key), Value, []).

get(Db, Table, Key) ->
    eleveldb:get(Db, make_table_key(Table,Key), []).

delete(Db, Table, Key) ->
    eleveldb:delete(Db, make_table_key(Table,Key), []).

iterator(Db, Table) ->
    TableKey = make_table_first_key(Table),
    case eleveldb:get(Db, TableKey, []) of
	not_found ->
	    {error, no_such_table};
	{ok,<<>>} ->
	    {ok,I} = eleveldb:iterator(Db, []),
	    {ok,{I,Table}}
    end.

first(Db, Iter={I,Tk}) ->
    TableKey = make_table_first_key(Tk),
    case eleveldb:iterator_move(I, TableKey) of
	{ok,TableKey} -> %% must exist!
	    next(Db,Iter);
	{ok,TableKey,_Value} -> %% must exist!
	    next(Db,Iter);
	{ok,_Key} ->
	    done;
	{ok,_Key,_Value} ->
	    done;
	{error, invalid_iterator} ->
	    done
    end.

last(Db, Iter={I,Tk}) ->
    FirstKey = make_table_first_key(Tk),
    FirstSize = byte_size(FirstKey),
    LastKey = make_table_last_key(Tk),
    case eleveldb:iterator_move(I, LastKey) of
	{ok,_AfterKey} ->
	    prev(Db, Iter);
	{ok,_AfterKey,_Value} ->
	    prev(Db, Iter);
	{error, invalid_iterator} ->
	    case eleveldb:iterator_move(I, last) of
		{ok,FirstKey} ->
		    done;
		{ok,FirstKey,_Value} ->
		    done;
		{ok,<<FirstKey:FirstSize/binary,Key/binary>>} ->
		    {ok,Key};
		{ok,<<FirstKey:FirstSize/binary,Key/binary>>,Value} ->
		    {ok,Key,Value};
		{ok,_AfterKey} ->
		    done;
		{ok,_AfterKey,_Value} ->
		    done;
		{error, invalid_iterator} ->
		    done
	    end
    end.

next(_Db, _Iter={I,Tk}) ->
    TableKey = make_table_key(Tk),
    KeySize = byte_size(TableKey),
    case eleveldb:iterator_move(I, next) of
	{ok,<<TableKey:KeySize/binary,Key/binary>>} ->
	    {ok,Key};
	{ok,<<TableKey:KeySize/binary,Key/binary>>,Value} ->
	    {ok,Key,Value};
	{ok,_} ->
	    done;
	{ok,_,_} ->
	    done;
	{error, invalid_iterator} ->
	    done;
	Error ->
	    Error
    end.

prev(_Db, _Iter={I,Tk}) ->
    TableKey = make_table_key(Tk),
    KeySize = byte_size(TableKey),
    case eleveldb:iterator_move(I, prev) of
	{ok,TableKey} ->
	    done;
	{ok,TableKey,_} ->
	    done;
	{ok,<<TableKey:KeySize/binary,Key/binary>>} ->
	    {ok,Key};
	{ok,<<TableKey:KeySize/binary,Key/binary>>,Value} ->
	    {ok,Key,Value};
	{ok,_} ->
	    done;
	{ok,_,_} ->
	    done;
	{error, invalid_iterator} ->
	    done;
	Error ->
	    Error
    end.


%% create key
make_table_first_key(Table) ->
    make_key(Table, $:, <<>>).

make_table_last_key(Table) ->
    make_key(Table, $;, <<>>).

make_table_key(Table) ->
    make_key(Table, $:, <<>>).

make_table_key(Table, Key) ->
    make_key(Table, $:, Key).

make_key(Table, Sep, Key) ->
    <<(atom_to_binary(Table,latin1))/binary,Sep,Key/binary>>.
