%%% @author Tony Rogvall <tony@rogvall.se>
%%% @copyright (C) 2011, Tony Rogvall
%%% @doc
%%%    ETS backend to kvdb
%%% @end
%%% Created : 29 Dec 2011 by Tony Rogvall <tony@rogvall.se>

-module(kvdb_ets).

-behaviour(kvdb).

-export([open/2, close/1]).
-export([add_table/2, delete_table/2]).
-export([put/4, get/3, delete/3]).
-export([iterator/2, iterator_close/2, first/2, last/2, next/2, prev/2]).

-compile(export_all).

open(Db, Options) ->
    FileName = file_name(Db, Options),
    FileMode  = file_mode(Options),
    case file:open(FileName, FileMode) of
	{ok,Fd} ->
	    Ets = ets:new(Db, [named_table, ordered_set]),
	    Result = file_load(Fd, Ets),
	    file:close(Fd),
	    case Result of
		ok ->
		    ets:insert(Ets, {{kvdb,meta_info},
				     [{file,FileName},{mode,FileMode}]}),
		    {ok,{?MODULE,Ets}};
		Error ->
		    ets:delete(Ets),
		    Error
	    end;
	Error ->
	    Error
    end.

close(Db) ->
    case ets:lookup(Db, {kvdb,meta_info}) of
	[{_,[{file,_FileName},{mode,[read,raw,binary]}]}]->
	    ets:delete(Db);
	[{_,[{file,FileName},{mode,[read,write,raw,binary]}]}] ->
	    case file:open(FileName, [write,raw,binary]) of
		{ok,Fd} ->
		    ets:delete(Db, {kvdb,meta_info}),
		    %% FIXME: only save if changed!!
		    Res = file_save(Fd, Db),
		    file:close(Fd),
		    ets:delete(Db),
		    Res;
		Error ->
		    Error
	    end;
	_ ->
	    {error, no_meta_info}
    end.

%% flush? to do a save?

add_table(Db, Table) ->
    ets:insert(Db, {{Table,table},[binary,binary]}).

delete_table(Db, Table) ->
    delete_all_elements(Db, Table),
    ets:delete(Db, {Table,table}).

put(Db, Table, Key, Value) ->
    case ets:insert(Db, {{Table,Key}, Value}) of
	true ->
	    ok;
	Error -> Error
    end.

get(Db, Table, Key) ->
    case ets:lookup(Db, {Table,Key}) of 
	[] ->
	    {error,not_found};
	[{_, Value}] ->
	    {ok,Value}
    end.

delete(Db, Table, Key) ->
    ets:delete(Db, {Table,Key}).

iterator(Db, Table) ->
    case ets:lookup(Db, {Table,table}) of
	[] ->
	    {error, no_such_table};
	[_] ->
	    Ref = make_ref(),
	    put(Ref, {Table,<<>>}),
	    {ok,Ref}
    end.

iterator_close(_Db, Iter) ->
    erase(Iter),
    ok.

first(Db, Iter) ->
    case get(Iter) of
	{Table,_} ->
	    case ets:next(Db,{Table,table}) of
		{Table,First} ->
		    [{_,Value}] = ets:lookup(Db,{Table,First}),
		    put(Iter, {Table,First}),
		    {ok,First,Value};
		{NextTable,table} when is_atom(NextTable) ->
		    done;
		'$end_of_table' ->
		    done
	    end;
	undefined ->
	    done
    end.

next(Db, Iter) ->
    case get(Iter) of
	{Table,Key} ->
	    case ets:next(Db,{Table,Key}) of
		{Table,Next} ->
		    [{_,Value}] = ets:lookup(Db,{Table,Next}),
		    put(Iter, {Table,Next}),
		    {ok,Next,Value};
		{NextTable,table} when is_atom(NextTable) ->
		    done;
		'$end_of_table' ->
		    done
	    end;
	undefined ->
	    done
    end.

last(Db, Iter) ->
    case get(Iter) of
	{Table,_} ->
	    Table1 = next_atom(Table),
	    case ets:prev(Db,{Table1,table}) of
		{Table,Last} ->
		    [{_,Value}] = ets:lookup(Db,{Table,Last}),
		    put(Iter, {Table,Last}),
		    {ok,Last,Value};
		'$end_of_table' ->
		    done
	    end;
	undefined ->
	    done
    end.

prev(Db, Iter) ->
    case get(Iter) of
	{Table,Key} ->
	    case ets:prev(Db,{Table,Key}) of
		{Table,table} ->
		    done;
		{Table,Prev} ->
		    [{_,Value}] = ets:lookup(Db,{Table,Prev}),
		    put(Iter, {Table,Prev}),
		    {ok,Prev,Value};
		'$end_of_table' ->
		    done
	    end;
	undefined ->
	    done
    end.

%%
%% Delete all elements in a virtual "table"
%%
delete_all_elements(Db, Table) ->
    delete_all_next(Db, Table, {Table,table}).

delete_all_next(Db, Table, NextKey) ->
    case ets:next(Db, NextKey) of
	{Table,Next} ->
	    NextKey1 = ets:next(Db, {Table,Next}),
	    ets:delete(Db, {Table,Next}),
	    delete_all_next(Db, Table, NextKey1);
	{NextTable,table} when is_atom(NextTable) ->
	    ok;
	'$end_of_table' ->
	    ok
    end.    
	
%% Internal
file_name(Db, Options) ->
    case proplists:get_value(file, Options) of
	undefined ->
	    atom_to_list(Db)++".db";
	Name ->
	    Name
    end.

file_mode(Options) ->
    case proplists:get_bool(read_only, Options) of
	true  -> [read,raw,binary];
	false -> [read,write,raw,binary]
    end.

%%
%% Load ets table from file
%%

file_load(Fd, Ets) ->
    file_load(Fd, Ets, undefined).

file_load(Fd, Ets, Table) ->
    case read_blob(Fd) of
	{ok,Key} ->
	    %% io:format("Table:~w, Key: ~w\n", [Table, Key]),
	    case read_blob(Fd) of
		{ok,Value} ->
		    %% io:format("Value: ~w\n", [Value]),
		    case Key of
			{Table1,table} ->
			    ets:insert(Ets, {Key,binary_to_term(Value)}),
			    file_load(Fd, Ets,Table1);
			_ when Table =/= undefined ->
			    ets:insert(Ets, {{Table,Key},Value}),
			    file_load(Fd, Ets,Table)
		    end;
		eof ->
		    io:format("Warning: trailing value at eof\n"),
		    ok;
		Error ->
		    Error
	    end;
	eof ->
	    ok;
	Error ->
	    Error
    end.

read_blob(Fd) ->
    case file:read(Fd, 4) of
	{ok,<<0:32>>} ->
	    {ok, <<Len>>} = file:read(Fd, 1),
	    {ok, Name} = file:read(Fd, Len),
	    {ok,{binary_to_atom(Name,latin1), table}};
	{ok,<<Size:32>>} -> 
	    file:read(Fd, Size);  %% not 100% correct!
	Other ->
	    Other
    end.

%%
%% Save ets table to file:
%% <0><table-name><table-info>
%% <key><value>
%%
file_save(Fd, Ets) ->
    file_save(Fd, ets:first(Ets), Ets).

file_save(_Fd, '$end_of_table', _Ets) ->
    ok;
file_save(Fd, EKey, Ets) ->
    %% io:format("Save: Key=~w\n", [EKey]),
    case ets:lookup(Ets, EKey) of
	[{{Table,table},Columns}] when is_atom(Table) ->
	    file:write(Fd, << 0:32 >>),  %% mark table def
	    TableName = atom_to_binary(Table, latin1),
	    file:write(Fd, <<(byte_size(TableName)):8, TableName/binary>>),
	    CBin = term_to_binary(Columns),
	    file:write(Fd, <<(byte_size(CBin)):32, CBin/binary>>),
	    file_save(Fd, ets:next(Ets, EKey), Ets);
	[{{Table,Key},Value}] when is_atom(Table) ->
	    file:write(Fd, <<(byte_size(Key)):32, Key/binary,
			     (byte_size(Value)):32, Value/binary>>),
	    file_save(Fd, ets:next(Ets, EKey), Ets);
	[] ->
	    io:format("Warning: no value for key ~p\n", [EKey]),
	    file_save(Fd, ets:next(Ets, EKey), Ets)
    end.

%% calculate X+1 when X is atom
next_atom(X) when is_atom(X) ->
    list_to_atom(next_byte_list(atom_to_list(X))).

%% add 1 to a byte list
next_byte_list(Xs) ->
    lists:reverse(next_byte_r_list(lists:reverse(Xs))).

next_byte_r_list([255|Xs]) ->
    [0|next_byte_r_list(Xs)];
next_byte_r_list([X|Xs]) when X < 255 ->
    [X+1|Xs];
next_byte_r_list([]) ->
    [1].
