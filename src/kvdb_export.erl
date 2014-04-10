-module(kvdb_export).

-compile(export_all).
-include("kvdb.hrl").

export(#kvdb_ref{mod = M, db = Db}, File) ->
    Format = format(File),
    Tabs = M:list_tables(Db),
    {Out, Close} = open_log(File, Format),
    try dump(Tabs, M, Db, Out)
    after
	Close()
    end.

import(#kvdb_ref{mod = M, db = Db}, File) ->
    import(M, Db, File, format(File)).

import(M, Db, File) ->
    import(M, Db, File, format(File)).

import(M, Db, File, binary) ->
    case disk_log:open([{name, ?MODULE},
			{file, File},
			{format, internal}]) of
	{ok, Log} ->
	    try import_bin_log(Log, M, Db)
	    after
		disk_log:close(Log)
	    end;
	Error ->
	    Error
    end.

format(F) ->
    case filename:extension(F) of
	".KBUPB" -> binary;
	_ -> error(unknown_extension)
    end.

dump([T|Tabs], M, Db, Out) ->
    #table{type = Type} = TabR = M:info(Db, {T, tabrec}),
    Out({table, T, lists:keydelete(name, 1, kvdb_lib:tabrec_to_list(TabR))}),
    with_tab(T, Type, M, Db, Out),
    dump(Tabs, M, Db, Out);
dump([], _, _, _) ->
    ok.

open_log(File, binary) ->
    case disk_log:open([{name, ?MODULE},
			{file, File},
			{format, internal}]) of
	{ok, Log} ->
	    {fun(Term) ->
		     disk_log:blog(Log, term_to_binary(Term, [compressed]))
	     end,
	     fun() ->
		     disk_log:close(Log)
	     end};
	{error, Reason} ->
	    error({Reason, {open_log, File}})
    end.

import_bin_log(Log, M, Db) ->
    import_bin_log(disk_log:bchunk(Log, start), Log, undefined, M, Db).

import_bin_log({Cont, Bins}, Log, Tab, M, Db) ->
    Tab1 = import_bin_log_(Bins, Tab, M, Db),
    import_bin_log(disk_log:bchunk(Log, Cont), Log, Tab1, M, Db);
import_bin_log({Cont, Bins, BadBytes}, Log, Tab, M, Db) ->
    io:fwrite("Warning: BadBytes = ~p~n", [BadBytes]),
    Tab1 = import_bin_log_(Bins, Tab, M, Db),
    import_bin_log(disk_log:bchunk(Log, Cont), Log, Tab1, M, Db);
import_bin_log(eof, _, _, _, _) ->
    ok.

import_bin_log_([Bin|Bins], Tab, M, Db) ->
    case binary_to_term(Bin) of
	{table, T, Opts} ->
	    #table{type = Type} = TR = kvdb_lib:make_tabrec(T, Opts),
	    M:add_table(Db, T, TR),
	    import_bin_log_(Bins, {T,Type}, M, Db);
	{obj, Obj} ->
	    add_to_tab(Tab, Obj, M, Db),
	    import_bin_log_(Bins, Tab, M, Db)
    end;
import_bin_log_([], Tab, _, _) ->
    Tab.

add_to_tab(undefined, Obj, _, _) ->
    io:fwrite("Unknown tab, skipping ~p~n", [Obj]);
add_to_tab({T, Type}, Obj, M, Db) ->
    IsQueue = kvdb_lib:valid_queue(Type),
    if Type == set ->
	    M:put(Db, T, Obj);
       IsQueue == true ->
	    {QK, St, O} = Obj,
	    M:queue_insert(Db, T, QK, St, O)
    end.

with_tab(Tab, Type, M, Db, Out) ->
    IsQueue = kvdb_lib:valid_queue(Type),
    if Type == set ->
	    chunk_load(M:prefix_match(Db, Tab, <<>>, 1000), Out);
       IsQueue == true ->
	    Filter = fun(St, QKey, Obj) ->
			     {keep, {obj,{QKey, St, Obj}}}
		     end,
	    all_queues(
	      fun(Q) ->
		      load_queue(
			M:list_queue(Db, Tab, Q, Filter, false, 100), Out)
	      end, M, Db, Tab)
    end.

chunk_load({Objs, Cont}, Out) ->
    [Out({obj, Obj}) || Obj <- Objs],
    chunk_load(Cont(), Out);
chunk_load(done, _) ->
    ok.

all_queues(F, M, Db, T) ->
    all_queues(M:first_queue(Db, T), F, M, Db, T).

all_queues(done, _, _, _, _) ->
    done;
all_queues({ok,Q}, F, M, Db, T) ->
    F(Q),
    all_queues(M:next_queue(Db, T, Q), F, M, Db, T).

load_queue({Objs, Cont}, F) ->
    lists:foreach(F, Objs),
    load_queue(Cont(), F);
load_queue(done, _) ->
    done.
