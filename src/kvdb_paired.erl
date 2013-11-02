%%%---- BEGIN COPYRIGHT -------------------------------------------------------
%%%
%%% Copyright (C) 2012 Feuerlabs, Inc. All rights reserved.
%%%
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at http://mozilla.org/MPL/2.0/.
%%%
%%%---- END COPYRIGHT ---------------------------------------------------------
%%% @author Ulf Wiger <ulf@feuerlabs.com>
%%% @doc
%%% Paired backend for kvdb
%%%
%%% NOTE: This is work in progress. Several things still do not work
%%%
%%% The idea with this backend is to combine two backends as a write-through
%%% pair. That is: all reads are done on 'backend 1', but writes are served
%%% in both 'backend 1' and 'backend 2'. An example of how to use this would
%%% be e.g. a kvdb_ets backend in front of a kvdb_riak backend:
%%%
%%% <pre lang="erlang">
%%% kvdb:open(p, [{backend, kvdb_paired},
%%%               {module1, kvdb_ets},
%%%               {module2, kvdb_riak},
%%%               {options2, [{update_index, false}]}]).
%%% </pre>
%%%
%%% The 'options2' list applies only to the 'backend 2' (riak in this case).
%%% With `{update_index, false}' on 'backend 2', we will not maintain indexes
%%% on the riak side, but rebuild them when the ets backend is populated.
%%% @end
-module(kvdb_paired).

-behaviour(kvdb).

-export([open/2, close/1]).
-export([add_table/3, delete_table/2, list_tables/1]).
-export([put/3, push/4, get/3, get_attrs/4, index_get/4, index_keys/4,
	 update_counter/4, pop/3, prel_pop/3, extract/3, delete/3,
	 list_queue/3, list_queue/6, is_queue_empty/3,
	 queue_read/3, queue_insert/5, queue_delete/3, mark_queue_object/4,
	 queue_head_write/4, queue_head_read/3, queue_head_delete/3]).
-export([first_queue/2, next_queue/3]).
-export([first/2, last/2, next/3, prev/3,
	 prefix_match/3, prefix_match/4, prefix_match_rel/5]).
-export([get_schema_mod/2,
	 schema_write/4,
	 schema_read/3,
	 schema_delete/3,
	 schema_fold/3]).
-export([info/2, is_table/2]).
-export([dump_tables/1]).

-export([proxy_childspecs/2]).

-include("kvdb.hrl").
-include_lib("lager/include/log.hrl").

-define(if_table(Db, Tab, Expr), if_table(Db, Tab, fun() -> Expr end)).

info(#db{ref = {{M,Db},_} = Ref}, What) ->
    case What of
	ref -> Ref;
	_ ->
	    M:info(Db, What)
    end.

proxy_childspecs(Name, Options) ->
    {M1, Opts1, M2, Opts2} = db_pair(Options),
    proxy_childspecs_(M1, Name, Opts1) ++
	proxy_childspecs_(M2, Name, Opts2).

proxy_childspecs_(M, Name, Opts) ->
    try M:proxy_childspecs(Name, Opts)
    catch
	error:undef ->
	    []
    end.

is_table(#db{ref = {{M,Db},_}}, Tab) ->
    M:is_table(Db, Tab).

get_schema_mod(#db{ref = {{M,Db},_}}, Default) ->
    M:get_schema_mod(Db, Default).

schema_write(#db{ref = {{M1,Db1},{M2,Db2}}}, Cat, K, V) ->
    ok = M1:schema_write(Db1, Cat, K, V),
    ok = M2:schema_write(Db2, Cat, K, V).

schema_read(#db{ref = {{M1,Db1},_}}, Cat, K) ->
    M1:schema_read(Db1, Cat, K).

schema_delete(#db{ref = {{M1,Db1},{M2,Db2}}}, Cat, K) ->
    ok = M1:schema_delete(Db1, Cat, K),
    ok = M2:schema_delete(Db2, Cat, K).

schema_fold(#db{ref = {{M1,Db1},_}}, F, A) ->
    M1:schema_fold(Db1, F, A).

dump_tables(#db{ref = {{M,Db},_}}) ->
    M:dump_tables(Db).


open(DbName, Options) ->
    {M1, O1, M2, O2} = db_pair(Options),
    case M1:open(DbName, O1) of
	{ok, Db1} ->
	    case M2:open(DbName, O2) of
		{ok, Db2} ->
		    Db = Db1#db{ref = {{M1, Db1}, {M2, Db2}}},
		    ok = load(Db),
		    {ok, Db};
		Error2 ->
		    M1:close(Db1),
		    Error2
	    end;
	Error1 ->
	    Error1
    end.

db_pair(Options) ->
    {_, M1} = lists:keyfind(module1, 1, Options),
    {_, M2} = lists:keyfind(module2, 1, Options),
    {O1, O2} = split_options(Options),
    {M1, O1, M2, O2}.

split_options(Options) ->
    {O1, R1} = take(options1, Options),
    {O2, R2} = take(options2, R1),
    {O1 ++ R2, O2 ++ R2}.

take(K, Options) ->
    case lists:keytake(K, 1, Options) of
	{value, {_, Found}, Rest} ->
	    {Found, Rest};
	false ->
	    {[], Options}
    end.

load(#db{ref = {{M1,Db1}, {M2, Db2}}} = Db) ->
    Tabs = M2:list_tables(Db2),
    lists:foreach(
      fun(T) ->
	      case M2:schema_read(Db2, property, {T,autoload}) of
		  false ->
		      M1:schema_write(Db1, property, {T, ram}, false),
		      ok;
		  _ ->
		      M1:schema_write(Db1, property, {T, ram}, true),
		      M1:delete_table(Db1, T),
		      TabR = M2:info(Db2, {T, tabrec}),
		      M1:add_table(Db1, T, TabR),
		      case has_disk(M2, Db2, T) of
			  false ->
			      io:fwrite("Table ~s defined as {disk, false}~n", [T]),
			      ok;
			  _ ->
			      case TabR#table.type of
				  set ->
				      chunk_load(M2:prefix_match(Db2, T, <<>>, 1000),
						 M1, Db1, T);
				  Type when Type==fifo; Type==lifo;
					    Type=={keyed,fifo}; Type=={keyed,lifo} ->
				      Filter = fun(St, QKey, Obj) ->
						       {keep, {QKey, St, Obj}}
					       end,
				      all_queues(
					fun(Q) ->
						load_queue(
						  M2:list_queue(Db2, T, Q,
								Filter, false, 100),
						  M1, Db1, T)
					end, M2, Db2, T)
			      end
		      end
	      end
      end, Tabs),
    load_schema(Db).

load_schema(#db{ref = {{M1,Db1}, {M2, Db2}}}) ->
    M2:schema_fold(
      Db2, fun(C, {K,V}, _) ->
		   case M1:schema_read(Db1, C, K) of
		       undefined ->
			   M1:schema_write(Db1, C, K, V);
		       _ ->
			   ok
		   end
	   end, ok).

all_queues(F, M, Db, T) ->
    all_queues(M:first_queue(Db, T), F, M, Db, T).

all_queues(done, _, _, _, _) ->
    done;
all_queues({ok,Q}, F, M, Db, T) ->
    F(Q),
    all_queues(M:next_queue(Db, T, Q), F, M, Db, T).

load_queue({Objs, Cont}, M, Db, T) ->
    lists:foreach(
      fun({QKey, St, Obj}) ->
	      M:queue_insert(Db, T, QKey, St, Obj)
      end, Objs),
    load_queue(Cont(), M, Db, T);
load_queue(done, _, _, _) ->
    done.



chunk_load({Objs, Cont}, M, Db, T) ->
    [M:put(Db, T, Obj) || Obj <- Objs],
    chunk_load(Cont(), M, Db, T);
chunk_load(done, _, _, _) ->
    ok.


close(#db{ref = {{M1,Db1},{M2,Db2}}}) ->
    M1:close(Db1),
    M2:close(Db2).

add_table(#db{ref = {{M1,Db1},{M2,Db2}}}, Table, Opts) when is_list(Opts) ->
    case M1:info(Db1, {Table, type}) of
	undefined ->
	    _TabR = kvdb_lib:make_tabrec(Table, check_encoding(Opts, Db1)),
	    M2:add_table(Db2, Table, Opts),
	    M1:add_table(Db1, Table, Opts);
	_ -> ok
    end;
add_table(#db{ref = {{M1,Db1},{M2,Db2}}}, Table, #table{} = TabR) ->
    case M1:is_table(Db1, Table) of
	true -> ok;
	false ->
	    M2:add_table(Db2, Table, TabR),
	    M1:add_table(Db1, Table, TabR)
    end.


list_tables(#db{ref = {{M,Db},_}}) ->
    M:list_tables(Db).

delete_table(#db{ref = {{M1,Db1},{M2,Db2}}}, Table) ->
    case M2:delete_table(Db2, Table) of
	ok ->
	    M1:delete_table(Db1, Table);
	Other ->
	    Other
    end.

put(#db{ref = {{M1,Db1},{M2,Db2}}}, Table, Obj) ->
    case has_disk(M1, Db1, Table) of
	true ->
	    case M2:put(Db2, Table, Obj) of
		ok ->
		    M1:put(Db1, Table, Obj);
		Other ->
		    Other
	    end;
	false ->
	    M1:put(Db1, Table, Obj)
    end.

update_counter(#db{ref = {{M1,Db1},{M2,Db2}}}, Table, Key, Incr) ->
    NewValue = M1:update_counter(Db1, Table, Key, Incr),
    {ok, Obj} = M1:get(Db1, Table, Key),
    case has_disk(M2, Db2, Table) of
	true -> M2:put(Db2, Table, Obj);
	false -> ok
    end,
    NewValue.
    %% M2:update_counter(Db2, Table, Key, Incr),
    %% M1:update_counter(Db1, Table, Key, Incr).

push(#db{ref = {{M1,Db1},{M2,Db2}}}, Table, Q, Obj) ->
    case has_disk(M1, Db1, Table) of
	false -> ok;
	_ ->
	    M2:push(Db2, Table, Q, Obj)
    end,
    M1:push(Db1, Table, Q, Obj).

get(#db{ref = {{M,Db},_}}, Tab, K) ->
    M:get(Db, Tab, K).

get_attrs(#db{ref = {{M,Db},_}}, Tab, K, As) ->
    M:get_attrs(Db, Tab, K, As).

index_get(#db{ref = {{M,Db},_}}, Tab, K, V) ->
    M:index_get(Db, Tab, K, V).

index_keys(#db{ref = {{M,Db},_}}, T, K, V) ->
    M:index_keys(Db, T, K, V).

pop(#db{ref = {{M1,Db1},{M2,Db2}}}, T, Q) ->
    M2:pop(Db2, T, Q),
    M1:pop(Db1, T, Q).

prel_pop(#db{ref = {{M1,Db1},{M2,Db2}}}, T, Q) ->
    M2:prel_pop(Db2, T, Q),
    M1:prel_pop(Db1, T, Q).

extract(#db{ref = {{M1,Db1},{M2,Db2}}}, T, K) ->
    M2:extract(Db2, T, K),
    M1:extract(Db1, T, K).

delete(#db{ref = {{M1,Db1},{M2,Db2}}}, T, K) ->
    case has_disk(M1, Db1, T) of
	true  -> M2:delete(Db2, T, K);
	false -> ok
    end,
    M1:delete(Db1, T, K).

list_queue(#db{ref = {{M,Db},_}}, T, Q) ->
    M:list_queue(Db, T, Q).

list_queue(#db{ref = {{M,Db},_}}, T, Q, Fltr, HeedBlock, Limit) ->
    M:list_queue(Db, T, Q, Fltr, HeedBlock, Limit).

is_queue_empty(#db{ref = {{M,Db},_}}, T, Q) ->
    M:is_queue_empty(Db, T, Q).

queue_read(#db{ref = {{M,Db},_}}, T, K) ->
    M:queue_read(Db, T, K).

queue_insert(#db{ref = {{M1,Db1},{M2,Db2}}}, T, K, St, Obj) ->
    case has_disk(M1, Db1, T) of
	true  -> M2:queue_insert(Db2, T, K, St, Obj);
	false -> ok
    end,
    M1:queue_insert(Db1, T, K, St, Obj).

queue_delete(#db{ref = {{M1,Db1},{M2,Db2}}}, T, K) ->
    case has_disk(M1, Db1, T) of
	true -> M2:queue_delete(Db2, T, K);
	false -> ok
    end,
    M1:queue_delete(Db1, T, K).

mark_queue_object(#db{ref = {{M1,Db1},{M2,Db2}}}, Tab, K, St) ->
    case has_disk(M1, Db1, Tab) of
	true -> M2:mark_queue_object(Db2, Tab, K, St);
	false -> ok
    end,
    M1:mark_queue_object(Db1, Tab, K, St).

queue_head_write(#db{ref = {{M1,Db1},{M2,Db2}}}, Tab, Q, Obj) ->
    case has_disk(M1, Db1, Tab) of
	true -> M2:queue_head_write(Db2, Tab, Q, Obj);
	false -> ok
    end,
    M1:queue_head_write(Db1, Tab, Q, Obj).

queue_head_read(#db{ref = {{M1,Db1},_}}, Tab, Q) ->
    M1:queue_head_read(Db1, Tab, Q).

queue_head_delete(#db{ref = {{M1,Db1},{M2,Db2}}}, Tab, Q) ->
    case has_disk(M1, Db1, Tab) of
	true -> M2:queue_head_delete(Db2, Tab, Q);
	false -> ok
    end,
    M1:queue_head_delete(Db1, Tab, Q).

first_queue(#db{ref = {{M,Db},_}}, Tab) ->
    M:first_queue(Db, Tab).

next_queue(#db{ref = {{M,Db},_}}, Tab, Q) ->
    M:next_queue(Db, Tab, Q).

first(#db{ref = {{M,Db},_}}, Tab) ->
    M:first(Db, Tab).

last(#db{ref = {{M,Db},_}}, Tab) ->
    M:last(Db, Tab).

next(#db{ref = {{M,Db},_}}, Tab, K) ->
    M:next(Db, Tab, K).

prev(#db{ref = {{M,Db},_}}, Tab, K) ->
    M:prev(Db, Tab, K).

prefix_match(#db{ref = {{M,Db},_}}, Tab, Pfx) ->
    M:prefix_match(Db, Tab, Pfx).

prefix_match(#db{ref = {{M,Db},_}}, Tab, Pfx, Limit) ->
    M:prefix_match(Db, Tab, Pfx, Limit).

prefix_match_rel(#db{ref = {{M,Db},_}}, Tab, Prefix, Start, Limit) ->
    M:prefix_match_rel(Db, Tab, Prefix, Start, Limit).


check_encoding(Opts, #db{encoding = Enc0}) ->
    case lists:keymember(encoding, 1, Opts) of
	true ->
	    Opts;
	false ->
	    [{encoding, Enc0}|Opts]
    end.

has_disk(M1, Db1, T) ->
    case M1:schema_read(Db1, property, {T, disk}) of
	false -> false;
	_  -> true
    end.

