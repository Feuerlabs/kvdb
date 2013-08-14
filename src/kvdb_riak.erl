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
%%% Highly experimental riak backend for kvdb
%%%
%%% NOTE: This is work in progress. Several things still do not work
%%%
%%% The best way to use this for now is via the kvdb_paired backend.
%%% @end
-module(kvdb_riak).

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
-export([get_schema_mod/2]).
-export([info/2, is_table/2]).
-export([dump_tables/1]).

-export([proxy_childspecs/2]).

-include("kvdb.hrl").
%% This is obviously a crude hack (riak_kv_wm_raw.hrl copied into include/)
-include("riak_kv_wm_raw.hrl").

-define(if_table(Db, Tab, Expr), if_table(Db, Tab, fun() -> Expr end)).

-record(ref, {bucket,
	      update_index = true}).

info(#db{} = Db, What) ->
    case What of
	tables   -> list_tables(Db);
	encoding -> Db#db.encoding;
	ref      -> Db#db.ref;
	{Tab,encoding} -> ?if_table(Db, Tab, encoding(Db, Tab));
	{Tab,index   } -> ?if_table(Db, Tab, index(Db, Tab));
	{Tab,type    } -> ?if_table(Db, Tab, type(Db, Tab));
	{Tab,schema  } -> ?if_table(Db, Tab, schema(Db, Tab));
	{Tab,tabrec  } -> schema_lookup(Db, {table, Tab}, undefined);
	_ -> undefined
    end.

encoding(_, _) -> error(nyi).
index(#db{ref = #ref{update_index = B}} = Db, Tab) ->
    if B -> schema_lookup(Db, {Tab, index}, []);
       true -> []
    end.
type(_, _) -> error(nyi).
schema(_, _) -> error(nyi).

schema_lookup(#db{metadata = Ets}, Key, Default) ->
    case ets:lookup(Ets, Key) of
	[] ->
	    Default;
	[{_, V}] ->
	    V
    end.

schema_write(#db{metadata = Ets}, {_, _} = Obj) ->
    ets:insert(Ets, Obj).

is_table(#db{metadata = ETS}, Tab) ->
    ets:member(ETS, {table, Tab}).

get_schema_mod(Db, Default) ->
    case schema_lookup(Db, schema_mod, undefined) of
	undefined ->
	    schema_write(Db, {schema_mod, Default}),
	    Default;
	M ->
	    M
    end.

if_table(Db, Tab, F) ->
    case is_table(Db, Tab) of
	true -> F();
	false -> undefined
    end.

dump_tables(_) ->
    [].


proxy_childspecs(Name, Options) ->
    [{riak_proxy, {kvdb_riak_proxy, start_link, [Name, Options]},
      permanent, 5000, worker, [kvdb_riak_proxy]}].

open(DbName, Options) ->
    case kvdb_riak_proxy:get_session(DbName) of
	undefined ->
	    error(no_session); % might want to improve this later
	Session ->
	    Ets = ets:new(kvdb_schema, [ordered_set, public]),
	    ets:insert(Ets, {riak_session, Session}),
	    Ref = #ref{bucket = <<"kvdb*", (to_bin(DbName))/binary>>,
		       update_index = proplists:get_value(update_index,
							  Options, true)},
	    Db = load_schema(#db{ref = Ref, metadata = Ets}, Session),
	    {ok, Db}
    end.

close(Db) ->
    case session_pid(Db) of
	undefined -> ok;
	{client, _} -> ok;
	{pb, Session} ->
	    riakc_pb_socket:stop(Session),
	    ok
    end.

add_table(#db{ref = Ref, metadata = Ets} = Db, Table, Opts) ->
    case is_table(Db, Table) of
	false ->
	    TabR = case Opts of
		       _ when is_list(Opts) ->
			   kvdb_lib:make_tabrec(Table, Opts);
		       #table{} ->
			   Opts
		   end,
	    case session_pid(Db) of
		undefined -> error(no_session);
		Session ->
		    riak_put(Session, Ref#ref.bucket, Table,
			     {Table, TabR}, []),
		    write_tabrec_ets(Ets, TabR)
	    end;
	true ->
	    {error, exists}
    end.


list_tables(#db{metadata = Ets}) ->
    ets:select(Ets, [{ {{table,'$1'},'_'}, [], ['$1']}]).

delete_table(#db{}, _Table) ->
    error(nyi).

put(#db{ref = Ref} = Db, Table, {K,_} = Obj) ->
    case session_pid(Db) of
	undefined -> error(no_session);
	Session ->
	    Bucket = tab_key(Ref, Table),
	    riak_put(Session, Bucket, K, Obj, []),
	    ok
    end;
put(#db{ref = Ref} = Db, Table, {K, Attrs, V} = Obj) ->
    case session_pid(Db) of
	undefined -> error(no_session);
	Session ->
	    Ix = index(Db, Table),
	    IxVals = kvdb_lib:index_vals(Ix, K, Attrs, fun() -> V end),
	    riak_put(Session, tab_key(Ref, Table), K, Obj, IxVals),
	    ok
    end.

riak_put({pb, Session}, Bucket, K, Val, IxVals) ->
    O = riakc_obj:new(Bucket, K, Val),
    O1 = if IxVals =/= [] ->
		 riakc_obj:set_secondary_index(
		   O, [{{binary_index, to_bin(N)}, [I]}
		       || {N,I} <- IxVals]);
	    true ->
		 O
	 end,
    riakc_pb_socket:put(Session, O1);
riak_put({client, Session}, Bucket, K, Val0, IxVals) ->
    Val = term_to_binary(Val0),
    Obj = if IxVals =/= [] ->
		  Ix = [{ix_field(Ni), Vi} || {Ni, Vi} <- IxVals],
		  riak_object:new(Bucket, K, Val,
				  dict:from_list(
				    [{?MD_INDEX, Ix}]));
	     true ->
		  riak_object:new(Bucket, K, Val)
	  end,
    riak_client:put(Obj, Session).

ix_field(Name) ->
    <<(to_bin(Name))/binary, "_bin">>.

to_bin(B) when is_binary(B) -> B;
to_bin(L) when is_list(L)   -> list_to_binary(L);
to_bin(A) when is_atom(A)   -> atom_to_binary(A, latin1).

update_counter(_Db, _Table, _Key, _Incr) ->
    error(nyi).

push(_, _, _, _) ->
    error(nyi).

get(#db{ref = Ref} = Db, Tab, K) ->
    case session_pid(Db) of
	undefined -> error(no_session);
	Session ->
	    riak_get(tab_key(Ref, Tab), K, Session)
    end.

get_attrs(_, _, _, _) ->
    error(nyi).

index_get(_, _, _, _) ->
    error(nyi).

index_keys(_, _, _, _) ->
    error(nyi).

pop(_, _, _) ->
    error(nyi).

prel_pop(_, _, _) ->
    error(nyi).

extract(_, _, _) ->
    error(nyi).

delete(#db{ref = Ref} = Db, Tab, K) ->
    case session_pid(Db) of
	undefined -> error(no_session);
	Session ->
	    riak_delete(tab_key(Ref, Tab), K, Session)
    end.

list_queue(_, _, _) ->
    error(nyi).

list_queue(_, _, _, _, _, _) ->
    error(nyi).

is_queue_empty(_, _, _) ->
    error(nyi).

queue_read(_, _, _) ->
    error(nyi).

queue_insert(_, _, _, _, _) ->
    error(nyi).

queue_delete(_, _, _) ->
    error(nyi).

mark_queue_object(_, _, _, _) ->
    error(nyi).

queue_head_write(_, _, _, _) ->
    error(nyi).

queue_head_read(_, _, _) ->
    error(nyi).

queue_head_delete(_, _, _) ->
    error(nyi).

first_queue(_, _) ->
    error(nyi).

next_queue(_, _, _) ->
    error(nyi).

first(_, _) ->
    error(nyi).

last(_, _) ->
    error(nyi).

next(_, _, _) ->
    error(nyi).

prev(_, _, _) ->
    error(nyi).

prefix_match(Db, Table, Pfx) ->
    ?if_table(Db, Table, prefix_match_(Db, Table, Pfx)).

prefix_match_(#db{ref = Ref} = Db, Table, Pfx) ->
    case session_pid(Db) of
	undefined -> error(no_session);
	{pb, Session} ->
	    Bucket = tab_key(Ref, Table),
	    case riakc_pb_socket:mapred_bucket(
		   Session, Bucket,
		   [kvdb_riak_mapred:map_prefix_match(Pfx),
		    riak_kv_mapreduce:reduce_sort(true)]) of
		{ok, []} ->
		    [];
		{ok, MapRedRes} ->
		    lists:last(MapRedRes);
		Other ->
		    error(Other)
	    end;
	_ ->
	    error(nyi)
    end.

prefix_match(Db, Table, Pfx, infinity) ->
    {prefix_match(Db, Table, Pfx), fun() -> done end};
prefix_match(Db, Table, Pfx, Limit) when is_integer(Limit), Limit > 0 ->
    %% Fake chunking for now
    chunk(prefix_match(Db, Table, Pfx), Limit).

chunk(L, Limit) ->
    if length(L) >= Limit ->
	    {A, B} = lists:split(Limit, L),
	    {A, fun() ->
			chunk(B, Limit)
		end};
       true ->
	    {L, fun() -> done end}
    end.

prefix_match_rel(_, _, _, _, _) ->
    error(nyi).


tab_key(#ref{bucket = B}, Tab) ->
    <<B/binary, "*", Tab/binary>>.

session_pid(#db{metadata = Ets}) ->
    case ets:lookup(Ets, riak_session) of
	[] ->
	    undefined;
	[{_, Pid}] ->
	    Pid
    end.

riak_get(Bucket, K, {pb, Session}) ->
    case riakc_pb_socket:get(Session, Bucket, K) of
	{ok, Obj} ->
	    {ok, binary_to_term(riakc_obj:get_value(Obj))};
	{error,notfound} ->
	    {error, not_found}
    end;
riak_get(Bucket, K, {client, C}) ->
    case riak_client:get(Bucket, K, C) of
	{ok, Obj} ->
	    {ok, binary_to_term(riak_object:get_value(Obj))};
	{error, notfound} -> {error, not_found}
    end.

riak_delete(Bucket, K, {pb, Session}) ->
    case riakc_pb_socket:delete(Session, Bucket, K) of
	ok ->
	    ok;
	{error,_} = Error ->
	    Error
    end;
riak_delete(Bucket, K, {client, C}) ->
    case riak_client:delete(Bucket, K, C) of
	ok ->
	    ok;
	{error,_} = Error ->
	    Error
    end.


load_schema(#db{ref = Ref, metadata = Ets} = Db, Session) ->
    case riak_list_keys(Bucket = Ref#ref.bucket, Session) of
	{ok, Keys} ->
	    lists:foreach(
	      fun(K) ->
		      case riak_get(Bucket, K, Session) of
			  {ok, {_Tab, TabR}} ->
			      write_tabrec_ets(Ets, TabR);
			  {error, not_found} ->
			      %% ?
			      ok
		      end
	      end, Keys);
	_ ->
	    ok
    end,
    Db.

riak_list_keys(Bucket, {client, Session}) ->
    riak_client:list_keys(Bucket, Session);
riak_list_keys(Bucket, {pb, Session}) ->
    riakc_pb_socket:list_keys(Session, Bucket).


write_tabrec_ets(Ets, #table{name = Name,
			     type = Type,
			     index = Ix,
			     encoding = Enc} = R) ->
    ets:insert(Ets, [{{table, Name}, R},
		     {{Name, type}, Type},
		     {{Name, index}, Ix},
		     {{Name, encoding, Enc}}]).
