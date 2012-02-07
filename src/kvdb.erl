%%% @author Tony Rogvall <tony@rogvall.se>
%%% @copyright (C) 2011, Tony Rogvall
%%% @doc
%%%    key value database frontend
%%% @end
%%% Created : 29 Dec 2011 by Tony Rogvall <tony@rogvall.se>

-module(kvdb).

-export([open/2, close/1]).
-export([add_table/2, delete_table/2]).
-export([put/4, get/3, delete/3]).
-export([iterator/2, iterator_close/2, first/2, last/2, next/2, prev/2]).

-export([behaviour_info/1]).

%% The plugin behaviour
behaviour_info(callbacks) ->
    [
     {open,2},
     {close,1},
     {add_table,2},
     {delete_table,2},
     {put,4},
     {get,3},
     {delete,3},
     {iterator,2},
     {iterator_close,2},
     {first,2},
     {last,2},
     {next,2},
     {prev,2}
    ];
behaviour_info(_Other) ->
    undefined.

-type db_ref()    :: {atom(),any()}.
-opaque itr_ref() :: any().

-spec open(Nam::atom(), Options::[{atom(),term()}]) ->
		  {ok,db_ref()} | {error,term()}.
		  
open(Name, Options) when is_atom(Name), is_list(Options) ->
    DbMod = proplists:get_value(backend, Options, kvdb_sqlite3),
    DbMod:open(Name,Options).

close({DbMod,Db}) ->
    DbMod:close(Db).    

-spec add_table(Db::db_ref(), Table::atom()) -> 
		       ok | {error, any()}.

add_table({DbMod,Db}, Table) 
  when is_atom(Table) ->
    DbMod:add_table(Db, Table).

-spec delete_table(Db::db_ref(), Table::atom()) -> 
			  ok | {error, any()}.

delete_table({DbMod,Db}, Table)
  when is_atom(Table) ->
    DbMod:delete_table(Db, Table).

-spec put(Db::db_ref(), Table::atom(), Key::binary(), Value::binary()) -> 
		 ok | {error, any()}.

put({DbMod,Db}, Table, Key, Value) 
  when is_atom(Table), is_binary(Key), is_binary(Value) ->
    DbMod:put(Db, Table, Key, Value).

-spec get(Db::db_ref(), Table::atom(), Key::binary()) ->
		 {ok, binary()} | {error,any()}.

get({DbMod,Db}, Table, Key)
  when is_atom(Table), is_binary(Key) ->
    DbMod:get(Db, Table, Key).

-spec delete(Db::db_ref(), Table::atom(), Key::binary()) ->
		    ok | {error, any()}.

delete({DbMod,Db}, Table, Key) 
  when is_atom(Table), is_binary(Key) ->
    DbMod:delete(Db, Table, Key).

-spec iterator(Db::db_ref(), Table::atom()) ->
		      {ok, itr_ref()}.

iterator({DbMod,Db}, Table)
  when is_atom(Table) ->
    DbMod:iterator(Db,Table).

-spec iterator_close(Db::db_ref(),Iter::itr_ref()) -> ok.

iterator_close({DbMod,Db}, Iter) ->
    DbMod:iterator_close(Db,Iter).

-spec first(Db::db_ref(), Iter::itr_ref()) ->
		   {ok,Key::binary()} |
		   {ok,Key::binary(),Value::binary()} |
		   done |
		   {error,any()}.

first({DbMod,Db}, Iter) ->
    DbMod:first(Db, Iter).

-spec last(Db::db_ref(), Iter::itr_ref()) ->
		   {ok,Key::binary()} |
		   {ok,Key::binary(),Value::binary()} |
		   done |
		   {error,any()}.

last({DbMod,Db}, Iter) ->
    DbMod:last(Db, Iter).
			 
-spec next(Db::db_ref(), Iter::itr_ref()) ->
		   {ok,Key::binary()} |
		   {ok,Key::binary(),Value::binary()} |
		   done |
		   {error,any()}.

next({DbMod,Db}, Iter) ->
    DbMod:next(Db, Iter).

-spec prev(Db::db_ref(), Iter::itr_ref()) ->
		  {ok,Key::binary()} |
		  {ok,Key::binary(),Value::binary()} |
		  done |
		  {error,any()}.

prev({DbMod,Db}, Iter) ->
    DbMod:prev(Db, Iter).




				
