-module(kvdb_trans_tests).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

fill_test_() ->
    {setup,
     fun() ->
	     ?debugVal(application:start(gproc)),
	     dbg:tracer(),
	     %% dbg:tpl(kvdb_server,x),
	     %% dbg:tp(kvdb_lib,x),
	     %% dbg:tpl(kvdb_lib,threshold_reached,x),
	     %% dbg:tp(kvdb_direct,x),
	     %% dbg:tp(kvdb,x),
	     %% dbg:tp(gproc,x),
	     %% dbg:tp(disk_log,x),
	     dbg:tpl(kvdb_ets,x),
	     dbg:tp(ets,file2tab,x),
	     dbg:tp(ets,tab2file,x),
	     dbg:p(all,[c]),
	     ?debugVal(application:start(kvdb)),
	     ok
     end,
     fun(_) ->
	     ?debugVal(application:stop(kvdb)),
	     ?debugVal(application:stop(gproc))
     end,
     [{foreachx,
       fun({N,Opts,D}) ->
	       open_db(N, Opts, D)
       end,
       [{{N,Opts,D}, fun(_, Db) ->
			     [?_test(?debugVal(fill_db(N, Db, Opts, D)))]
		     end} ||
	   {N,Opts,D} <- [new_opts(foo_10, 10)]]
      }]}.

open_db(N, Opts, D) ->
    {ok, Db} = kvdb:open(N, [{log_dir, filename:join(D, "kvdb.log")},
			     {filename, filename:join(D, "kvdb.tab")}
			     | Opts]),
    Db.

new_opts(Name, N) ->
    {Name, [{backend, ets},
	    {log_threshold, [{writes,N}]}], dirname(Name)}.

dirname(Name) ->
    {_,S,U} = erlang:now(),
    filename:join("/tmp", atom_to_list(Name)
		  ++ "." ++ integer_to_list(S) ++ "." ++ integer_to_list(U)).


fill_db(Name, Db, Opts, D) ->
    ?assertMatch(ok, kvdb:add_table(Name, t, [{encoding,sext}])),
    Objs = [{N,a} || N <- lists:seq(1,30)],
    lists:foreach(
      fun(Obj) ->
	      kvdb:put(Name, t, Obj)
      end, Objs),
    kvdb:close(Name),
    ?debugFmt("DB closed. Trying to reopen...~n", []),
    open_db(Name, Opts, D),
    ?assertMatch({Objs, _}, kvdb:prefix_match(Name, t, '_', infinity)).

-endif.
