-module(kvdb_usb_ets).

-compile(export_all).

run() ->
    run("/Volumes/USB20FD/t1").

run2() ->
    run("/Users/uwiger/tmp/usb_test/t1").

run3() ->
    run_mnesia("/Users/uwiger/tmp/usb_test/m1").

run(Dir) ->
    init(),
    create(usb_test, [{writes, 10}], Dir),
    write(usb_test, 30, 50).

run_mnesia(Dir) ->
    application:load(mnesia),
    application:set_env(mnesia, dir, Dir),
    application:set_env(mnesia, dump_log_write_threshold, 10),
    mnesia:create_schema([node()]),
    mnesia:start(),
    mnesia:create_table(t, [{disc_copies, [node()]}]),
    mnesia:wait_for_tables([t], 10000),
    ets:new(?MODULE, [ordered_set, named_table]),
    write_mnesia(30, 50).

init() ->
    ets:new(?MODULE, [ordered_set, named_table]),
    kvdb:start().

create(Name, Threshold, Dir) ->
    Base = filename:join(Dir, kvdb_lib:good_string(Name)),
    kvdb:open(
      Name, [{file, Base ++ ".db"},
	     {log_dir, Base ++ ".log"},
	     {backend, ets},
	     {save_mode, [on_close, on_switch]},
	     {log_threshold, Threshold}]).

write(Name, N, M) ->
    lists:foreach(
      fun({N1,M1}) ->
	      case M1 rem 2 of
		  0 ->
		      {T, ok} = timer:tc(?MODULE, write_set, [Name, N1]),
		      log(T, ins);
		  _ ->
		      {T, ok} = timer:tc(?MODULE, delete_set, [Name, N1]),
		      log(T, del)
	      end
      end, [{Nx, Mx} || Mx <- lists:seq(1,M),
			Nx <- lists:seq(1,N)]).

write_mnesia(N, M) ->
    lists:foreach(
      fun({N1,M1}) ->
	      case M1 rem 2 of
		  0 ->
		      {T, ok} = timer:tc(?MODULE, write_set_m, [N1]),
		      log(T, ins);
		  _ ->
		      {T, ok} = timer:tc(?MODULE, delete_set_m, [N1]),
		      log(T, del)
	      end
      end, [{Nx, Mx} || Mx <- lists:seq(1,M),
			Nx <- lists:seq(1,N)]).

log(T, Op) ->
    ets:insert(?MODULE, {erlang:now(), {T, Op}}),
    io:fwrite("~p: ~p~n", [Op, T]).

write_set(Name, N) ->
    kvdb_trans:run(
      Name, fun(_) ->
		    kvdb:add_table(Name, Tab = "t_" ++ integer_to_list(N),
				   [{encoding,sext}]),
		    R = [kvdb:put(Name, Tab, {N1, a}) || N1 <- lists:seq(1,10)],
		    true = lists:all(fun(X) -> X == ok end, R),
		    ok
	    end).
write_set_m(N) ->
    mnesia:activity(
      transaction,
      fun() ->
	      Tab = iolist_to_binary(["t_", integer_to_list(N)]),
	      R = [mnesia:write({t, {Tab,N1}, a}) ||
		      N1 <- lists:seq(1,12)],
	      true = lists:all(fun(X) -> X == ok end, R),
	      ok
      end).

delete_set(Name, N) ->
    kvdb_trans:run(
      Name, fun(_) ->
		    kvdb:delete_table(Name, "t_" ++ integer_to_list(N))
	    end).

delete_set_m(N) ->
    mnesia:activity(
      transaction,
      fun() ->
	      Tab = iolist_to_binary(["t_", integer_to_list(N)]),
	      Objs = mnesia:match_object({t, {Tab,'_'}, '_'}),
	      [mnesia:delete({t, element(1,O)}) || O <- Objs],
	      ok
      end).
