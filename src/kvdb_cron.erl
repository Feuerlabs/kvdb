%%---- BEGIN COPYRIGHT -------------------------------------------------------
%%
%% Copyright (C) 2012 Feuerlabs, Inc. All rights reserved.
%%
%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at http://mozilla.org/MPL/2.0/.
%%
%%---- END COPYRIGHT ---------------------------------------------------------
%% @author Ulf Wiger <ulf@feuerlabs.com>
%% @doc Persistent timers for KVDB
%%
%% Grammar for timer expressions:
%%
%% <verbatim>```
%% <expr> ::= "{" <time> "}"
%%          | "{" <time> ";" <repeat> "}"
%%          | "{" <time> ";" <repeat> ";" <until> "}"
%%
%% <time>    ::= "in" <in> | "at" <at>
%% <in>      ::= <int> | <in_expr>
%%             | <in_expr> "," <in>
%% <in_expr> ::= <int> <unit>
%%             | <ldom>
%% <at>      ::= <at_expr>
%%             | <at_expr> "," <at>
%% <at_expr> ::= <date_expr> | <time_expr>
%% <date_expr> ::= <int> "/" <int> "/" <int>
%%               | <int> "-" <int> "-" <int>
%% <time_expr> ::= <int> ":" <int> ":" <int>
%%               | <int> ":" <int> ":" <fixnum>
%% <repeat>  ::= "repeat"
%%             | "once"
%%             | <int> "times"
%%             | "each" <each>
%%             | "daily" | "weekly" | "monthly" | "yearly" | "annually"
%%             | <ldom>
%% <until>   ::= <until_expr>
%%             | "until" <until_expr>
%% <until_expr> ::= "forever"
%%                | <at>
%% <each>    ::= <each_expr>
%%             | <each_expr> "," <each>
%% <each_expr> ::= <int> <unit>
%% <ldom>    ::= "last_day_of_month" | "ldom"
%% <unit>    ::= "ms"
%%             | "sec" | "secs" | "seconds"
%%             | "min" | "minutes"
%%             | "hr" | "hrs" | "hours"
%%             | "mo" | "months"
%%             | "y" | "yr" | "years"
%% '''</verbatim>
%% @end
-module(kvdb_cron).
-behaviour(gen_server).

-export([create_crontab/2]).
-export([start_link/2,
	 add/7, add/8,
	 delete/3, delete/4,
	 delete_abs/3,
	 set_timers/1]).

-export([init/1,
	 handle_call/3,
	 handle_cast/2,
	 handle_info/2,
	 terminate/2,
	 code_change/3]).
-export([init_meta/1]).
-export([testf/0]).
-compile(export_all).

-include_lib("stdlib/include/qlc.hrl").
-include_lib("parse_trans/include/exprecs.hrl").
-include_lib("kvdb.hrl").
-include_lib("lager/include/log.hrl").

-export_records([job]).

-define(SERVER, ?MODULE).
-define(TAB, ?MODULE).

-record(st, {db,
	     tabs = []}).

-record(tabst, {tab, next, tref, pid, check = false}).

-record(job, {mfa,
	      interval,
	      repeat,
	      from,
	      until,
	      next,
	      spec}).

-define(CURRENT_TIME, erlang:universaltime()).

-define(CRON_META, <<"kvdb__CRON">>).

init_meta(Db) ->
    kvdb:add_table(Db, ?CRON_META, [{type, set},
				    {encoding, sext}]).

create_crontab(Db, Tab) ->
    kvdb:in_transaction(
      Db,
      fun(Db1) ->
	      ok = kvdb:add_table(Db1, Tab, [{type, {keyed, fifo}},
					     {encoding, {sext, sext, sext}}]),
	      kvdb:put(Db1, ?CRON_META, {{table, Tab}, []})
      end).

testf() ->
    io:fwrite("~nCRON!! ~p~n", [kvdb_lib:timestamp_to_datetime(
				  kvdb_lib:timestamp())]).

%%% @spec (Db, Tab, When::timespec(), Key::any(),
%%%        M::atom(), F::atom(), As::[any()]) -> ok | error()
%%%
add(Db, Tab, When, Options, M, F, As) ->
    add(Db, Tab, <<>>, When, Options, M, F, As).

add(Db, Tab, Q, When, Options, M, F, As) when is_list(When);
					      is_binary(When);
					      is_integer(When) ->
    ?debug("add(~p, ~p, ~p, ~p, ~p, ~p, ~p, ~p)~n",
	   [kvdb:db_name(Db),Tab,Q,When,Options,M,F,As]),
    case new_job(When, Options, M, F, As) of
	{NextT,_,_} = Entry ->
	    ?debug("pushing entry ~p~n", [Entry]),
	    {ok, Key} = kvdb:push(Db, Tab, Q, Entry),
	    cast(Db, {next_time, NextT, Tab, Q}),
	    {ok, Key};
	[] ->
	    false
    end.

delete(Db, Tab, Key) ->
    delete(Db, Tab, <<>>, Key).

delete(Db, Tab, Q, Key) ->
    kvdb:in_transaction(
      Db,
      fun(Db1) ->
	      ListRes = kvdb:list_queue(
			  Db1, Tab, Q, fun(_, QK, Obj) ->
					       {keep, setelement(1,Obj,QK)}
				       end, false, 50),
	      search_delete(ListRes, Db1, Tab, Q, Key)
      end).


set_timers(Db) ->
    call(Db, set_timers).

search_delete({Entries, Cont}, Db, Tab, Q, Key) ->
    case find_job(Entries, Key) of
	false ->
	    search_delete(Cont(), Db, Tab, Q, Key);
	QKey ->
	    delete_abs(Db, Tab, QKey)
    end;
search_delete(done, _, _, _, _) ->
    ok.

find_job([{Key, As, _}|T], Id) ->
    case lists:member({id, Id}, As) of
	true ->
	    Key;
	false ->
	    find_job(T, Key)
    end;
find_job([], _) ->
    false.



new_job(Spec0, Options0, M, F, As) ->
    Spec = normalize_spec(Spec0),
    Now = kvdb_lib:timestamp(),
    MFA = valid_function(M, F, As),
    Options = valid_options(Options0),
    Job = #job{mfa = MFA, spec = Spec},
    case valid_time(Spec, Now, Job) of
	#job{next = never} ->
	    [];
	#job{next = NextTime} = Job1 when is_integer(NextTime) ->
	    {NextTime, Options, Job1}
    end.

normalize_spec(W) when is_binary(W) ->
    parse_spec(binary_to_list(W));
normalize_spec(W) when is_list(W) ->
    parse_spec(W);
normalize_spec({Time, Repeat, Until}) ->
    {normalize_(Time), normalize_(Repeat), normalize_until(normalize_(Until))};
normalize_spec(W) when is_integer(W) ->
    {{in, [{W, ms}]}, [], []};
normalize_spec({in, _} = In) ->
    {In, [], []};
normalize_spec({at, _} = At) ->
    {At, [], []};
normalize_spec({Time, Repeat}) when Time =/= in; Time =/= at ->
    {Time, Repeat, []}.

parse_spec(W) ->
    case kvdb_cron_scan:string(W) of
	{ok, Tokens, _} ->
	    case kvdb_cron_parse:parse(Tokens) of
		{ok, Form} ->
		    normalize_form(Form);
		ParseErr ->
		    error(ParseErr)
	    end;
	ScanErr ->
	    error(ScanErr)
    end.

normalize_form({Time, Repeat, Until}) ->
    {normalize_(Time), normalize_(Repeat), normalize_until(normalize_(Until))}.

normalize_until(forever) -> [];
normalize_until(Other) ->
    Other.

normalize_({nil,_}) -> [];
normalize_({integer,_,I}) -> I;
normalize_({fixnum,_,{I,F}}) -> {I,F};
normalize_({Op, _, Args}) when is_atom(Op), is_list(Args) ->
    {Op, [normalize_(A) || A <- Args]};
normalize_({A,B,C}) when is_tuple(A), is_tuple(B), is_tuple(C) ->
    {normalize_(A), normalize_(B), normalize_(C)};
normalize_({T1,T2}) when is_tuple(T1), is_tuple(T2) ->
    {normalize_(T1), normalize_(T2)};
normalize_({Op, _, Arg}) when is_atom(Op), is_tuple(Arg) ->
    {Op, normalize_(Arg)};
normalize_({Op, L}) when is_atom(Op), is_integer(L) ->
    Op;
normalize_(X) ->
    X.


reschedule_job(#job{next = Last, spec = Spec} = Job,
	       Tab, Q, Db, Opts, Parent) ->
    ?debug("reschedule_job(~p, ~p, ~p)~n", [Job, Tab, Q]),
    if is_integer(Spec) ->
	    ?debug("is integer - will not reschedule~n", []),
	    ok;
       true ->
	    reschedule_(decr_repeat(Job), Last, Tab, Q, Db, Opts, Parent)
    end.

reschedule_(#job{next = never}, _, _, _, _, _, _) ->
    ok;
reschedule_(#job{spec = Spec} = Job, Last, Tab, Q, Db, Opts, Parent) ->
    ?debug("reschedule(~p, ~p, ~p, ~p... Parent=~p)~n", [Job,Last,Tab,Q,Parent]),
    case valid_time(Spec, Last, Job) of
	#job{next = never} ->
	    ?debug("next = never~n", []),
	    ok;
	#job{next = NextTime} = Job1 when is_integer(NextTime) ->
	    ?debug("Rescheduled job: ~p (Now = ~p)~n",
		   [Job1, kvdb_lib:timestamp()]),
	    Res = (catch kvdb:push(Db, Tab, Q, {NextTime, Opts, Job1})),
	    ?debug("push result ~p~n", [Res]),
	    gen_server:cast(Parent, {next_time, NextTime, Tab, Q})
    end.


%% delete(Db, Tab, Key) ->
%%     call(Db, {delete, Tab, Key}).


start_link(Db, Options) ->
    gen_server:start_link(?MODULE, {Db, Options}, []).

init({Db, Options}) ->
    gproc:reg(regname(Db)),
    Tabs = case proplists:get_value(set_timers, Options, true) of
	       true ->
		   set_timers_(Db);
	       false ->
		   []
	   end,
    {ok, #st{db = Db, tabs = Tabs}}.

cast(Db, Msg) ->
    gen_server:cast(gproc:where(regname(Db)), Msg).

call(Db, Req) ->
    gen_server:call(gproc:where(regname(Db)), Req).


%% ===
handle_call(set_timers, _From, #st{db = Db} = S) ->
    Tabs = set_timers_(Db),
    {reply, ok, S#st{tabs = Tabs}};
handle_call(_Req, _From, St) ->
    {noreply, St}.

handle_cast({next_time, TS, Tab, Q}, #st{db = Db, tabs = Tabs} = S) ->
    ?debug("{next_time, ~p, ~p, ~p}~n", [TS, Tab, Q]),
    TQ = {Tab,Q},
    Tabs1 =
	with_tab(
	  fun(undefined) ->
		  ?debug("no previous tab st ~p~n", [{Tab,Q}]),
		  set_new_timer(TS, TQ, #tabst{tab = TQ}, Db);
	     (#tabst{next = T, tref = TRef} = TSt) when is_integer(T), T < TS ->
		  ?debug("canceling previous timer, Tab=~p~n", [TQ]),
		  erlang:cancel_timer(TRef),
		  set_new_timer(TS, TQ, TSt, Db);
	     (#tabst{next = T} = TSt) when is_integer(T), T >= TS ->
		  TSt
	  end, TQ, Tabs),
    ?debug("Tabs1 = ~p~n", [Tabs1]),
    {noreply, S#st{tabs = Tabs1}};
handle_cast(_Msg, St) ->
    ?debug("unknown cast: ~p~n", [_Msg]),
    {noreply, St}.

handle_info({'DOWN', _, process, Pid, _}, #st{db = Db, tabs = Tabs} = S) ->
    case lists:keyfind(Pid, #tabst.pid, Tabs) of
	false -> {noreply, S};
	#tabst{tab = Tab, check = Check} = TSt ->
	    TSt1 = TSt#tabst{pid = undefined},
	    if Check -> dispatch(
			  kvdb_lib:timestamp(), Tab, TSt1, Db);
	       true ->
		    {noreply, S#st{tabs = set_tab_state(Tab, TSt1, Tabs)}}
	    end
    end;
handle_info({timeout, _Ref, {TS, Tab, Q}}, #st{db = Db, tabs = Tabs} = S) ->
    ?debug("timeout: Tab = ~p, Q = ~p~n", [Tab, Q]),
    Tabs1 =
	with_tab(
	  fun(#tabst{} = TabSt) ->
		  dispatch(TS, {Tab,Q}, TabSt, Db);
	     (undefined) ->
		  ?debug("no matching tab state~n", []),
		  undefined
	  end, {Tab,Q}, Tabs),
    {noreply, S#st{tabs = Tabs1}};
handle_info(_Msg, St) ->
    ?debug("got unknown msg: ~p~n", [_Msg]),
    {noreply, St}.

terminate(_Reason, _St) ->
    ok.

code_change(_FromVsn, St, _Extra) ->
    {ok, St}.

%% ===

set_timers_(Db) ->
    try
	Pat = [{ {{table,'$1'},'_'},[], ['$1'] }],
	kvdb:in_transaction(
	  Db,
	  fun(Db1) ->
		  set_timers_(kvdb:select(Db1, ?CRON_META, Pat), Db, [])
	  end)
    catch
	error:Reason ->
	    ?error("ERROR setting ~p timers: ~p~n", [?MODULE, Reason]),
	    ok
    end.

set_timers_({Tabs, Cont}, Db, Acc) ->
    NewAcc =
	lists:foldl(fun(T, Acc1) ->
			    First = kvdb:first_queue(Db, T),
			    revisit_queues(First, Db, T, Acc1)
		    end, Acc, Tabs),
    set_timers_(Cont(), Db, NewAcc);
set_timers_(done, _, Acc) ->
    Acc.

revisit_queues({ok, Q}, Db, Tab, Acc) ->
    NewAcc =
	with_tab(
	  fun(undefined) ->
		  TS = kvdb_lib:timestamp(),
		  case kvdb:peek(Db, Tab, Q) of
		      {ok, #q_key{key = TS1}, _} ->
			  Time = erlang:max(
				   0, erlang:min((TS1-TS) div 1000,
						 timer:minutes(10))),
			  TRef = erlang:start_timer(Time, self(), {TS1, Tab, Q}),
			  #tabst{tab = {Tab,Q},
				 next = TS1,
				 tref = TRef};
		      done ->
			  undefined
		  end;
	     (#tabst{} = TSt) ->
		  TSt
	  end, {Tab, Q}, Acc),
    revisit_queues(kvdb:next_queue(Db, Tab, Q), Db, Tab, NewAcc);
revisit_queues(done, _, _, Acc) ->
    Acc.



set_new_timer(TS, {Tab, Q}, #tabst{} = TabSt, Db) ->
    Now = kvdb_lib:timestamp(),
    ?debug("set_new_timer(TS = ~p); Now = ~p~n", [TS, Now]),
    case (TS - Now) div 1000 of
	MS when MS > 0 ->
	    TRef = erlang:start_timer(MS, self(), {TS,Tab, Q}),
	    TabSt#tabst{next = TS,
			tref = TRef};
	MS when MS =< 0 ->
	    %% already expired
	    dispatch(TS, {Tab, Q}, TabSt, Db)
    end.

with_tab(F, Tab, Tabs) when is_function(F, 1) ->
    set_tab_state(Tab, F(get_tab_state(Tab, Tabs)), Tabs).

get_tab_state(Tab, Tabs) ->
    case lists:keyfind(Tab, #tabst.tab, Tabs) of
	false   -> undefined;
	#tabst{} = St -> St
    end.

set_tab_state(Tab, #tabst{} = St, Tabs) ->
    lists:keystore(Tab, #tabst.tab, Tabs, St);
set_tab_state(Tab, undefined, Tabs) ->
    lists:keydelete(Tab, #tabst.tab, Tabs).


dispatch(TS, {Tab,Q}, #tabst{pid = undefined} = TabSt, Db) ->
    Me = self(),
    {Pid,_} = spawn_monitor(
		fun() ->
			do_dispatch(TS, Tab, Q, Db, Me)
		end),
    TabSt#tabst{pid = Pid, check = false};
dispatch(_TS, _, #tabst{pid = P} = TabSt, _) when is_pid(P) ->
    TabSt#tabst{check = true}.

do_dispatch(TS, Tab, Q, Db, Parent) ->
    kvdb:in_transaction(
      Db, fun(Db1) ->
		  do_dispatch_(TS, Tab, Q, Db1, Parent)
	  end).

do_dispatch_(TS, Tab, Q, Db, Parent) ->
    ?debug("do_dispatch_(~p, ~p, ~p, ~p, ~p)~n", [TS,Tab,Q,Db,Parent]),
    case kvdb:peek(Db, Tab, Q) of
	{ok, #q_key{key = TS1} = QK, {_,Opts,Job}} when TS1-TS < 1000 ->
	    ?debug("peek(~p, ~p) -> ~p; ~p~n", [kvdb:db_name(Db),
						Tab,QK,Job]),
	    case should_i_call(TS1 - TS, Opts) of
		true ->
		    ?debug("should_i_call(TS1 = ~p) -> true~n", [TS1]),
		    call_job(Job);
		false ->
		    ?debug("should_i_call(TS1 = ~p) -> false~n", [TS1]),
		    ignore
	    end,
	    %% even if we didn't call, we attempt to reschedule
	    delete_abs(Db, Tab, QK),
	    reschedule_job(Job, Tab, Q, Db, Opts, Parent),
	    do_dispatch_(TS, Tab, Q, Db, Parent);
	{ok, #q_key{key = TS1} = QK, _} ->
	    ?debug("peek(~p, ~p) -> ~p; future job~n",
		   [kvdb:db_name(Db), Tab, QK]),
	    gen_server:cast(Parent, {next_time, TS1, Tab, Q});
	done ->
	    ok
    end.

delete_abs(Db, Tab, QK) ->
    kvdb:delete(Db, Tab, QK).

should_i_call(Diff, _Opts) when Diff > -5000 ->
    true;
should_i_call(Diff, Opts) ->
    case proplists:get_value(call_if_delayed, Opts, false) of
	true -> true;
	Limit when is_integer(Limit) ->
	    Limit > -Diff;
	false -> false
    end.

call_job(#job{mfa = {M, F, A}} = J) ->
    try
	R = apply(M, F, A),
	?debug("~p Job ~p -> ~p~n", [?MODULE, J, R])
    catch
	error:Err ->
	    ?debug("~p Job ~p -> ERROR:~p~n~p~n",
		   [?MODULE, J, Err, erlang:get_stacktrace()]);
	exit:Ex ->
	    ?debug("~p Job ~p -> EXIT:~p~n~p~n",
		   [?MODULE, J, Ex, erlang:get_stacktrace()]);
	throw:Th ->
	    ?debug("~p Job ~p -> THROW:~p~n~p~n",
		   [?MODULE, J, Th, erlang:get_stacktrace()])
    end.

regname(Db) ->
    Name = kvdb:db_name(Db),
    {n, l, {?MODULE, Name}}.


decr_repeat(#job{repeat = 0} = J) ->
    J#job{next = never};
decr_repeat(#job{repeat = {times,N}} = J) when is_integer(N) ->
    case N-1 of
	N1 when N1 > 0 ->
	    J#job{repeat = {times,N1}};
	0 ->
	    J#job{next = never, repeat = {times,0}}
    end;
decr_repeat(J) ->
    J.

valid_time({TimeDef, RepeatDef, UntilDef}, Now, Job) ->
    TS = get_datetime(TimeDef, Now),
    Repeat = valid_repeat(RepeatDef, Job),
    Int = valid_interval(TimeDef, RepeatDef, Job),
    Until = valid_until(UntilDef, Repeat, Int, TS),
    Job#job{next = TS, repeat = Repeat, interval = Int, until = Until}.

valid_repeat(_, #job{repeat = R}) when R=/=undefined ->
    R;
valid_repeat(Spec, #job{repeat = undefined}) ->
    case Spec of
	[] -> {times, 1};
	repeat -> repeat;
	{each, _} -> Spec;
	{times, _} -> Spec;
	Other ->
	    error({bad_repeat, Other})
    end.

valid_interval(_, _, #job{interval = I}) when I =/= undefined ->
    I;
valid_interval(Time, Repeat, #job{interval = undefined}) ->
    case Repeat of
	{each, Each} ->
	    [valid_each_(E) || E <- Each];
	[] ->
	    undefined;
	_ when Repeat==repeat; element(1, Repeat) == times ->
	    case Time of
		{in, In} ->
		    In;
		_ ->
		    error(unknown_interval)
	    end
    end.

valid_each_(E) ->
    case E of
	{N,Unit}=I when is_integer(N), N > 0, is_atom(Unit) ->
	    case lists:member(Unit, [ms, seconds, minutes, hours,
				     days, weeks, months, years,
				     last_day_of_month]) of
		true ->
		    {N,Unit};
		false ->
		    erlang:error({bad_interval, I})
	    end;
	I when is_atom(I) ->
	    case I of
		daily   -> {1, days};
		weekly  -> {1, weeks};
		monthly -> {1, months};
		yearly  -> {1, years};
		last_day_of_month -> {1, last_day_of_month};
		_ ->
		    erlang:error({bad_interval, I})
	    end
    end.

valid_until(Spec, Times, Int, TS) ->
    case Spec of
	{at, At} ->
	    at_spec(At, TS);
	[] ->
	    case Times of
		{times,1} ->
		    {undefined, undefined};
		{times, N} when is_integer(N), N > 1 ->
		    lists:foldl(
		      fun(_, Last) ->
			      step_interval(Int, Last)
		      end, TS, lists:seq(1,N-1));
		_ ->
		    {undefined, undefined}
	    end;
	{date, UntilD} ->
	    DT = {valid_date(UntilD), {23,59,59}},
	    kvdb_lib:datetime_to_timestamp({DT, 0});
	{time, {_,_,_} = UntilT} ->
	    {{Date,_},US} = kvdb_lib:timestamp_to_datetime(TS),
	    kvdb_lib:datetime_to_timestamp({{Date, UntilT}, US})
	%% {{_,_,_},{_,_,_}} = UntilDt ->
	%%     kvdb_lib:datetime_to_timestamp({UntilDt, 0});
	%% undefined ->
	%%     case proplists:get_value(times, Spec, undefined) of
	%% 	undefined ->
	%% 	    {undefined, undefined};
	%% 	1 ->
	%% 	    TS;
	%% 	N when is_integer(N), N > 1 ->
	%%     end
    end.

get_datetime({in, In}, Now) ->
    in_spec(In, Now);
get_datetime({at, At}, Now) ->
    at_spec(At, Now);
get_datetime(Other, _) ->
    error(invalid_time_spec, Other).

in_spec(In, TS) ->
    lists:foldl(
      fun({N, Unit} = Step, TS1) when is_integer(N), is_atom(Unit) ->
	      step_interval(Step, TS1);
	 (MS, TS1) when is_integer(MS) ->
	      step_interval({MS,ms}, TS1);
	 (Other, _) ->
	      error(invalid_spec, [in, Other])
      end, TS, In).

at_spec(At, TS) ->
    lists:foldl(
      fun({{_,_,_},{_,_,_}} = DT, _) ->
	      throw({result, kvdb_lib:datetime_to_timestamp({DT,0})});
	 ({time,{_,_,_}=T}, TS1) ->
	      {{D,_},US} = kvdb_lib:datetime_to_timestamp(TS1),
	      kvdb_lib:timestamp_to_datetime({{D,T},US});
	 ({date,{_,_,_}=D}, TS1) ->
	      {{_,T},US} = kvdb_lib:datetime_to_timestamp(TS1),
	      kvdb_lib:timestamp_to_datetime({{D,T},US});
	 ({Part,X}, TS1) when Part==year; Part==month; Part==day ->
	      {{D,T},US} = kvdb_lib:datetime_to_timestamp(TS1),
	      kvdb_lib:timestamp_to_datetime(
		{{set_date_part(Part, X, D),T},US});
	 ({Part,X}, TS1) when Part==hour; Part==minute; Part==second ->
	      {{D,T},US} = kvdb_lib:datetime_to_timestamp(TS1),
	      kvdb_lib:timestamp_to_datetime(
		{{D,set_time_part(Part,X,T)},US});

	 (last_day_of_the_month, TS1) ->
	      {{{Y,Mo,_},{H,Mi,S}},US} = kvdb_lib:datetime_to_timestamp(TS1),
	      D1 = calendar:last_day_of_the_month(Y,Mo),
	      kvdb_lib:timestamp_to_datetime(
		{{{Y,Mo,D1},{H,Mi,S}}, US})
      end, TS, At).

set_date_part(year , Y, {_,M,D}) -> {Y,M,D};
set_date_part(month, M, {Y,_,D}) -> {Y,M,D};
set_date_part(day  , D, {Y,M,_}) -> {Y,M,D}.

set_time_part(hour  , H, {_,M,S}) -> {H,M,S};
set_time_part(minute, M, {H,_,S}) -> {H,M,S};
set_time_part(second, S, {H,M,_}) -> {H,M,S}.


valid_date(Date) ->
    case calendar:valid_date(Date) of
	true ->
	    Date;
	false ->
	    erlang:error({invalid_date, Date})
    end.

valid_datetime(DateTime) ->
    try DtSecs = dt2gs(DateTime),
	{DateTime, DtSecs}
    catch
	error:_ ->
	    erlang:error({invalid_time, DateTime})
    end.

get_opt([], _, Default) ->
    Default;
get_opt([K|Alt], L, Default) ->
    case proplists:get_value(K, L) of
	undefined ->
	    get_opt(Alt, L, Default);
	Other ->
	    Other
    end.



valid_function(M, F, As) when is_atom(M), is_atom(F), is_list(As) ->
    {M, F, As};
valid_function(_, _, _) ->
    erlang:error(invalid_function).



valid_options([{call_if_delayed, B}=O|Os]) when is_boolean(B) ->
    [O|valid_options(Os)];
valid_options([{id, _} = O|Os]) ->
    [O|valid_options(Os)];
valid_options([O|_]) ->
    erlang:error({invalid_option, O});
valid_options([]) ->
    [].



next_event(#job{next = never}, _) -> never;
next_event(#job{until = Until} = C, CurSecs)
  when is_integer(Until), Until < CurSecs ->
    {true, C#job{next = never}};
next_event(#job{from = From, next = Next} =C, CurSecs)
  when From > CurSecs ->
    {From == Next, C#job{next = From}};
next_event(#job{next = Next} = C, CurSecs) when Next > CurSecs ->
    {false, C};
next_event(#job{interval = Int,
		until = Until,
		next = Last} = C, _CurSecs) ->
    NewNext =
	case step_interval(Int, Last) of
	    Next when Next > Until ->
		never;
	    Next when Next > Last ->  % guard is a sanity check
		Next
	end,
    {true, C#job{next = NewNext}}.

step_interval({N,Unit}, TS) when N > 0 ->
    case Unit of
	ms      -> TS + (N * 1000);
	seconds -> TS + (N * 1000000);
	minutes -> TS + (N * 60 * 1000000);
	hours   -> TS + (N * 3600 * 1000000);
	days ->
	    {{Date,Time},US} = to_dt(TS),
	    to_ts({{gd2d(d2gd(Date) + N), Time}, US});
	weeks ->
	    {{Date,Time},US} = to_dt(TS),
	    to_ts({{gd2d(d2gd(Date) + (7*N)), Time}, US});
	months ->
	    {{Date,Time},US} = to_dt(TS),
	    {Y,M,D} = Date,
	    {Y1,M1} = next_month(N, Y, M),
	    to_ts({{{Y1,M1,D}, Time}, US});
	last_day_of_month ->
	    {{Date,Time},US} = to_dt(TS),
	    {Y,M,_} = Date,
	    {Y1,M1} = next_month(N, Y, M),
	    D1 = calendar:last_day_of_the_month(Y1,M1),
	    to_ts({{{Y1,M1,D1}, Time}, US});
	years ->
	    {{Date,Time},US} = to_dt(TS),
	    {Y,M,D} = Date,
	    to_ts({{{Y+N,M,D}, Time}, US})
    end;
step_interval(L, TS) when is_list(L) ->
    lists:foldl(fun step_interval/2, TS, L).



%%% Helper functions to find next time in an interval

to_ts(DT) ->
    kvdb_lib:datetime_to_timestamp(DT).
to_dt(TS) ->
    kvdb_lib:timestamp_to_datetime(TS).

next_month(N, Y, M) ->
    case M+N of
	M1 when M1 > 12 ->
	    {Y+(M1 div 12), M1 rem 12};
	M1 ->
	    {Y,M1}
    end.


%%% mnemonics to prevent carpal-tunnel syndrome

d2gd(Date) ->
    calendar:date_to_gregorian_days(Date).

gd2d(Days) ->
    calendar:gregorian_days_to_date(Days).

dt2gs(DateTime) ->
    calendar:datetime_to_gregorian_seconds(DateTime).

gs2dt(Seconds) ->
    calendar:gregorian_seconds_to_datetime(Seconds).
