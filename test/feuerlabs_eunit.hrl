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
%%%
%%% Helpful macros for EUnit tests

%% ?_t(E): Like ?test(E), but with a 1-minute timeout, and generates a stack
%% trace after exceptions.
-define(_t(E), {timeout,60000,
		[?_test(try E catch error:_R_ ->
				      error({_R_, erlang:get_stacktrace()})
			      end)]}).


%% ?dbg(E): executes test case, printing the result on success, and a
%% thorough error report on failure.
-define(dbg(E),
	(fun() ->
		 try (E) of
		     __V ->
			 ?debugFmt(<<"~s = ~P">>, [(??E), __V, 15]),
			 __V
		 catch
		     error:__Err ->
			 io:fwrite(user,
				   "FAIL: test = ~s~n"
				   "Error = ~p~n"
				   "Trace = ~p~n", [(??E), __Err,
						    erlang:get_stacktrace()]),
			 error(__Err)
		 end
	  end)()).

%% ?trace(Mods, E) Enables a call trace on the modules (or {Mod,Fun} patterns)
%% in Mods, evaluates expression E and then stops the trace.
-define(trace(Mods, Expr), begin dbg:tracer(),
				 lists:foreach(
				   fun(_M_) when is_atom(_M_) ->
					   dbg:tpl(_M_,x);
				      ({_M_,_F_}) ->
					   dbg:tpl(_M_,_F_,x)
				   end, Mods),
				 dbg:p(all,[c]),
				 try Expr
				 after
				     lists:foreach(
				       fun(_M_) when is_atom(_M_) ->
					       dbg:ctpl(_M_);
					  ({_M_,_F_}) ->
					       dbg:ctpl(_M_,_F_)
				       end, Mods),
				     dbg:stop()
				 end
			   end).
