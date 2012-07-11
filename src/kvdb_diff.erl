%%%---- BEGIN COPYRIGHT -------------------------------------------------------
%%%
%%% Copyright (C) 2012 Feuerlabs, Inc. All rights reserved.
%%%
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at http://mozilla.org/MPL/2.0/.
%%%
%%%---- END COPYRIGHT ---------------------------------------------------------
%%% @author Ulf Wiger <ulf@wiger.net>
%%% @author Tony Rogvall <tony@rogvall.se>
%%% @doc
%%%     Simple diff algorithm 
%%% @end
%%% Created : 17 Feb 2012 by Tony Rogvall <tony@rogvall.se>

-module(kvdb_diff).

-compile(export_all).

%%
%% diff to parts of a config tree like:
%% A = <<"/devices/123/config/candidate">>
%% B = <<"/devices/123/config/running">>
%%  paths(A,B)
%% return a list of items:
%% [ {add,{Key,As,Value,Tree}} | {delete,Key}, {update,Key,Value} ]
%% that when applied to A make that subtree equal to B's subtree
%% 
tree(A, B) ->
    tree_obj(A, prefix_read(A,A), B, prefix_read(B,B), []).

tree_obj(Ax, {A,AObj={Ak,_As,Av}}, Bx, {B,BObj={Bk,Bs,Bv}}, Acc) ->
    if A < B ->
	    tree_obj(Ax, prefix_next(Ax,Ak), Bx, {B,BObj},
		     [{delete,A}|Acc]);
       A > B ->
	    tree_obj(Ax, {A,AObj}, Bx, prefix_next(Bx,Bk),
		     [{add,{B,Bs,Bv}}|Acc]);
       A =:= B ->
	    Acc1 = if Av =:= Bv -> Acc;
		      true -> [{update,A,Bv}|Acc]
		   end,
	    tree_obj(Ax, prefix_next(Ax,Ak), Bx, prefix_next(Bx,Bk),Acc1)
    end;
tree_obj(_Ax, done, _Bx, done, Acc) ->
    Acc;
tree_obj(Ax, done, Bx, {B,{Bk,Bs,Bv}}, Acc) ->
    tree_obj(Ax, done, Bx, prefix_next(Bx,Bk),[{add,{B,Bs,Bv}}|Acc]);
tree_obj(Ax, {A,{Ak,_,_}}, Bx, done, Acc) ->
    tree_obj(Ax, prefix_next(Ax,Ak), Bx, done, [{delete,A}|Acc]).
    
prefix_read(Prefix, Key) ->
    case kvdb_conf:read(Key) of
	{ok, Obj} ->
	    {remove_prefix(Prefix,Key),Obj};
	{error,_} ->
	    prefix_next(Prefix, Key)
    end.

prefix_next(Prefix, Key) ->
    case kvdb_conf:next(Key) of
	{ok, Obj={Key1,_As,_Data}} ->
	    case prefix(Prefix, Key1) of
		true ->
		    {remove_prefix(Prefix,Key1), Obj};
		false ->
		    done
	    end;
	done ->
	    done
    end.

%% Prefix = <<"">> | << "abc" >> | << "abc*edfg" >>
remove_prefix(Prefix, Key) ->
    PrefixSz = byte_size(Prefix),
    case Key of
	<<Prefix:PrefixSz/binary,$*,Key1/binary>> ->
	    Key1;
	<<Prefix:PrefixSz/binary,Key1/binary>> ->
	    Key1
    end.
    

prefix(Key1, Key2) ->
    Key1Sz = byte_size(Key1),
    case Key2 of
	<<Key1:Key1Sz/binary,_/binary>> ->
	    true;
	_ ->
	    false
    end.

trim_prefix(A, At) ->
    Parts = binary:split(A, <<"*">>, [global]),
    trim_parts(Parts, At).

trim_parts([K|Ks], [{K,[],<<>>,T}]) ->
    trim_parts(Ks, T);
trim_parts([], T) ->
    T.


    


	    
	    
    



    

    
    
