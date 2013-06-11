%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 1996-2013. All Rights Reserved.
%%
%% The contents of this file are subject to the Erlang Public License,
%% Version 1.1, (the "License"); you may not use this file except in
%% compliance with the License. You should have received a copy of the
%% Erlang Public License along with this software. If not, it can be
%% retrieved online at http://www.erlang.org/.
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and limitations
%% under the License.
%%
%% %CopyrightEnd%
%%
%% ===================================================================
%% This code is a modified version of ets:tab2file/[2,3],
%% adding a 'sync' option. This allows the dumped file to be flushed
%% directly to disk. /Ulf Wiger <ulf@feuerlabs.com>
%% ===================================================================
-module(kvdb_ets_dumper).

-export([tab2file/2,
	 tab2file/3]).
-export([file2tab/1,
	 file2tab/2]).

-type tab()        :: atom() | tid().
%% a similar definition is also in erl_types
-opaque tid()      :: integer().

-define(MAJOR_F2T_VERSION,1).
-define(MINOR_F2T_VERSION,0).

-record(filetab_options,
        {
          object_count = false :: boolean(),
          md5sum       = false :: boolean(),
	  sync         = false :: boolean()  % UW addition
         }).


%% file2tab/[1,2] are just here for symmetry; they pass through to ets. /UW
-spec file2tab(Filename) -> {'ok', Tab} | {'error', Reason} when
      Filename :: file:name(),
      Tab :: tab(),
      Reason :: term().

file2tab(File) ->
    ets:file2tab(File).

-spec file2tab(Filename, Options) -> {'ok', Tab} | {'error', Reason} when
      Filename :: file:name(),
      Tab :: tab(),
      Options :: [Option],
      Option :: {'verify', boolean()},
      Reason :: term().

file2tab(File, Options) ->
    ets:file2tab(File, Options).


-spec tab2file(Tab, Filename) -> 'ok' | {'error', Reason} when
      Tab :: tab(),
      Filename :: file:name(),
      Reason :: term().

tab2file(Tab, File) ->
    tab2file(Tab, File, []).



-spec tab2file(Tab, Filename, Options) -> 'ok' | {'error', Reason} when
      Tab :: tab(),
      Filename :: file:name(),
      Options :: [Option],
      Option :: {'extended_info', [ExtInfo]},
      ExtInfo :: 'md5sum' | 'object_count',
      Reason :: term().

tab2file(Tab, File, Options) ->
    try
        {ok, FtOptions} = parse_ft_options(Options),
        file:delete(File),
        case file:read_file_info(File) of
            {error, enoent} -> ok;
            _ -> throw(eaccess)
        end,
        Name = make_ref(),
        case disk_log:open([{name, Name}, {file, File}, {repair, truncate }]) of
            {ok, Name} -> ok;
            {repaired, Name} -> ok;
            {error, Reason} ->
                throw(Reason)
        end,
        try
            Info0 = case ets:info(Tab) of
                       undefined ->
                           %% erlang:error(badarg, [Tab, File, Options]);
                           throw(badtab);
                       I ->
                           I
            end,
            Info = [list_to_tuple(Info0 ++
                                  [{major_version,?MAJOR_F2T_VERSION},
                                   {minor_version,?MINOR_F2T_VERSION},
                                   {extended_info,
                                    ft_options_to_list(FtOptions)}])],
            {LogFun, InitState} =
            case FtOptions#filetab_options.md5sum of
                true ->
                    {fun(Oldstate,Termlist) ->
                             {NewState,BinList} =
                                 md5terms(Oldstate,Termlist),
                             disk_log:blog_terms(Name,BinList),
                             NewState
                     end,
                     erlang:md5_init()};
                false ->
                    {fun(_,Termlist) ->
                             disk_log:log_terms(Name,Termlist),
                             true
                     end,
                     true}
            end,
            ets:safe_fixtable(Tab,true),
            {NewState1,Num} = try
                                  NewState = LogFun(InitState,Info),
                                  dump_file(
                                      ets:select(Tab,[{'_',[],['$_']}],100),
                                      LogFun, NewState, 0)
                              after
                                  (catch ets:safe_fixtable(Tab,false))
                              end,
            EndInfo =
            case  FtOptions#filetab_options.object_count of
                true ->
                    [{count,Num}];
                false ->
                    []
            end ++
            case  FtOptions#filetab_options.md5sum of
                true ->
                    [{md5,erlang:md5_final(NewState1)}];
                false ->
                    []
            end,
            case EndInfo of
                [] ->
                    ok;
                List ->
                    LogFun(NewState1,[['$end_of_table',List]])
            end,
	    %% UW addition:
	    case FtOptions#filetab_options.sync of
		true ->
		    disk_log:sync(Name);
		false ->
		    ok
	    end,
            disk_log:close(Name)
        catch
            throw:TReason ->
                disk_log:close(Name),
                file:delete(File),
                throw(TReason);
            exit:ExReason ->
                disk_log:close(Name),
                file:delete(File),
                exit(ExReason);
            error:ErReason ->
                disk_log:close(Name),
                file:delete(File),
                erlang:raise(error,ErReason,erlang:get_stacktrace())
        end
    catch
        throw:TReason2 ->
            {error,TReason2};
        exit:ExReason2 ->
            {error,ExReason2}
    end.

dump_file('$end_of_table', _LogFun, State, Num) ->
    {State,Num};
dump_file({Terms, Context}, LogFun, State, Num) ->
    Count = length(Terms),
    NewState = LogFun(State, Terms),
    dump_file(ets:select(Context), LogFun, NewState, Num + Count).


ft_options_to_list(#filetab_options{md5sum = MD5, object_count = PS}) ->
    case PS of
        true ->
            [object_count];
        _ ->
            []
    end ++
        case MD5 of
            true ->
                [md5sum];
            _ ->
                []
        end.

md5terms(State, []) ->
    {State, []};
md5terms(State, [H|T]) ->
    B = term_to_binary(H),
    NewState = erlang:md5_update(State, B),
    {FinState, TL} = md5terms(NewState, T),
    {FinState, [B|TL]}.

parse_ft_options(Options) when is_list(Options) ->
    {Opt,Rest} = case (catch lists:keytake(extended_info,1,Options)) of
                     false ->
                         {[],Options};
                     {value,{extended_info,L},R} when is_list(L) ->
                         {L,R}
                 end,
    parse_ft_info_options(#filetab_options{}, Opt ++ Rest);
parse_ft_options(Malformed) ->
    throw({malformed_option, Malformed}).

parse_ft_info_options(FtOpt,[]) ->
    {ok,FtOpt};
parse_ft_info_options(FtOpt,[object_count | T]) ->
    parse_ft_info_options(FtOpt#filetab_options{object_count = true}, T);
parse_ft_info_options(FtOpt,[md5sum | T]) ->
    parse_ft_info_options(FtOpt#filetab_options{md5sum = true}, T);
parse_ft_info_options(FtOpt, [sync | T]) ->
    parse_ft_info_options(FtOpt#filetab_options{sync = true}, T);
parse_ft_info_options(_,[Unexpected | _]) ->
    throw({unknown_option,[{extended_info,[Unexpected]}]});
parse_ft_info_options(_,Malformed) ->
    throw({malformed_option,Malformed}).
