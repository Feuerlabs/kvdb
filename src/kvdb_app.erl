-module(kvdb_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    io:fwrite("starting kvdb~n", []),
    %% dbg:tracer(),
    %% dbg:tpl(kvdb,x),
    %% dbg:tpl(kvdb_sup,x),
    %% dbg:tp(kvdb_sqlite,x),
    %% dbg:tp(kvdb_leveldb,x),
    %% dbg:p(all,[c]),
    kvdb_sup:start_link().

stop(_State) ->
    ok.
