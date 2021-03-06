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
%%% @hidden
%%%
-module(kvdb_lib).
-export([table_name/1,
	 db_file/1,
	 valid_table_name/1,
	 index_vals/4,
	 valid_indexes/1,
	 enc/3,
	 dec/3,
	 try_decode/1,
	 enc_prefix/3,
	 is_prefix/3,
	 binary_match/2,
	 check_valid_encoding/1,
         matches_encoding/2,
	 actual_key/3,
	 actual_key/4,
	 actual_key/5,
	 split_queue_key/2, split_queue_key/3,
	 q_key_to_actual/3,
         q_head_key/2,
	 queue_prefix/2,
	 queue_prefix/3,
	 valid_queue/1,
	 queue_list_direction/1,
         queue_list_direction/2,
	 raw_queue_prefix/2,
	 timestamp/0, timestamp/1,
	 timestamp_to_datetime/1,
	 datetime_to_timestamp/1,
	 good_string/1]).
-export([common_open/3,
	 log_filename/1,
	 open_log/2,
	 replay_logs/3,
	 purge_logs/2,
	 clear_log_thresholds/1,
	 log/2,
	 process_log_event/3,
	 on_update/4,
	 commit/2,
	 make_tabrec/2,
	 make_tabrec/3,
	 tabrec_to_list/1]).

-export([escape/1, unescape/1]).

-export([backend_mod/1]).

%% Intended for replication
-export([nodes_of/2]).

-include("kvdb.hrl").
-include("log.hrl").

-define(Q_SEP_FLOOR, "$").
-define(Q_SEP, "%").
-define(Q_SEP_CEIL, "&").



valid_table_name(Table0) ->
    Table = table_name(Table0),
    case [C || <<C:8>> <= Table, lists:member(C, "-=?+.,:;$%&^*/")] of
	[_|_] ->
	    erlang:error({illegal_table_name, Table0});
	[] ->
	    Table
    end.

table_name(Table) when is_atom(Table) ->
    atom_to_binary(Table, latin1);
table_name(Table) when is_binary(Table) ->
    Table;
table_name(Table) when is_list(Table) ->
    list_to_binary(Table).

db_file(DbName) ->
    NameStr = kvdb_lib:good_string(DbName),
    File = NameStr ++ ".db",
    filelib:ensure_dir(File),
    File.


index_vals(Ixs, K, Attrs, ValF) ->
    %% Pre-fetch value, if needed, so we only fetch it once.
    %% If value doesn't exist, we must re-throw when asked for.
    Val = case needs_value(Ixs) of
	      true ->
		  try begin
			  V = ValF(),
			  fun() -> V end
		      end
		  catch
		      throw:_ -> fun() -> throw(no_value) end
		  end;
	      false ->
		  []
	  end,
    index_vals_(Ixs, K, Attrs, Val).

needs_value([{_, _, {value}}|_]) -> true;
needs_value([{_, _, {_, _}}|_]) -> true;
needs_value([_|T]) -> needs_value(T);
needs_value([]) ->  false.

index_vals_([H|T], K, Attrs, Val) when is_atom(H) ->
    ix_val_(H, H, K, Attrs, Val, T);
index_vals_([{H,value}      |T], K,As,V) -> ix_val_(H,H, K, As, V, T);
index_vals_([{IxN,value,A}  |T], K,As,V) -> ix_val_(IxN,A, K, As, V, T);
index_vals_([{IxN, words, A}|T], K,As,V) -> ix_words_(IxN, A, K,As,V, T);
index_vals_([{A, words}     |T], K,As,V) -> ix_words_(A  , A, K,As,V, T);
index_vals_([{IxN, each, A} |T], K,As,V) -> ix_each_(IxN, A, K,As,V, T);
index_vals_([{A, each}      |T], K,As,V) -> ix_each_(A  , A, K,As,V, T);
index_vals_([], _,_,_) ->
    [].

ix_val_(N,{value}, K, Attrs, Val, T) ->
    try Val() of
	V ->
	    [{N,V} | index_vals_(T, K, Attrs, Val)]
    catch
	throw:_ -> index_vals_(T, K, Attrs, Val)
    end;
ix_val_(N,{M,F}, K, Attrs, Val, T) ->
    try M:F(K, Attrs, Val()) of V ->
	    [{N,V} | index_vals_(T, K, Attrs, Val)]
    catch
	throw:_ -> index_vals_(T, K, Attrs, Val);
	error:_ -> index_vals_(T, K, Attrs, Val)
    end;
ix_val_(N,A, K, Attrs, Val, T) ->
    case lists:keyfind(A, 1, Attrs) of
	{_,V}  ->
	    [{N,V} | index_vals_(T, K, Attrs, Val)];
	false ->
	    index_vals_(T, K, Attrs, Val)
    end.

ix_each_(IxN, A, K, Attrs, Val, T) ->
    case get_ix_val_(A, K, Attrs, Val) of
	{_, L} when is_list(L) ->
	    %% We could check if the list contains duplicates, but strictly
	    %% speaking, it should resolve itself when we store the values.
	    [{IxN,X} || X <- L] ++ index_vals_(T, K, Attrs, Val);
	_ ->
	    index_vals_(T, K, Attrs, Val)
    end.

ix_words_(IxN, A, K, Attrs, Val, T) ->
    case get_ix_val_(A, K, Attrs, Val) of
	{_, S} when is_list(S); is_binary(S) ->
	    try lists:usort(
		  [{IxN,X} || X <- re:split(S, "[()\\.,\\-:;\\[\\]{}\\s]+")])
		     ++ index_vals_(T, K, Attrs, Val)
	    catch
		error:_ ->
		    index_vals_(T, K, Attrs, Val)
	    end;
	_ ->
	    index_vals_(T, K, Attrs, Val)
    end.

get_ix_val_({value}, _, _, V) ->
    if_val_(V, fun(Val) -> {value, Val} end);
get_ix_val_({M,F}, K, As, V) ->
    try {hook, M:F({K, As, V()})}
    catch
	throw:_ -> false;
	error:_ -> false
    end;
get_ix_val_(A, _, As, _) -> lists:keyfind(A, 1, As).

if_val_(V, F) ->
    try V() of Val ->
	    F(Val)
    catch
	throw:_ -> false
    end.


valid_indexes(Ix) ->
    case lists:foldr(
	   fun(A, Acc) when is_atom(A) -> Acc;
	      ({value}, Acc) -> Acc;
	      ({A, each}=X , Acc) -> if_valid_ix_ref(A, X, Acc);
	      ({A, value}, Acc) when is_atom(A) -> Acc;
	      ({{value}, value}, Acc) -> Acc;
	      ({A, words}=X, Acc) -> if_valid_ix_ref(A, X, Acc);
	      ({_N, value, A}=X, Acc)  -> if_valid_ix_ref(A, X, Acc);
	      ({_N, each, A}=X, Acc)  -> if_valid_ix_ref(A, X, Acc);
	      ({_N, words, A}=X, Acc) -> if_valid_ix_ref(A, X, Acc);
	      (Other, Acc) -> [Other|Acc]
	   end, [], Ix) of
	[]  -> ok;
	Bad -> {error, Bad}
    end.

if_valid_ix_ref({value} , _, Acc) -> Acc;
if_valid_ix_ref({M,F}   , _, Acc) when is_atom(M), is_atom(F) -> Acc;
if_valid_ix_ref(A       , _, Acc) when is_atom(A) -> Acc;
if_valid_ix_ref(_, X, Acc) -> [X|Acc].

enc(_, X, raw ) when is_binary(X) -> X;
enc(_, X, term) -> term_to_binary(X);
enc(_, X, {term,Opts}) -> term_to_binary(X, Opts);
enc(_, X, sext) -> sext:encode(X);
enc(key  , X, {Enc,_}  ) -> enc(key, X, Enc);
enc(key  , X, {Enc,_,_}) -> enc(key, X, Enc);
enc(value, X, {_,Enc}  ) -> enc(value, X, Enc);
enc(value, X, {_,_,Enc}) -> enc(value, X, Enc);
enc(attrs, X, {_,Enc,_}) -> enc(attrs, X, Enc);
enc(W, X, E) ->
    erlang:error({cannot_encode, [W,X,E]}).

dec(_, X, raw ) when is_binary(X) -> X;
dec(_, X, term) -> binary_to_term(X);
dec(_, X, {term,_}) -> binary_to_term(X);
dec(_, X, sext) -> try_sext_decode(X);
dec(key  , X, {Enc,_}  ) -> dec(key, X, Enc);
dec(key  , X, {Enc,_,_}) -> dec(key, X, Enc);
dec(value, X, {_,Enc}  ) -> dec(value, X, Enc);
dec(value, X, {_,_,Enc}) -> dec(value, X, Enc);
dec(attrs, X, {_,Enc,_}) -> dec(attrs, X, Enc);
dec(W,X,E) ->
    erlang:error({cannot_decode, [W,X,E]}).

try_decode(V) ->
    try binary_to_term(V)
    catch
	error:_ ->
            try_sext_decode(V)
    end.

try_sext_decode(V) ->
    try sext:partial_decode(V) of
        {full,Res,_} ->
            Res;
        {partial,{Q,'_','_'},<<0>>} ->
            #q_key{queue = Q, ts = ?Q_HEAD_FLOOR, key = ?Q_HEAD_KEY};
        {partial,{Q,'_','_'},<<127>>} ->
            #q_key{queue = Q, ts = ?Q_HEAD_CEIL, key = ?Q_HEAD_KEY}
    catch
        error:_ ->
            V
    end.


enc_prefix(_, X, raw ) -> X;
enc_prefix(_, X, sext) ->
    case X of
	<<>> -> <<>>;
	_ when is_binary(X) ->
	    %% sext-encoding terminates with padding and an end byte.
	    %% This won't work for prefix matching, so we emulate the
	    %% encoding, but without the ending.
	    Enc = << 18, (<< <<1:1, B1:8>> || <<B1>> <= X >>)/bitstring >>,
	    Sz = bit_size(Enc) div 8,
	    <<P:Sz/binary, _/bitstring>> = Enc,
	    P;
	_ ->
	    sext:prefix(X)
    end;
enc_prefix(key  , X, {Enc,_}  ) -> enc_prefix(key, X, Enc);
enc_prefix(key  , X, {Enc,_,_}) -> enc_prefix(key, X, Enc);
enc_prefix(value, X, {_,Enc}  ) -> enc_prefix(value, X, Enc);
enc_prefix(value, X, {_,_,Enc}) -> enc_prefix(value, X, Enc);
enc_prefix(attrs, X, {_,Enc,_}) -> enc_prefix(attrs, X, Enc).

%% The key encoding must be sortable (i.e. sext or raw)
-define(VALID_KEY_ENC(E), (E==sext orelse E==raw)).
%% Other elements can use any supported encoding
-define(VALID_ENC(E), (E==sext orelse
                       E==raw orelse
                       E==term orelse
                       (tuple_size(E) == 2
                        andalso element(1,E)==term
                        andalso is_list(element(2,E))))).
check_valid_encoding({E1,E2})
  when ?VALID_KEY_ENC(E1) andalso ?VALID_ENC(E2) -> true;
check_valid_encoding({E1,E2,E3}) when ?VALID_KEY_ENC(E1)
				      andalso ?VALID_ENC(E2)
				      andalso ?VALID_ENC(E3) -> true;
check_valid_encoding(E) when ?VALID_KEY_ENC(E) -> true;
check_valid_encoding(E) -> erlang:error({illegal_encoding, E}).


%% FIXME! This function should also check that we are type-compatible with
%% the requested encoding, even if we don't actually encode.
matches_encoding(Enc, Obj) ->
    case {Enc, Obj} of
	{{_,_,_}, {_,_,_}} ->
	    true;
	{_, {_,_}} ->
	    true;
	_ ->
	    false
    end.

queue_prefix(Enc, Q) when Enc == raw; element(1, Enc) == raw ->
    raw_queue_prefix(Q);
queue_prefix(Enc, Q) when Enc == sext; element(1, Enc) == sext ->
    {Q, '_', '_'}.

queue_prefix(Enc, Q, End) when Enc == raw; element(1, Enc) == raw ->
    raw_queue_prefix(Q, End);
queue_prefix(Enc, Q, End) when Enc == sext; element(1, Enc) == sext ->
    %% The timestamp is a positive integer
    case End of
	first ->
	    {Q, -1, '_'};
	last ->
	    {Q, a, '_'}
    end.

queue_list_direction(Type) ->
    queue_list_direction(Type, false).

queue_list_direction(Type, Reverse) ->
    Dir = if Type == lifo; element(2, Type) == lifo -> lifo;
	     Type == fifo; element(2, Type) == fifo -> fifo;
	     element(1,Type) == fifo -> fifo;
	     element(1,Type) == lifo -> lifo
	  end,
    if Reverse ->
	    case Dir of
		fifo -> lifo;
		lifo -> fifo
	    end;
       true -> Dir
    end.

queue_order({keyed,_} = T) -> T;
queue_order({keyed,D,_}) -> {keyed,D};
queue_order({fifo,_}) -> fifo;
queue_order({lifo,_}) -> lifo;
queue_order(fifo) -> fifo;
queue_order(lifo) -> lifo.

actual_key(Enc, Q, Key) ->
    actual_key(Enc, fifo, Q, Key).

actual_key(Enc, T, Q, Key) when Enc==raw; element(1, Enc) == raw ->
    TS = timestamp(),
    {raw_queue_key(Q, T, TS, Key), #q_key{queue = Q, ts = TS, key = Key}};
actual_key(Enc, {keyed,_}, Q, Key) when Enc==sext; element(1, Enc) == sext ->
    TS = timestamp(),
    {{Q, Key, TS}, #q_key{queue = Q, ts = TS, key = Key}};
actual_key(Enc, _, Q, Key) when Enc==sext; element(1, Enc) == sext ->
    TS = timestamp(),
    {{Q, TS, Key}, #q_key{queue = Q, ts = TS, key = Key}}.

actual_key(Enc, T, Q, TS, Key) when Enc==raw; element(1, Enc) == raw ->
    {raw_queue_key(Q, T, TS, Key), #q_key{queue = Q, ts = TS, key = Key}};
actual_key(Enc, {keyed,_}, Q, TS, Key)
  when Enc==sext; element(1, Enc) == sext ->
    {{Q, Key, TS}, #q_key{queue = Q, ts = TS, key = Key}};
actual_key(Enc, _, Q, TS, Key) when Enc==sext; element(1, Enc) == sext ->
    {{Q, TS, Key}, #q_key{queue = Q, ts = TS, key = Key}}.

split_queue_key(_, #q_key{} = QK) ->
    QK;
split_queue_key(Enc, Key) ->
    split_queue_key(Enc, fifo, Key).

split_queue_key(_, _, #q_key{} = QK) ->
    QK;
split_queue_key(Enc, T, Key) when Enc == raw; element(1, Enc) == raw ->
    split_raw_queue_key(T, Key);
split_queue_key(Enc, T, Key) when Enc == sext; element(1, Enc) == sext ->
    case {T, Key} of
	{{keyed,_}, {Q, K, TS}} ->
	    #q_key{queue = Q, ts = TS, key = K};
	{_, {Q, TS, K}} ->
	    #q_key{queue = Q, ts = TS, key = K}
    end.

q_key_to_actual(#q_key{queue = Q, key = ?Q_HEAD_KEY}, Enc, Type) ->
    if Enc == raw; element(1, Enc) == raw ->
            raw_queue_head_key(Q, Type);
       Enc == sext; element(1, Enc) == sext ->
            sext_queue_head_key(Q, Type)
    end;
q_key_to_actual(#q_key{queue = Q, ts = TS, key = K}, Enc, Type) when
      Enc==raw; element(1,Enc) == raw ->
    raw_queue_key(Q, Type, TS, K);
q_key_to_actual(#q_key{queue = Q, ts = TS, key = K}, Enc, Type) when
      Enc==sext; element(1,Enc) == sext ->
    case Type of
	{keyed,_} ->
	    sext:encode({Q, K, TS});
	_ ->
	    sext:encode({Q, TS, K})
    end.


timestamp() ->
    timestamp(erlang:now()).

timestamp({MS,S,US}) ->
    %% Invented epoc is {1258,0,0}, or 2009-11-12, 4:26:40
    (MS-1258)*1000000000000 + S*1000000 + US.

timestamp_to_datetime(TS) ->
    %% Our internal timestamps are relative to Now = {1258,0,0}
    %% It doesn't really matter much how we construct a now()-like tuple,
    %% as long as the weighted sum of the three numbers is correct.
    S = TS div 1000000,
    US = TS rem 1000000,
    %% return {Datetime, Milliseconds}
    {calendar:now_to_datetime({1258,S,0}), US}.


%% calendar:datetime_to_gregorian_seconds({{2009,11,12}, {4,26,40}})
-define(EPOCH_GREGORIAN_SECS, 63425219200).

datetime_to_timestamp({{{_Y,_Mo,_D},{_H,_Mi,_S}} = DT, US}) ->
    ((calendar:datetime_to_gregorian_seconds(DT)
      - ?EPOCH_GREGORIAN_SECS) * 1000000) + US.

%% Encode a 56-bit prefix using our special-epoch timestamp.
%% It will not overflow until year 4293 - hopefully that will be sufficient.

%% The queue head must be either before or after all possible queue entries.
%% Queue entries are either (<Q> "-" <TS> <Key>) or (<Q> "-" <Key> <TS>),
%% so for the head we use (<Q> <Marker>) where Marker is less than (",")
%% or greater than (".") "-".
%% CORRECTION: Since '.' and '-' are not escapable, we can't use them.
%% Use instead '$', '%', and '&' as separators.
raw_queue_head_key(Q, Type) ->
    case queue_list_direction(Type) of
	fifo ->
	    <<(escape(Q))/binary, ?Q_SEP_FLOOR>>;
	lifo ->
	    <<(escape(Q))/binary, ?Q_SEP_CEIL>>
    end.

%% For the sext-encoded queue head, we make use of the fact that we can decode
%% a sext-encoded term followed by a non-sext sequence. Thus, we prefix-encode
%% {Q,'_','_'} and append a value that's either lower than (0) or greater than
%% (127) any sext type tag.
sext_queue_head_key(Q, Type) ->
    Pfx = sext:prefix({Q,'_','_'}),
    case queue_list_direction(Type) of
	fifo ->
	    <<Pfx/binary, 0>>;
	lifo ->
	    <<Pfx/binary, 127>>
    end.

q_head_key(Q, Type) ->
    case queue_list_direction(Type) of
	fifo ->
	    #q_key{queue = Q, ts = ?Q_HEAD_FLOOR, key = ?Q_HEAD_KEY};
	lifo ->
	    #q_key{queue = Q, ts = ?Q_HEAD_CEIL,  key = ?Q_HEAD_KEY}
    end.

raw_queue_key(Q, T, TS, K) ->
    case {queue_order(T), TS, K} of
	{fifo, ?Q_HEAD_FLOOR, ?Q_HEAD_KEY} ->
	    raw_queue_head_key(Q, fifo);
	{lifo, ?Q_HEAD_CEIL, ?Q_HEAD_KEY} ->
	    raw_queue_head_key(Q, lifo);
	{{keyed,_}, _, _} ->
	    raw_queue_keyed_key_(Q, TS, K);
	_ ->
	    raw_queue_key_(Q, TS, K)
    end.

raw_queue_key_(Q, TS, K) ->
    <<(escape(Q))/binary, ?Q_SEP, TS:56/integer, K/binary>>.

raw_queue_keyed_key_(Q, TS, K) ->
    <<(escape(Q))/binary, ?Q_SEP, (escape(K))/binary, TS:56/integer>>.

raw_queue_prefix(Q, first) ->
    <<(escape(Q))/binary, ?Q_SEP_FLOOR>>;
raw_queue_prefix(Q, last) ->
    <<(escape(Q))/binary, ?Q_SEP_CEIL>>.

raw_queue_prefix(Q) ->
    <<(escape(Q))/binary, ?Q_SEP>>.

split_raw_queue_key({keyed,_}, K) ->
    Sz = byte_size(K),
    case binary:match(K, <<?Q_SEP>>) of
        {P,_} ->
            KSz = Sz - P - 7 - 1,
            <<Q:P/binary, ?Q_SEP, Key:KSz/binary, TS:56/integer>> = K,
            #q_key{queue = unescape(Q), ts = TS, key = unescape(Key)};
        nomatch ->
            split_raw_queue_head_key(K, Sz)
    end;
split_raw_queue_key(_, K) ->
    case binary:match(K, <<?Q_SEP>>) of
        {P, _} ->
            <<Q:P/binary, ?Q_SEP, TS:56/integer, Key/binary>> = K,
            #q_key{queue = unescape(Q), ts = TS, key = Key};
        nomatch ->
            split_raw_queue_head_key(K, byte_size(K))
    end.

split_raw_queue_head_key(K, Sz) ->
    Sz1 = Sz-1,
    case K of
        <<Q:Sz1/binary, ?Q_SEP_FLOOR>> ->
            #q_key{queue = unescape(Q), ts = ?Q_HEAD_FLOOR,
                   key = ?Q_HEAD_KEY};
        <<Q:Sz1/binary, ?Q_SEP_CEIL>> ->
            #q_key{queue = unescape(Q), ts = ?Q_HEAD_CEIL,
                   key = ?Q_HEAD_KEY}
    end.

is_prefix(Pfx, K, Enc) when is_binary(Pfx) ->
    case dec(key, K, Enc) of
	Bk when is_binary(Bk) ->
	    binary_match(Bk, Pfx);
	_ ->
	    false
    end;
is_prefix(Pfx, K, Enc) ->
    MS = ets:match_spec_compile([{Pfx, [], ['$_']}]),
    case ets:match_spec_run([dec(key,K,Enc)], MS) of
	[_] ->
	    true;
	[] ->
	    false
    end.

binary_match(A, <<>>) when is_binary(A) ->
    %% binary:match/2 doesn't handle this case
    true;
binary_match(A, B) ->
    case binary:match(A, B) of
	{0, _} -> true;
	_ -> false
    end.

%% @spec(Name :: any()) -> string()
%%
%% @doc Ensures that a database name doesn't contain weird characters
%%
%% Kvdb database names need to be mapped to filesystem names. This function
%% produces a formatting of the name that is filesystem-friendly.
%% @end
good_string(Name) when is_atom(Name) ->
    atom_to_list(Name);
good_string(Bin) when is_binary(Bin) ->
    binary_to_list(Bin);
good_string(T) when is_tuple(T), size(T) > 0 ->
    L = tuple_to_list(T),
    lists:flatten([str(hd(L)) | [["_", str(X)] || X <- tl(L)]]);
good_string([I|_] = L) when is_integer(I) ->
    case lists:all(fun(X) when $0 =< X, X =< $9 -> true;
		      (X) when $a =< X, X =< $z -> true;
		      (X) when $A =< X, X =< $Z -> true;
		      (X) -> lists:member(X, ".#%=+!()-_åäöÅÄÖ")
		   end, L) of
	true ->
	    lists:flatten(L);
	false ->
	    lists:flatten([str(hd(L)) | [["-", str(X)] || X <- tl(L)]])
    end;
good_string([_|_] = L) ->
    lists:flatten([str(hd(L)) | [["-", str(X)] || X <- tl(L)]]);
good_string(Other) ->
    str(Other).

str(I) when is_integer(I) ->
    integer_to_list(I);
str(X) ->
    binary_to_list(bin(X)).

bin(A) when is_atom(A) ->
    atom_to_binary(A, latin1);
bin(L) when is_list(L) ->
    try iolist_to_binary(L)
    catch
	error:_ ->
	    iolist_to_binary(io_lib:fwrite("~w", [L]))
    end;
bin(B) when is_binary(B) ->
    B;
bin(T) ->
    iolist_to_binary(io_lib:fwrite("~w", [T])).


common_open(Module, #db{} = Db, Options) ->
    case proplists:get_value(log_dir, Options, undefined) of
	undefined ->
	    {ok, Db#db{log = false}};
	D ->
	    replay_logs(D, Module, Db),
	    kvdb_meta:write(Db, log_dir, D),
	    F = log_filename(D),
	    do_open_log(F, Db, log_threshold(Options))
    end.

replay_logs(Dir, Module, #db{} = Db) ->
    LastDump = kvdb_meta:read(Db, last_dump, undefined),
    FileKey = if LastDump == undefined -> "";
		 true -> filename:basename(log_filename(Dir, LastDump, []))
	      end,
    case file:list_dir(Dir) of
	{ok, [_|_] = Fs} ->
	    UseLogs = use_logs(FileKey, lists:sort(Fs)),
	    UseFiles = [filename:join(Dir, F) || F <- UseLogs],
	    lager:debug("Logs = ~p~n"
			"FileKey = ~p~n"
			"UseLogs = ~p~n", [Fs, FileKey, UseFiles]),
	    Res = try
		      lists:foreach(fun(F) ->
					    ok = replay_log(F, Module, Db, LastDump)
				    end, UseFiles)
		  catch
		      { error_opening_log, [_LogF, _Error]} -> ok
		  end,
	    if Res == ok ->
		    case Module:save(Db) of
			ok ->
			    lists:foreach(
			      fun(F) ->
				      _DeleteRes = file:delete(F),
				      ?debug("delete(~s) -> ~p~n",
					     [F, _DeleteRes])
			      end, UseFiles);
			Error ->
			    erlang:error({save_error, Error})
		    end;
	       true ->
		    erlang:error({replay_error, Res})
	    end;
	Other ->
	    lager:debug("No logs? ~p~n"
			"FileKey = ~p~n", [Other, FileKey])
    end.

replay_log(LogF, Module, Db, LastDump) ->
    ?debug("replay_log(~p, ~p, Db, ~p)~n", [LogF, Module, LastDump]),
    case open_log(LogF, self()) of
	{ok, Info} ->
	    {_, Log} = lists:keyfind(id, 1, Info),
	    try eat_log(disk_log:chunk(Log, start), Log, Module, Db, LastDump)
	    after
		disk_log:close(Log)
	    end;
	Error ->
	    throw({error_opening_log, [LogF, Error]})
    end.

eat_log(eof, _, _, _, _) ->
    ok;
eat_log({Cont, Terms}, Log, Mod, Db, Last) ->
    lists:foreach(fun({{_,_,_} = TS, _} = Event) when TS > Last ->
			  process_log_event(Event, Mod, Db);
		     (_) -> skip
		  end, Terms),
    eat_log(disk_log:chunk(Log, Cont), Log, Mod, Db, Last).

process_log_event({_TS, Op}, Mod, Db) ->
    ?debug("process_log_event(Op = ~p)~n", [Op]),
    case Op of
	?KVDB_LOG_INSERT(?META_TABLE, {Cat, K, V}) ->
	    Mod:schema_write(Db, Cat, K, V);
	?KVDB_LOG_INSERT(Table, Obj) ->
	    Mod:put(Db, Table, Obj);
	?KVDB_LOG_DELETE(?META_TABLE, {Cat, K}) ->
	    Mod:schema_delete(Db, Cat, K);
	?KVDB_LOG_DELETE(Table, Key) ->
	    Mod:delete(Db, Table, Key);
	?KVDB_LOG_Q_INSERT(Table, QKey, St, Obj) ->
	    Mod:queue_insert(Db, Table, QKey, St, Obj);
	?KVDB_LOG_Q_DELETE(Table, QKey) ->
	    _ = Mod:extract(Db, Table, QKey);
	?KVDB_LOG_ADD_TABLE(Table, TabR) ->
	    Mod:add_table(Db, Table, TabR);
	?KVDB_LOG_DELETE_TABLE(Table) ->
	    Mod:delete_table(Db, Table);
	?KVDB_LOG_COMMIT(CommitRec) ->
	    commit(CommitRec, Mod, Db)
    end.


use_logs(F, [A,B|_] = L) when A < F, F < B ->
    L;
use_logs(F, [A,B|T]) when F > A, F > B ->
    use_logs(F, [B|T]);
use_logs(_, L) ->
    L.

purge_logs(#db{} = Db, TS) when TS =/= undefined ->
    case kvdb_meta:read(Db, log_dir, undefined) of
	undefined ->
	    ok;
	Dir ->
	    RefF = filename:basename(log_filename(Dir, TS, [])),
	    case file:list_dir(Dir) of
		{ok, [_|_] = Fs} ->
		    Remove = Fs -- use_logs(RefF, Fs),
		    lists:foreach(fun(F) ->
					  file:delete(filename:join(Dir, F))
				  end, Remove);
		_ ->
		    ok
	    end
    end.

log_filename(D) ->
    case file:list_dir(D) of
	{ok, Fs} ->
	    log_filename(D, Fs);
	{error,_} ->
	    case filelib:ensure_dir(F = log_filename(D, [])) of
		ok ->
		    F;
		{error, E} ->
		    erlang:error({log_filename, [D, E]})
	    end
    end.

log_threshold(Options) ->
    case proplists:get_value(log_threshold, Options) of
	undefined -> #thr{writes = 10};
	TOpts ->
	    T0 = #thr{},
	    Writes = proplists:get_value(writes, TOpts, T0#thr.writes),
	    Bytes = proplists:get_value(bytes, TOpts, T0#thr.bytes),
	    %% Time = proplists:get_value(writes, TOpts, T0#thr.time),
	    T0#thr{writes = Writes,
		   bytes = Bytes
		  }
    end.

do_open_log(F, Db, Thr) ->
    case open_log(F, self()) of
	{ok, LogInfo} ->
	    {_, Log} = lists:keyfind(id, 1, LogInfo),
	    kvdb_meta:write(Db, log_info, LogInfo),
	    {ok, Db#db{log = {Log, Thr}}};
	{error,_} = E ->
	    {error, {open_log_error, [F, E]}}
    end.

open_log(F, Pid) ->
    Name = {kvdb_log, list_to_binary(F)},
    case disk_log:open([{name, Name},
			{file, F},
			{linkto, Pid},
			{size, infinity},
			{type, halt}]) of
	{ok, Log} ->
	    {ok, [{id,Log},
		  {name, Name},
		  {file, F}]};
	{repaired, Log, Rec, Bad} ->
	    ?warning("Log file ~p repaired: ~p; ~p.~n",
		     [F, Rec, Bad]),
	    {ok, [{id, Log},
		  {name, Name},
		  {file, F}]};
	{error, _} = Err ->
	    Err
    end.



log_filename(Dir, Fs) ->
    log_filename(Dir, os:timestamp(), Fs).

log_filename(Dir, TS, Fs) ->
    {_, _, US} = TS,
    {{Y,Mo,D},{H,Mi,S}} = calendar:now_to_datetime(TS),
    F = lists:flatten(
	  io_lib:format("~s.~2..0w~2..0w~2..0w-~2..0w~2..0w~2..0w~w",
			[filename:join(Dir,"kvdb_log"),
			 Y rem 100,Mo,D,H,Mi,S,US div 1000])),
    case lists:member(F, Fs) of
	true ->
	    %% how likely is that?
	    timer:sleep(100),
	    log_filename(Dir, Fs);
	false ->
	    F
    end.

commit(#commit{} = Commit, #kvdb_ref{mod = M,db=Db0}) ->
    commit(Commit, M, Db0).

commit(#commit{_ = []}, _, _) ->
    ok;
commit(#commit{write = Writes,
	       delete = Deletes,
	       add_tables = AddTabs,
	       del_tables = DelTabs} = Commit, M, #db{} = Db0) ->
    Db = Db0#db{log = false},
    Ops = [{fun(T) ->
		    M:delete_table(Db, T)
	    end, DelTabs},
	   {fun({T,TRec,DelFirst}) ->
		    if DelFirst ->
			    M:delete_table(Db, T);
		       true -> ok
		    end,
		    M:add_table(Db, T, TRec)
	    end, AddTabs},
	   {fun({T, #q_key{} = QK, St, Obj}) ->
		    M:queue_insert(Db, T, QK, St, Obj);
		({T, Obj}) ->
		    _Res = M:put(Db, T, Obj)
	    end, Writes},
	   {fun({T, K}) ->
		    M:delete(Db, T, K)
	    end, Deletes}],
    [lists:foreach(F, L) || {F, L} <- Ops],
    log(Db0, ?KVDB_LOG_COMMIT(Commit)).         % note: original log mode,
						% since Db0 used.

on_update(Event, #kvdb_ref{} = DbRef, Table, Info) ->
    %% log(DbRef, {Event, Table, Info}),
    kvdb_trans:on_update(Event, DbRef, Table, Info).


log(#db{log = false}, _) ->
    ok;
log(#db{log = {Log,Thr}} = Db, Data) ->
    Term = {os:timestamp(), Data},
    Bytes = erts_debug:flat_size(Term),
    disk_log:log(Log, Term),
    disk_log:sync(Log),
    SumBytes = kvdb_meta:update_counter(Db, logged_bytes, Bytes),
    SumWrites = kvdb_meta:update_counter(Db, logged_writes, 1),
    case threshold_reached(#thr{bytes = SumBytes, writes = SumWrites}, Thr) of
	true ->
	    case kvdb_meta:write_new(Db, log_threshold, true) of
		true ->
		    Name = kvdb_meta:read(Db, name, undefined),
		    kvdb_server:cast(Name, log_threshold);
		false ->
		    ok
	    end;
	false ->
	    kvdb_meta:delete(Db, log_threshold),
	    ok
    end.

nodes_of({commit, #commit{add_tables = [], del_tables = []} = Rec}, Db) ->
    lists:usort(lists:append(([nodes_of_(T, Db) || T <- updated_tables(Rec)])));
nodes_of({commit, _Commit}, Db) ->
    kvdb_meta:read(Db, schema_nodes, [node()]);
nodes_of(Evt, Db) ->
    nodes_of_(element(2, Evt), Db).

nodes_of_(T, Db) ->
    kvdb_meta:read(Db, {T, nodes}, [node()]).

updated_tables(#commit{write = Writes, delete = Deletes}) ->
    Tw = [element(1,W) || W <- Writes],
    Td = [element(1,D) || D <- Deletes],
    lists:usort(Tw ++ Td).


threshold_reached(#thr{bytes = ABs, writes = AWs},
		  #thr{bytes = TBs, writes = TWs}) ->
    (TBs =/= undefined andalso ABs > TBs)
	orelse
	(TWs =/= undefined andalso AWs > TWs).

clear_log_thresholds(Db) ->
    kvdb_meta:write(Db, logged_bytes, 0),
    kvdb_meta:write(Db, logged_writes, 0),
    kvdb_meta:delete(Db, log_threshold).


backend_mod(mnesia ) -> kvdb_mnesia;
backend_mod(leveldb) -> kvdb_leveldb;
backend_mod(sqlite3) -> kvdb_sqlite3;
backend_mod(sqlite ) -> kvdb_sqlite3;
backend_mod(ets    ) -> kvdb_ets;
backend_mod(riak   ) -> kvdb_riak;
backend_mod(M) ->
    case is_behaviour(M) of
        true ->
            M   % TODO: implement actual check
        %% false ->
        %%     error(illegal_backend_type)
    end.

is_behaviour(_M) ->
    %% TODO: check that exported functions match those listed in
    %% behaviour_info(callbacks).
    true.

make_tabrec(Tab, Opts) ->
    make_tabrec(Tab, Opts, #table{}).

make_tabrec(Tab, Opts, Rec) ->
    check_options(Opts, record_info(fields, table), Rec#table{name = Tab}).

tabrec_to_list(#table{} = TR) ->
    lists:zip(record_info(fields, table), tl(tuple_to_list(TR))).

check_options([{type, T}|Tl], Flds, Rec) ->
    case valid_type(T) of
	true ->
	    check_options(Tl, Flds, Rec#table{type = T});
	false ->
	    error({invalid_option, type})
    end;
check_options([{encoding, E}|Tl], Flds, Rec) ->
    Rec1 = Rec#table{encoding = E},
    kvdb_lib:check_valid_encoding(E),
    check_options(Tl, Flds, Rec1);
check_options([{index, Ix}|Tl], Flds, Rec) ->
    case kvdb_lib:valid_indexes(Ix) of
	ok -> check_options(Tl, Flds, Rec#table{index = Ix});
	{error, Bad} ->
	    error({invalid_index, Bad})
    end;
check_options([{K,V}|T], Flds, Rec) ->
    case key_pos(K, Flds) of
	0 ->
	    check_options(T, Flds, Rec);
	P -> check_options(T, Flds, setelement(P, Rec, V))
    end;
check_options([], _, Rec) ->
    Rec.

valid_type(set) -> true;
valid_type(T) ->
    valid_queue(T).

valid_queue(fifo) -> true;
valid_queue(lifo) -> true;
valid_queue({keyed,fifo}) -> true;
valid_queue({keyed,lifo}) -> true;
valid_queue({keyed,T,L}) when T==fifo; T==lifo ->
    is_integer(L) andalso L >= 0;
valid_queue({T,L}) when T==fifo; T==lifo ->
    is_integer(L) andalso L >= 0;
valid_queue(_) -> false.


key_pos(K, L) ->
    key_pos(K, L, 2).

key_pos(K, [K|_], P) -> P;
key_pos(K, [_|T], P) -> key_pos(K, T, P+1);
key_pos(_, [], _)    -> 0.

%% Key escaping

%% Macros for kvdb key escaping
%%
%% fixme: bitmap version is actually not that fast as I tought
%% break even is plenty of tests.
%%
unescape(<<>>) ->
    <<>>;
unescape(<< $=, Enc/binary >>) ->
    unescape_(Enc).

unescape_(<<$@, A, B, Rest/binary>>) ->
    <<(erlang:list_to_integer([A,B], 16)):8/integer,
      (unescape_(Rest))/binary>>;
unescape_(<<C, Rest/binary>>) ->
    <<C, (unescape_(Rest))/binary>>;
unescape_(<<>>) ->
    <<>>.

escape(<<>>) -> <<>>;
escape(Bin) when is_binary(Bin) ->
    Enc = << <<(id_char(C))/binary>> || <<C>> <= Bin >>,
    << $=, Enc/binary >>.

id_char(C) when C==?Q_HEAD_FLOOR; C==?Q_HEAD_CEIL; C==?Q_SEP; C==$@ ->
    <<$@, (to_hex(C bsr 4)):8/integer, (to_hex(C)):8/integer >>;
id_char(C) ->
    <<C>>.

to_hex(C) ->
    element((C band 16#f)+1, {$0,$1,$2,$3,$4,$5,$6,$7,$8,$9,
			      $A,$B,$C,$D,$E,$F}).
