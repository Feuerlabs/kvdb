-module(kvdb_lib).
-export([table_name/1,
	 valid_table_name/1,
	 index_vals/4,
	 valid_indexes/1,
	 enc/3,
	 dec/3,
	 try_decode/1,
	 enc_prefix/3,
	 is_prefix/3,
	 check_valid_encoding/1,
	 actual_key/3,
	 actual_key/4,
	 actual_key/5,
	 split_queue_key/2, split_queue_key/3,
	 queue_prefix/2,
	 queue_prefix/3,
	 timestamp/0, timestamp/1,
	 timestamp_to_datetime/1,
	 good_string/1]).

valid_table_name(Table0) ->
    Table = table_name(Table0),
    case [C || <<C:8>> <= Table, lists:member(C, "-=?+.,:;^*/")] of
	[_|_] ->
	    error({illegal_table_name, Table0});
	[] ->
	    Table
    end.

table_name(Table) when is_atom(Table) ->
    atom_to_binary(Table, latin1);
table_name(Table) when is_binary(Table) ->
    Table;
table_name(Table) when is_list(Table) ->
    list_to_binary(Table).


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

enc(_, X, raw ) -> X;
enc(_, X, term) -> term_to_binary(X);
enc(_, X, sext) -> sext:encode(X);
enc(key  , X, {Enc,_}  ) -> enc(key, X, Enc);
enc(key  , X, {Enc,_,_}) -> enc(key, X, Enc);
enc(value, X, {_,Enc}  ) -> enc(value, X, Enc);
enc(value, X, {_,_,Enc}) -> enc(value, X, Enc);
enc(attrs, X, {_,Enc,_}) -> enc(attrs, X, Enc).

dec(_, X, raw ) -> X;
dec(_, X, term) -> binary_to_term(X);
dec(_, X, sext) -> sext:decode(X);
dec(key  , X, {Enc,_}  ) -> dec(key, X, Enc);
dec(key  , X, {Enc,_,_}) -> dec(key, X, Enc);
dec(value, X, {_,Enc}  ) -> dec(value, X, Enc);
dec(value, X, {_,_,Enc}) -> dec(value, X, Enc);
dec(attrs, X, {_,Enc,_}) -> dec(attrs, X, Enc).

try_decode(V) ->
    try binary_to_term(V)
    catch
	error:_ ->
	    try sext:decode(V)
	    catch
		error:_ ->
		    V
	    end
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

-define(VALID_ENC(E), (E==sext orelse E==raw orelse E==term)).
check_valid_encoding({E1,E2}) when ?VALID_ENC(E1) andalso ?VALID_ENC(E2) -> true;
check_valid_encoding({E1,E2,E3}) when ?VALID_ENC(E1)
				      andalso ?VALID_ENC(E2)
				      andalso ?VALID_ENC(E3) -> true;
check_valid_encoding(E) when ?VALID_ENC(E) -> true;
check_valid_encoding(E) -> error({illegal_encoding, E}).


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

actual_key(Enc, Q, Key) ->
    actual_key(Enc, fifo, Q, Key).

actual_key(Enc, T, Q, Key) when Enc==raw; element(1, Enc) == raw ->
    raw_queue_key(Q, T, Key);
actual_key(Enc, {keyed,_}, Q, Key) when Enc==sext; element(1, Enc) == sext ->
    {Q, Key, timestamp()};
actual_key(Enc, _, Q, Key) when Enc==sext; element(1, Enc) == sext ->
    {Q, timestamp(), Key}.

actual_key(Enc, T, Q, TS, Key) when Enc==raw; element(1, Enc) == raw ->
    raw_queue_key(Q, T, TS, Key);
actual_key(Enc, {keyed,_}, Q, TS, Key)
  when Enc==sext; element(1, Enc) == sext ->
    {Q, Key, TS};
actual_key(Enc, _, Q, TS, Key) when Enc==sext; element(1, Enc) == sext ->
    {Q, TS, Key}.

split_queue_key(Enc, Key) ->
    split_queue_key(Enc, fifo, Key).

split_queue_key(Enc, T, Key) when Enc == raw; element(1, Enc) == raw ->
    split_raw_queue_key(T, Key);
split_queue_key(Enc, T, Key) when Enc == sext; element(1, Enc) == sext ->
    case {T, Key} of
	{{keyed,_}, {Q, K, _TS}} ->
	    {Q, K};
	{_, {Q, _TS, K}} ->
	    {Q, K}
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

%% Encode a 56-bit prefix using our special-epoch timestamp.
%% It will not overflow until year 4293 - hopefully that will be sufficient.
raw_queue_key(Q, {keyed,_}, K) when is_binary(Q), is_binary(K) ->
    raw_keyed_queue_key(Q, timestamp(), K);
raw_queue_key(Q, _, K) when is_binary(Q), is_binary(K) ->
    raw_queue_key_(Q, timestamp(), K).

raw_queue_key(Q, {keyed,_}, TS, K) when is_binary(Q), is_binary(K) ->
    raw_keyed_queue_key(Q, TS, K);
raw_queue_key(Q, _, TS, K) ->
    raw_queue_key_(Q, TS, K).


raw_queue_key_(Q, TS, K) ->
    <<Q/binary, "-", TS:56/integer, K/binary>>.

raw_keyed_queue_key(Q, TS, K) ->
    <<Q/binary, "-", K/binary, TS:56/integer>>.

raw_queue_prefix(Q, first) ->
    <<Q/binary, ",">>;
raw_queue_prefix(Q, last) ->
    <<Q/binary, ".">>.

raw_queue_prefix(Q) ->
    <<Q/binary, "-">>.

split_raw_queue_key({keyed,_}, K) ->
    Sz = byte_size(K),
    {P,_} = binary:match(K, <<"-">>),
    KSz = Sz - P - 7 - 1,
    <<Q:P/binary, "-", Key:KSz/binary, _:56/integer>> = K,
    {Q, Key};
split_raw_queue_key(_, K) ->
    {P,_} = binary:match(K, <<"-">>),
    <<Q:P/binary, "-", _:56/integer, Key/binary>> = K,
    {Q, Key}.
%% split_raw_queue_key(<<_:56/integer, K/binary>>) ->
%%     K.

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
