-module(kvdb_lib).
-export([table_name/1,
	 valid_table_name/1,
	 enc/3,
	 dec/3,
	 enc_prefix/3,
	 is_prefix/3,
	 check_valid_encoding/1,
	 actual_key/3,
	 split_queue_key/2,
	 queue_prefix/2,
	 queue_prefix/3,
	 timestamp/0,
	 timestamp_to_datetime/1,
	 good_string/1]).

valid_table_name(Table0) ->
    Table = table_name(Table0),
    case [C || <<C:8>> <= Table, lists:member(C, "-.,:;*/")] of
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

enc_prefix(_, X, raw ) -> X;
enc_prefix(_, X, sext) ->
    case X of
	<<>> -> <<>>;
	_ when is_binary(X) ->
	    %% sext-encoding terminates with padding and an end byte. This won't work
	    %% for prefix matching, so we emulate the encoding, but without the ending.
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

actual_key(Enc, Q, Key) when Enc==raw; element(1, Enc) == raw ->
    raw_queue_key(Q, Key);
actual_key(Enc, Q, Key) when Enc==sext; element(1, Enc) == sext ->
    {Q, timestamp(), Key}.

split_queue_key(Enc, Key) when Enc == raw; element(1, Enc) == raw ->
    split_raw_queue_key(Key);
split_queue_key(Enc, {Q, _TS, Key}) when Enc == sext; element(1, Enc) == sext ->
    {Q, Key}.

timestamp() ->
    %% Invented epoc is {1258,0,0}, or 2009-11-12, 4:26:40
    {MS,S,US} = erlang:now(),
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
raw_queue_key(Q, K) when is_binary(Q), is_binary(K) ->
    TS = timestamp(),
    <<Q/binary, "-", TS:56/integer, K/binary>>.

raw_queue_prefix(Q, first) ->
    <<Q/binary, ",">>;
raw_queue_prefix(Q, last) ->
    <<Q/binary, ".">>.

raw_queue_prefix(Q) ->
    <<Q/binary, "-">>.

split_raw_queue_key(K) ->
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
