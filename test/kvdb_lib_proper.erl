-module(kvdb_lib_proper).

-ifdef(PROPER).

-export([q_raw_keyed/0,
	 q_raw_keyed_sorts/0,
	 escape_unescape/0,
	 escape_sorts/0]).

-include_lib("proper/include/proper.hrl").


-type raw_key() :: binary().

q_raw_keyed() ->
    T = {keyed,fifo},
    ?FORALL(Key, raw_key(),
	    begin
		{Raw,QK} =
		    kvdb_lib:actual_key(raw, T, <<"q">>, Key),
		QK == kvdb_lib:split_queue_key(raw, T, Raw)
	    end).

q_raw_keyed_sorts() ->
    T = {keyed,fifo},
    ?FORALL({K1,K2}, {raw_key(), raw_key()},
	    begin
		{Raw1,_} = kvdb_lib:actual_key(raw, T, <<"q">>, K1),
		{Raw2,_} = kvdb_lib:actual_key(raw, T, <<"q">>, K2),
		(Raw1 > Raw2) == (K1 > K2)
	    end).

escape_sorts() ->
    ?FORALL({A,B}, {raw_key(), raw_key()},
	    begin
		X = kvdb_lib:escape(A),
		Y = kvdb_lib:escape(B),
		(A < B) == (X < Y)
	    end).

escape_unescape() ->
    ?FORALL(K, raw_key(),
	    K == kvdb_lib:unescape(kvdb_lib:escape(K))).

-endif. % PROPER
