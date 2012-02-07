-module(kvdb_lib).
-export([enc/3,
	 dec/3]).

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
