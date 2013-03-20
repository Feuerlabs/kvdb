%% -*- erlang -*-
%%---- BEGIN COPYRIGHT -------------------------------------------------------
%%
%% Copyright (C) 2012 Feuerlabs, Inc. All rights reserved.
%%
%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at http://mozilla.org/MPL/2.0/.
%%
%%---- END COPYRIGHT ---------------------------------------------------------
Definitions.

D = [0-9]
WS  = ([\000-\s]|%.*)

Rules.

{  : {token, {'{', TokenLine}}.
}  : {token, {'}', TokenLine}}.
/  : {token, {'/', TokenLine}}.
,  : {token, {',', TokenLine}}.
:  : {token, {':', TokenLine}}.
;  : {token, {';', TokenLine}}.
-  : {token, {'-', TokenLine}}.

{D}+ :
  {token, {integer, TokenLine, list_to_integer(TokenChars)}}.

{D}+\.{D}+ :
  {token, {fixnum, TokenLine, mk_fixnum(TokenChars)}}.

in : {token, {in, TokenLine}}.
at : {token, {at, TokenLine}}.
each : {token, {each, TokenLine}}.
repeat  : {token, {repeat, TokenLine}}.
once : {token, {once, TokenLine}}.
forever : {token, {forever, TokenLine}}.
until  : {token, {until, TokenLine}}.
times : {token, {times, TokenLine}}.
ms : {token, {ms, TokenLine}}.
sec|secs|seconds : {token, {seconds, TokenLine}}.
min|minutes : {token, {minutes, TokenLine}}.
hr|hrs|hours : {token, {hours, TokenLine}}.
mo|months : {token, {months, TokenLine}}.
y|yr|years : {token, {years, TokenLine}}.
daily  : {token, {daily, TokenLine}}.
monthly  : {token, {monthly, TokenLine}}.
weekly   : {token, {weekly, TokenLine}}.
annually  : {token, {yearly, TokenLine}}.
yearly  : {token, {yearly, TokenLine}}.
last_day_of_month : {token, {last_day_of_month, TokenLine}}.
ldom : {token, {last_day_of_month, TokenLine}}.

{WS}+       :   skip_token.

Erlang code.

%%% @hidden

mk_fixnum(Cs) ->
    [I,F] = re:split(Cs, <<"\\.">>, [{return, list}]),
    {list_to_integer(I), to_ms(F)}.

to_ms([A,B,C|_]) -> list_to_integer("1" ++ [A,B,C]) - 1000;
to_ms([_|_] = S) -> to_ms(S, 100).

to_ms("0" ++ S, M) -> to_ms(S, M div 10);
to_ms([], _) -> 0;
to_ms(S, M) -> list_to_integer(S) * M.
