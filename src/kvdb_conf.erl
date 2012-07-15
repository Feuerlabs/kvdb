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
%%% @doc
%%% API to store NETCONF-style config data in kvdb
%%%
%%% identifiers are stored as a structured key (binary)
%%% delimited by:
%%%
%%% Example:
%%% `{system,[{services, [{ssh,[...]}]}]}' would be stored as
%%%
%%% {"system", []}
%%% {"system*services", []}
%%% {"system*services*ssh", []}
%%%
%%% Netconf identifiers can consist of alphanumerics, '-', '_' or '.'.
%%% The '*' as delimiter is chosen so that a "wildcard" character can be
%%% used that is greater than the delimiter, but smaller than any identifier
%%% character.
%%% @end

-module(kvdb_conf).

-export([read/1,            %% (Key) -> read(data, Key)
	 read/2,            %% (Tab, Key)
	 write/1,           %% (Obj) -> write(data, Obj)
	 write/2,           %% (Tab, Obj)
	 update_counter/2,  %% (Key, Incr) -> update_counter(data, Key, Incr)
	 update_counter/3,  %% (Tab, Key, Incr)
	 delete/1,          %% (Key) -> delete(data, Key)
	 delete/2,          %% (Tab, Key)
	 delete_all/1,      %% (Prefix) -> delete_all(data, Prefix)
	 delete_all/2,      %% (Tab, Prefix)
	 read_tree/1,       %% (Key) -> (data, Key)
	 read_tree/2,       %% (Tab, Key)
	 write_tree/2,      %% (Parent, Tree) -> (data, Parent, Tree)
	 write_tree/3,      %% (Tab, Parent, Tree)
	 store_tree/1,      %% (Tree) -> (data, Tree)
	 store_tree/2,      %% (Tab, Tree)
	 all/0,             %% () -> all(data)
	 all/1,             %% (Tab)
	 first/0,           %% () -> first(data)
	 first/1,           %% (Tab)
	 last/0,            %% () -> last(data)
	 last/1,            %% (Tab)
	 next/1,            %% (Key) -> next(data, Key)
	 next/2,            %% (Tab, Key)
	 prev/1,            %% (Key) -> next(data, Key)
	 prev/2,            %% (Tab, Key)
	 next_at_level/1,   %% (Key) -> next_at_level(data, Key)
	 next_at_level/2,   %% (Tab, Key)
	 first_top_key/0,   %% () -> first_top_key(data)
	 first_top_key/1,   %% (Tab)
	 first_tree/0,      %% () -> first_tree(data)
	 first_tree/1,      %% (Tab)
	 last_tree/0,       %% () -> last_tree(data)
	 last_tree/1,       %% (Tab)
	 next_tree/1,       %% (Key) -> next_tree(data, Key)
	 next_tree/2,       %% (Tab, Key)
	 make_tree/1,
	 flatten_tree/1,
	 split_key/1,
	 join_key/1,
	 list_key/2,
	 is_list_key/1,
	 encode_id/1,
	 decode_id/1,
	 escape_key/1,
	 unescape_key/1,
	 escape_prefix/1]).

-export([open/1, open/2, close/0, options/1,
	 add_table/1, add_table/2]).

-type key() :: binary().
-type attrs() :: [{atom(), any()}].
-type data() :: binary().
-type conf_tree() :: [conf_node() | conf_obj()].
-type node_key()  :: key() | integer().
-type conf_obj() :: {node_key(), attrs(), data()}.
-type conf_node() :: {node_key(), attrs(), data(), conf_tree()}
		     | {node_key(), conf_tree()}.

%% Macros for kvdb key escaping
%%
%% fixme: bitmap version is actually not that fast as I tought
%% break even is plenty of tests.
%%
-define(bm(A,B), (((1 bsl (((B)-(A))+1))-1) bsl (A))).

-define(bit(A),  (1 bsl (A))).

-define(is_set(BM,A), ((((BM) bsr (A)) band 1) =:= 1)).

-define(bm_lower,   ?bm($a,$z)).
-define(bm_upper,   ?bm($A,$Z)).
-define(bm_digit,   ?bm($0,$9)).
-define(bm_xdigit,  (?bm($0,$9) bor ?bm($A,$F) bor ?bm($a,$f))).
-define(bm_alpha,   (?bm($A,$Z) bor ?bm($a,$z))).
-define(bm_alnum,   (?bm_alpha bor ?bm_digit)).
-define(bm_wsp,     (?bit($\s) bor ?bit($\t))).
-define(bm_space,   (?bm_wsp bor ?bit($\r) bor ?bit($\n))).

-define(bm_id1, (?bm_alpha bor ?bit($_))).
-define(bm_id2, (?bm_id1 bor ?bm_digit bor ?bit($.) bor ?bit($-))).
-define(bm_id3, (?bm_id1 bor ?bm_digit bor ?bit($.) bor ?bit($-) bor ?bit($:))).

-define(is_id1(X), ?is_set(?bm_id1,(X))).
-define(is_id2(X), ?is_set(?bm_id2,(X))).
-define(is_id3(X), ?is_set(?bm_id3,(X))).


instance_() ->
    case get(kvdb_conf) of
	undefined -> ?MODULE;
	Name -> Name
    end.

open(File) ->
    kvdb:open_db(?MODULE, options(File)).

open(File, Options) ->
    case proplists:lookup(name, Options) of
	none ->
	    put(kvdb_conf,?MODULE),
	    kvdb:open(?MODULE, Options++options(File, Options));
	{name,Name} ->
	    put(kvdb_conf,Name),
	    Options1 = proplists:delete(name, Options),
	    kvdb:open(Name, Options1++options(File, Options))
    end.

options(undefined) ->
    options_();
options(File) ->
    [{file, File}|options_()].

options_() ->
    [{backend, sqlite},
     {tables, [data]},
     {encoding, {raw,term,raw}}].

options(File, Opts) ->
    [O || {K,_} = O <- options(File), not lists:keymember(K,1,Opts)].

-spec add_table(kvdb:table()) -> ok.
%% @equiv add_table(T, [])
%%
add_table(T) ->
    add_table(T, []).

-spec add_table(kvdb:table(), kvdb:options()) -> ok.
%% @doc Adds a table to the `kvdb_conf' database. By default, `kvdb_conf'
%% uses a table called `data', which is created when the database is first
%% created. Additional tables can opt for different types of indexing, or
%% encoding of the `Value' part. The `Key' part must always be `raw', since
%% the `kvdb_conf' API requires the keys to be of type `binary()', and the
%% object structure must be `{Key, Attributes, Value}'. The default encoding
%% is `{raw, term, raw}'.
%%
%% (While it is of course possible to store binary keys with any type of
%% encoding, enforcing `raw' encoding ensures that this restriction is not
%% subverted through the normal `kvdb' API).
%%
%% All other table options supported by {@link kvdb:add_table/3} are also
%% supported here.
%% @end
add_table(T, Opts) ->
    {Enc, Opts1} = case lists:keyfind(encoding, 1, Opts) of
		       {_, E} -> {E, Opts};
		       false ->
			   Edef = kvdb:info(instance_(), encoding),
			   {Edef, [{encoding, Edef}|Opts]}
		   end,
    case Enc of
	{raw,_,_} -> ok;
	Other -> error({illegal_encoding, [T, Other]})
    end,
    kvdb:add_table(instance_(), T, Opts1).

close() ->
    kvdb:close(instance_()).


-spec read(key()) -> {ok, conf_obj()} | {error, any()}.
%% @doc Reads a configuration object from the database
%%
%% The returned item is always the single node. See read_tree(Prefix) on how to read an
%% entire tree structure.
%% @end
read(Key) when is_binary(Key) ->
    read(data, Key).

read(Tab, Key) when is_binary(Key) ->
    kvdb:get(instance_(), Tab, Key).

-spec write(conf_obj()) -> ok.
%% @doc Writes a configuration object into the database.
%%
%% Each node or leaf in the tree is stored as a separate object, so updating
%% or inserting a node or leaf in the tree is a very cheap operation.
%%
%% Note that the `kvdb_conf' API only accepts keys of type `binary()',
%% even though it allows kvdb_conf tables to select a different key encoding.
%% @end
write({K, _As, _Data} = Obj) when is_binary(K) ->
    write(data, Obj).

write(Tab, {K, _As, _Data} = Obj) when is_binary(K) ->
    case kvdb:put(instance_(), Tab, Obj) of
	ok ->
	    ok;
	{error, _} = Error ->
	    Error
    end.

update_counter(K, Incr) when is_binary(K), is_integer(Incr) ->
    update_counter(data, K, Incr).

update_counter(Tab, K, Incr) when is_binary(K), is_integer(Incr) ->
    kvdb:update_counter(instance_(), Tab, K, Incr).

delete(K) when is_binary(K) ->
    delete(data, K).

delete(Tab, K) when is_binary(K) ->
    kvdb:delete(instance_(), Tab, K).


delete_all(Prefix) when is_binary(Prefix) ->
    delete_all(data, Prefix).

delete_all(Tab, Prefix) when is_binary(Prefix) ->
    delete_all_(Tab, kvdb:prefix_match(instance_(), Tab, Prefix, 100)).

delete_all_(Tab, {Objs, Cont}) ->
    _ = [delete(Tab, K) || {K,_,_} <- Objs],
    delete_all_(Tab, Cont());
delete_all_(_Tab, done) ->
    ok.

all() ->
    all(data).

all(Tab) ->
    all_(kvdb:first(instance_(), Tab), Tab).

all_({ok, {K,_,_} = Obj}, Tab) ->
    [Obj | all_(kvdb:next(instance_(), Tab, K), Tab)];
all_(done, _) ->
    [].

first() -> first(data).
last () -> last(data).
next(K) when is_binary(K) -> next(data, K).
prev(K) when is_binary(K) -> prev(data, K).

first(Tab) -> kvdb:first(instance_(), Tab).
last (Tab) -> kvdb:last(instance_(), Tab).

next(Tab, K) when is_binary(K) -> kvdb:next(instance_(), Tab, K).
prev(Tab, K) when is_binary(K) -> kvdb:prev(instance_(), Tab, K).

first_tree() ->
    first_tree(data).

first_tree(Tab) ->
    case first(Tab) of
	{ok, {First, _, _}} ->
	    read_tree(Tab, First);
	done ->
	    []
    end.

last_tree() ->
    last_tree(data).

last_tree(Tab) ->
    case last(Tab) of
	{ok, {K,_,_}} ->
	    Top = hd(raw_split_key(K)),
	    read_tree(Tab, Top);
	done ->
	    []
    end.

next_tree(K) when is_binary(K) ->
    next_tree(data, K).

next_tree(Tab, K) when is_binary(K) ->
    case next_at_level(Tab, K) of
	{ok, Next} ->
	    read_tree(Tab, Next);
	done ->
	    []
    end.

next_at_level(K) when is_binary(K) ->
    next_at_level(data, K).

next_at_level(Tab, K) when is_binary(K) ->
    Len = length(raw_split_key(K)),
    Sz = byte_size(K),
    case next(Tab, << K:Sz/binary, $+ >>) of
	{ok, {Next,_,_}} ->
	    case length(SplitNext = raw_split_key(Next)) of
		Len ->
		    {ok, Next};
		L when L > Len ->
		    {ok, raw_join_key(lists:sublist(SplitNext, 1, Len))};
		_ ->
		    done
	    end;
	done ->
	    done
    end.

first_top_key() ->
    first_top_key(data).

first_top_key(Tab) ->
    case kvdb:first(instance_(), Tab) of
	{ok, Obj} ->
	    {ok, element(1, Obj)};
	_ ->
	    done
    end.

-spec read_tree(Prefix::binary()) -> conf_tree().
%% @doc Read a configuration (sub-)tree matching Prefix.
%%
%% This function does a prefix match on the configuration database, and builds a tree
%% from the result. The empty binary will result in the whole tree being built.
%% @end
read_tree(Prefix) when is_binary(Prefix) ->
    read_tree(data, Prefix).

read_tree(Tab, Prefix) when is_binary(Prefix) ->
    {Objs,_} = kvdb:prefix_match(instance_(), Tab,
				 escape_prefix(Prefix), infinity),
    make_tree(Objs).

-spec make_tree([conf_obj()]) -> conf_tree().
%% @doc Converts an ordered list of configuration objects into a configuration tree.
%% @end
%%
make_tree(Objs) ->
    make_tree_([split_key_part(O) || O <- Objs]).

split_key_part({K,A,V}) ->
    {split_key(K), A, V}.

-spec split_key(binary()) -> [binary()].
%% @doc Splits a `kvdb_conf` key into a list of key parts
%%
%% Example: `split_key(<<"a*b*c">>) -> [<<"a">>,<<"b">>,<<"c">>].'
%% @end
%%
split_key(K) when is_binary(K) ->
    [decode_list_key(Id) || Id <- re:split(K, "\\*", [{return,binary}])].

%% Used when we don't care about escaping/unescaping, or know it doesn't matter.
raw_split_key(K) when is_binary(K) ->
    re:split(K, "\\*", [{return,binary}]).

-spec join_key([binary()]) -> binary().
%% @doc Creates a kvdb_conf key out of a list of key parts
%%
%% (See {@link split_key/1}).
%%
%% Example: `join_key([<<"a">>, <<"b">>, <<"c">>]) -> <<"a*b*c">>'
%% @end
%%
join_key([{K,I}|T]) when is_binary(K), is_integer(I) ->
    join_key(T, list_key(K,I));
join_key([H|T]) when is_binary(H) ->
    join_key(T, encode_id(H));
join_key([]) ->
    <<>>.

join_key([{K,I}|T], Acc) when is_binary(K), is_integer(I) ->
    join_key(T, <<Acc/binary, "*", (list_key(K,I))/binary>>);
join_key([H|T], Acc) ->
    join_key(T, <<Acc/binary, "*", (encode_id(H))/binary>>);
join_key([], Acc) ->
    Acc.



%% Used when we don't care about escaping/unescaping, or know it doesn't matter.
raw_join_key([H|T]) when is_binary(H) ->
    Rest = << <<"*", B/binary>> || <<B/binary>> <- T >>,
    << H/binary, Rest/binary >>.

escape_key(K) ->
    join_key(split_key(K)).

unescape_key(K) ->
    [H|T] = split_key(K),
    iolist_to_binary([H | [[$*,X] || X <- T]]).

escape_prefix(<<>>) -> <<>>;
escape_prefix(P) ->
    escape_key(P).

%% Encoding users @ as an escape character followed by the escaped char
%% hex-coded (e.g. "@" -> "@40", "/" -> "@2F"). In order to know that the
%% id has been encoded - so we don't encode it twice - we prepend a '='
%% to the encoded id. Since '=' lies between ASCII numbers and letters
%% (just as '@' does), it won't upset the kvdb sort order.
%%
%% As a consequence, no unescaped ID string may begin with '='.
%%
encode_id(L) when is_list(L) ->
    encode_id(list_to_binary(L));
encode_id(<<$=, _/binary>> = Enc) ->
    Enc;
encode_id(I) when is_integer(I) ->
    encode_id(list_to_binary(integer_to_list(I)));
encode_id(Bin) when is_binary(Bin) ->
    Enc = << <<(id_char(C))/binary>> || <<C>> <= Bin >>,
    <<$=, Enc/binary>>.

decode_id(<<$=, Enc/binary>>) ->
    decode_id_(Enc);
decode_id(Bin) when is_binary(Bin) ->
    Bin.

decode_id_(<<$@, A, B, Rest/binary>>) ->
    <<(list_to_integer([A,B], 16)):8/integer, (decode_id_(Rest))/binary>>;
decode_id_(<<C, Rest/binary>>) ->
    <<C, (decode_id_(Rest))/binary>>;
decode_id_(<<>>) ->
    <<>>.

decode_list_key(<<$=, Enc/binary>>) ->
    decode_list_key_(Enc);
decode_list_key(Bin) ->
    case is_list_key(Bin) of
	{true, Base, Pos} ->
	    {Base, Pos};
	false ->
	    Bin
    end.

%% very similar to decode_id_/1 above.
decode_list_key_(K) ->
    decode_list_key_(K, <<>>).

decode_list_key_(<<"@", A, B, Rest/binary>>, Acc) ->
    decode_list_key_(
      Rest, <<Acc/binary, (list_to_integer([A,B], 16)):8/integer>>);
decode_list_key_(<<"[", Rest/binary>>, Acc) ->
    Sz = byte_size(Rest),
    N = Sz - 1,
    case Rest of
	<<Pb:N/binary, "]">> ->
	    {Acc, list_to_integer(binary_to_list(Pb))};
	_ ->
	    decode_list_key_(Rest, <<Acc/binary, "]">>)
    end;
decode_list_key_(<<C, Rest/binary>>, Acc) ->
    decode_list_key_(Rest, << Acc/binary, C:8 >>);
decode_list_key_(<<>>, Acc) ->
    Acc.


id_char(C) ->
    case ?is_id2(C) of
	true -> <<C>>;
	false when C =< 255 ->
	    <<$@, (to_hex(C bsr 4)):8/integer, (to_hex(C)):8/integer >>
    end.

to_hex(C) ->
    element((C band 16#f)+1, {$0,$1,$2,$3,$4,$5,$6,$7,$8,$9,
			      $A,$B,$C,$D,$E,$F}).


list_key(Name, Pos) when is_binary(Name), is_integer(Pos), Pos >= 0 ->
    %% IX = list_to_binary(integer_to_list(Pos, 19)),
    IX = list_to_binary(pos_key(Pos)),
    <<(encode_id(Name))/binary, "[", IX/binary, "]">>.

pos_key(ID) when is_integer(ID), ID >= 0, ID =< 16#ffffffff ->
    tl(integer_to_list(16#100000000+ID,16));
pos_key(<<ID:32/integer>>) ->
    tl(integer_to_list(16#100000000+ID,16)).


is_list_key(I) when is_integer(I) -> false;
is_list_key(K) ->
    case re:run(K, <<"\\[">>, [global]) of
	{match, Ps} ->
	    {P,_} = lists:last(lists:flatten(Ps)),
	    Sz = byte_size(K),
	    N = Sz - P -2,
	    case K of
		<<Base:P/binary, "[", Ib:N/binary, "]">> ->
		    Pos = list_to_integer(binary_to_list(Ib), 16),
		    {true, Base, Pos};
		_ ->
		    false
	    end;
	_ ->
	    false
    end.


make_tree_([]) ->
    [];
make_tree_([{[{B,I} = H], A, V}|Rest]) ->
    {Rest1, Rest2} = lists:splitwith(fun({[{X,_}|_], _, _}) -> X == B;
					(_) -> false
				     end,
				     Rest),
    {Children, Next} = children(H, Rest1),
    case Children of
	[] ->
	    [{B,[{I, A, V} | make_tree_i(B,pad_levels(Next))]} |
	     make_tree_(Rest2)];
	[_|_] ->
	    [{B,[{I, A, V, make_tree_(pad_levels(Children))}
		 | make_tree_i(B, pad_levels(Next))]}
	     | make_tree_(Rest2)]
    end;
make_tree_([{B,I}|Rest]) when is_binary(B), is_integer(I) ->
    {Rest1, Rest2} = lists:splitwith(fun({[{X,_}|_], _, _}) -> X == B;
					(_) -> false
				     end,
				     Rest),
    {Children, Next} = children({B,I}, Rest1),
    case Children of
	[_|_] ->
	    [{B, [{I, make_tree_(pad_levels(Children))}
		  | make_tree_i(B, pad_levels(Next))]}
	     | make_tree_(Rest2)]
    end;
make_tree_([H | Rest]) when is_binary(H) ->
    {Children, Next} = children(H, Rest),
    case Children of
	[] ->
	    make_tree_(Next);
	[_|_] ->
	    [{H, make_tree_(pad_levels(Children))} | make_tree_(Next)]
    end;
make_tree_([{[H], A, V}|Rest]) ->
    {Children, Next} = children(H, Rest),
    case Children of
	[] ->
	    [{H, A, V} | make_tree_(Next)];
	[_|_] ->
	    [{H, A, V, make_tree_(pad_levels(Children))} | make_tree_(Next)]
    end;
make_tree_([{[_,_|_],_,_}|_] = Tree) ->
    make_tree_(pad_levels(Tree)).

make_tree_i(B, [{B, I} = H|Rest]) ->
    {Children, Next} = children(H, Rest),
    case Children of
	[_|_] ->
	    [{I, make_tree_(pad_levels(Children))} | make_tree_i(B, Next)]
    end;
make_tree_i(B, [{[{B, I} = H], A, V}|Rest]) ->
    {Children, Next} = children(H, Rest),
    case Children of
	[] ->
	    [{I, A, V} | make_tree_i(B, Next)];
	[_|_] ->
	    [{I, A, V, make_tree_(pad_levels(Children))} |
	     make_tree_i(B, Next)]
    end;
make_tree_i(_, []) ->
    [].


-spec write_tree(_Parent::key(), conf_tree()) -> ok.
%% @doc Writes a configuration tree under the given parent.
write_tree(Parent, Tree) ->
    write_tree(data, Parent, Tree).

write_tree(Table, Parent, Tree) when is_binary(Parent) ->
    Objs = lists:flatten([flatten_tree(T, Parent) || T <- Tree]),
    [write(Table, Obj) || Obj <- Objs],
    ok.

-spec flatten_tree(conf_tree()) -> [conf_obj()].
%% @doc Converts a configuration tree into an ordered list of configuration objects.
%% @end
%%
flatten_tree(Tree) when is_list(Tree) ->
    lists:flatten([flatten_tree(T, <<>>) || T <- Tree]).

flatten_tree({K, C}, Parent) ->
    C1 = lists:map(fun(X) when is_integer(element(1,X)) ->
			   Kl = list_key(K, element(1,X)),
			   setelement(1, X, Kl);
		      (X) when is_binary(element(1, X)) ->
			   K1 = next_key(K, element(1, X)),
			   setelement(1, X, K1)
		   end, C),
    [flatten_tree(Ch, Parent) || Ch <- C1];
flatten_tree({K, A, V}, Parent) ->
    case lists:member({1,1}, A) of
	true ->
	    [];
	false ->
	    Key = next_key(Parent, K),
	    [{Key, A, V}]
    end;
flatten_tree({K, A, V, C}, Parent) ->
    Key = next_key(Parent, K),
    case lists:member({1,1}, A) of
	false ->
	    [{Key, A, V} | [flatten_tree(Ch, Key) || Ch <- C]];
	true  ->
	    [flatten_tree(Ch, Key) || Ch <- C]
    end.

%% there may be missing top- or intermediate nodes in the tree.
%% If so, insert an empty node.
%%
pad_levels([{[_],_,_}|_] = Children) ->
    Children;
pad_levels([{[H,_|_],_,_}|_] = Children) ->
    [H | Children].
%% pad_levels([{[H,_|_],_,_}|_] = Children) ->
%%     [{[H], [{1,1}], <<>>}|Children].

children(H, Objs) ->
    children(H, Objs, []).

children(H, [{[H|T],A,V}|Rest], Acc) ->
    children(H, Rest, [{T,A,V}|Acc]);
children(_, Rest, Acc) ->
    {lists:reverse(Acc), Rest}.



-spec store_tree(Tree::conf_tree()) -> ok.
%% @doc Store a configuration tree in the database.
%%
%% Each node in the tree will be stored as a separate object in the database.
%%
store_tree(Tree) when is_list(Tree) ->
    store_tree(data, Tree).

store_tree(Tab, Tree) when is_list(Tree) ->
    [store_tree(Tab, T, <<>>) || T <- Tree],
    ok.

store_tree(Tab, {_, _, _} = Node, Parent) ->
    store_node(Tab, Node, [], Parent);
store_tree(Tab, {K,V,D,C}, Parent) when is_list(C) ->
    store_node(Tab, {K,V,D}, C, Parent).

store_node(Tab, {K, Attrs, Data}, Children, Parent) when is_binary(K) ->
    Key = next_key(Parent, K),
    write(Tab, {Key, Attrs, Data}),
    [store_tree(Tab, Child, Key) || Child <- Children].


next_key(<<>>, Key) ->
    Key;
next_key(Parent, Key) ->
    join_key([Parent,Key]).
    %% << Parent/binary, "*", Key/binary >>.
