%% @doc
%% API to store NETCONF-style config data in kvdb
%%
%% identifiers are stored as a structured key (binary)
%% delimited by:
%%
%% Example:
%% `{system,[{services, [{ssh,[...]}]}]}' would be stored as
%%
%% {"system", []}
%% {"system*services", []}
%% {"system*services*ssh", []}
%%
%% Netconf identifiers can consist of alphanumerics, '-', '_' or '.'. The '*' as delimiter
%% is chosen so that a "wildcard" character can be used that is greater than the delimiter,
%% but smaller than any identifier character.
%% @end

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
	 first_tree/0,      %% () -> first_tree(data)
	 first_tree/1,      %% (Tab)
	 last_tree/0,       %% () -> last_tree(data)
	 last_tree/1,       %% (Tab)
	 next_tree/1,       %% (Key) -> next_tree(data, Key)
	 next_tree/2,       %% (Tab, Key)
	 make_tree/1,
	 flatten_tree/1,
	 split_key/1,
	 join_key/1]).

-export([open/1, open/2, close/0, options/1,
	 add_table/1, add_table/2]).


-type key() :: binary().
-type attrs() :: [{atom(), any()}].
-type data() :: binary().
-type conf_tree() :: [conf_node() | conf_obj()].
-type conf_obj() :: {key(), attrs(), data()}.
-type conf_node() :: {key(), attrs(), data(), conf_tree()}.

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
	    kvdb:open_db(?MODULE, Options++options(File));
	{name,Name} ->
	    put(kvdb_conf,Name),
	    Options1 = proplists:delete(name, Options),
	    kvdb:open_db(Name, Options1++options(File))
    end.

options(File) ->
    [{file, File},
     {backend, sqlite},
     {tables, [data]},
     {encoding, {raw,term,raw}}].

add_table(T) ->
    add_table(T, []).

add_table(T, Opts) ->
    %% Currently, we don't check options. The representation must be
    %% {Key, Attrs, Value} for the kvdb_conf API to work.
    Opts1 =
	case lists:keyfind(encoding, 1, Opts) of
	    false ->
		[{encoding, {raw,term,raw}}|Opts];
	    _ ->
		Opts
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
%% Each node or leaf in the tree is stored as a separate object, so updating or inserting
%% a node or leaf in the tree is a very cheap operation.
%% @end
write({K, As, Data} = Obj) when is_binary(K), is_list(As), is_binary(Data) ->
    write(data, Obj).

write(Tab, {K, As, Data} = Obj) when
      is_binary(K), is_list(As), is_binary(Data) ->
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
    all_(kvdb:first(instance_()), Tab).

all_({ok, {K,_,_} = Obj}, Tab) ->
    [Obj | all_(kvdb:next(instance_(), data, K), Tab)];
all_(done, _) ->
    [].

first() -> first(data).
last () -> last(data).
next(K) -> next(data, K).
prev(K) -> prev(data, K).

first(Tab) -> kvdb:first(instance_(), Tab).
last (Tab) -> kvdb:last(instance_(), Tab).
next(Tab, K) -> kvdb:next(instance_(), Tab, K).
prev(Tab, K) -> kvdb:prev(instance_(), Tab, K).

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
	    Top = hd(split_key(K)),
	    read_tree(Tab, Top);
	done ->
	    []
    end.

next_tree(K) ->
    next_tree(data, K).

next_tree(Tab, K) ->
    case next_at_level(Tab, K) of
	{ok, Next} ->
	    read_tree(Tab, Next);
	done ->
	    []
    end.

next_at_level(K) ->
    next_at_level(data, K).

next_at_level(Tab, K) ->
    Len = length(split_key(K)),
    Sz = byte_size(K),
    case next(Tab, << K:Sz/binary, $+ >>) of
	{ok, {Next,_,_}} ->
	    case length(SplitNext = split_key(Next)) of
		Len ->
		    {ok, Next};
		L when L > Len ->
		    {ok, join_key(lists:sublist(SplitNext, 1, Len))};
		_ ->
		    done
	    end;
	done ->
	    done
    end.

-spec read_tree(Prefix::binary()) -> conf_tree().
%% @doc Read a configuration (sub-)tree matching Prefix.
%%
%% This function does a prefix match on the configuration database, and builds a tree
%% from the result. The empty binary will result in the whole tree being built.
%% @end
read_tree(Prefix) ->
    read_tree(data, Prefix).

read_tree(Tab, Prefix) ->
    {Objs,_} = kvdb:prefix_match(instance_(), Tab, Prefix, infinity),
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
    re:split(K, "\\*", [{return,binary}]).

-spec join_key([binary()]) -> binary().
%% @doc Creates a kvdb_conf key out of a list of key parts
%%
%% (See {@link split_key/1}).
%%
%% Example: `join_key([<<"a">>, <<"b">>, <<"c">>]) -> <<"a*b*c">>'
%% @end
%%
join_key([H|T]) ->
    Rest = << <<"*", B/binary>> || <<B/binary>> <- T >>,
    << H/binary, Rest/binary >>.

make_tree_([]) ->
    [];
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


-spec flatten_tree(conf_tree()) -> [conf_obj()].
%% @doc Converts a configuration tree into an ordered list of configuration objects.
%% @end
%%
flatten_tree(Tree) when is_list(Tree) ->
    lists:flatten([flatten_tree(T, <<>>) || T <- Tree]).

flatten_tree({K, A, V}, Parent) ->
    Key = next_key(Parent, K),
    [{Key, A, V}];
flatten_tree({K, A, V, C}, Parent) ->
    Key = next_key(Parent, K),
    [{Key, A, V} | [flatten_tree(Ch, Key) || Ch <- C]].

%% there may be missing top- or intermediate nodes in the tree.
%% If so, insert an empty node.
%%
pad_levels([{[_],_,_}|_] = Children) ->
    Children;
pad_levels([{[H,_|_],_,_}|_] = Children) ->
    [{[H], [], <<>>}|Children].

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
    << Parent/binary, "*", Key/binary >>.

