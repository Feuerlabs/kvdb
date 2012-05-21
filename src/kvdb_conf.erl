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

-export([read/1,
	 write/1,
	 update_counter/2,
	 delete/1,
	 delete_all/1,
	 read_tree/1,
	 store_tree/1,
	 all/0,
	 first/0,
	 last/0,
	 next/1,
	 prev/1,
	 next_at_level/1,
	 first_tree/0,
	 last_tree/0,
	 next_tree/1,
	 make_tree/1,
	 flatten_tree/1]).

-export([open/1, open/2, close/0, options/1]).


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

close() ->
    kvdb:close(instance_()).


-spec read(key()) -> {ok, conf_obj()} | {error, any()}.
%% @doc Reads a configuration object from the database
%%
%% The returned item is always the single node. See read_tree(Prefix) on how to read an
%% entire tree structure.
%% @end
read(Key) when is_binary(Key) ->
    kvdb:get(instance_(), data, Key).

-spec write(conf_obj()) -> ok.
%% @doc Writes a configuration object into the database.
%%
%% Each node or leaf in the tree is stored as a separate object, so updating or inserting
%% a node or leaf in the tree is a very cheap operation.
%% @end
write({K, As, Data} = Obj) when is_binary(K), is_list(As), is_binary(Data) ->
    case kvdb:put(instance_(), data, Obj) of
	ok ->
	    ok;
	{error, _} = Error ->
	    Error
    end.

update_counter(K, Incr) when is_binary(K), is_integer(Incr) ->
    kvdb:update_counter(instance_(), data, K, Incr).

delete(K) when is_binary(K) ->
    kvdb:delete(instance_(), data, K).

delete_all(Prefix) when is_binary(Prefix) ->
    delete_all_(kvdb:prefix_match(instance_(), data, Prefix, 100)).

delete_all_({Objs, Cont}) ->
    _ = [delete(K) || {K,_,_} <- Objs],
    delete_all_(Cont());
delete_all_(done) ->
    ok.

all() ->
    all(kvdb:first(instance_(), data)).

all({ok, {K,_,_} = Obj}) ->
    [Obj | all(kvdb:next(instance_(), data, K))];
all(done) ->
    [].

first() -> kvdb:first(instance_(), data).
last () -> kvdb:last(instance_(), data).
next(K) -> kvdb:next(instance_(), data, K).
prev(K) -> kvdb:prev(instance_(), data, K).

first_tree() ->
    case first() of
	{ok, {First, _, _}} ->
	    read_tree(First);
	done ->
	    []
    end.

last_tree() ->
    case last() of
	{ok, {K,_,_}} ->
	    Top = hd(split_key_part(K)),
	    read_tree(Top);
	done ->
	    []
    end.

next_tree(K) ->
    case next_at_level(K) of
	{ok, Next} ->
	    read_tree(Next);
	done ->
	    []
    end.

next_at_level(K) ->
    Len = length(split_key_part(K)),
    Sz = byte_size(K),
    case next(<< K:Sz/binary, $+ >>) of
	{ok, {Next,_,_}} ->
	    case length(split_key_part(Next)) of
		Len ->
		    {ok, Next};
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
    {Objs,_} = kvdb:prefix_match(instance_(), data, Prefix, infinity),
    make_tree(Objs).

-spec make_tree([conf_obj()]) -> conf_tree().
%% @doc Converts an ordered list of configuration objects into a configuration tree.
%% @end
%%
make_tree(Objs) ->
    make_tree_([split_key(O) || O <- Objs]).

split_key({K,A,V}) ->
    {split_key_part(K), A, V}.

split_key_part(K) when is_binary(K) ->
    re:split(K, "\\*", [{return,binary}]).

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
    [store_tree(T, <<>>) || T <- Tree],
    ok.

store_tree({_, _, _} = Node, Parent) ->
    store_node(Node, [], Parent);
store_tree({K,V,D,C}, Parent) when is_list(C) ->
    store_node({K,V,D}, C, Parent).

store_node({K, Attrs, Data}, Children, Parent) when is_binary(K) ->
    Key = next_key(Parent, K),
    write({Key, Attrs, Data}),
    [store_tree(Child, Key) || Child <- Children].


next_key(<<>>, Key) ->
    Key;
next_key(Parent, Key) ->
    << Parent/binary, "*", Key/binary >>.

