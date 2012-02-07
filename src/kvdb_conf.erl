 -module(kvdb_conf).

-export([open/1,
	 all/0,
	 options/1,
	 store_tree/1]).

-export([test_tree/0]).

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

open(File) ->
    kvdb:open_db(?MODULE, options(File)).

all() ->
    all(kvdb:first(?MODULE, data)).

all({ok, {K,_,_} = Obj}) ->
    [Obj | all(kvdb:next(?MODULE, data, K))];
all(done) ->
    [].


options(File) ->
    [{file, File},
     {backend, sqlite},
     {tables, [data]},
     {encoding, {raw,term,raw}}].

read_tree(Prefix) ->
    Objs = kvdb:prefix_match(?MODULE, data, Prefix),
    make_tree([split_key(O) || O <- Objs], []).

split_key({K,_,_}) ->
    re:split(K,"\\*", [{return,string}]).

make_tree([{K,A,V}|T], Prev) ->
    Parts = split_key(K),
    foo.

store_tree(Tree) ->
    store_tree(Tree, <<>>).

store_tree({K, Attrs, ChildData}, Parent) when is_binary(K) ->
    Key = next_key(Parent, K),
    if is_binary(ChildData) ->
	    put_data({Key, Attrs, ChildData});
       is_list(ChildData) ->
	    put_data({Key, Attrs, <<>>}),
	    [store_tree(Child, Key) || Child <- ChildData]
    end.

next_key(<<>>, Key) ->
    Key;
next_key(Parent, Key) ->
    << Parent/binary, "*", Key/binary >>.

put_data({_,_,_} = Obj) ->
    kvdb:put(?MODULE, data, Obj).

test_tree() ->
    {<<"a">>,[],[{<<"1">>,[],
		  [
		   {<<"aa">>,[],<<"aa data">>},
		   {<<"bb">>,[],<<"bb data">>}
		  ]
		 },
		{<<"2">>,[],
		 [
		  {<<"zz">>, [], <<"zz data">>},
		  {<<"yy">>, [], <<"yy data">>}
		 ]}
		]}.
