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
%%% @doc kvdb definitions.
%%% @end

-record(conf_tree, {root = <<>> :: key(),
		    tree = []   :: conf_tree()
		   }).

-type key() :: binary().
-type attrs() :: [{atom(), any()}].
-type data() :: binary().
-type conf_tree() :: [conf_node() | conf_obj()].
-type node_key()  :: key() | integer().
-type conf_obj() :: {node_key(), attrs(), data()}.
-type conf_node() :: {node_key(), attrs(), data(), conf_tree()}
		     | {node_key(), conf_tree()}.
