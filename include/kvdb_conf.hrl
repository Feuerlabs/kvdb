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

-type key_part()  :: binary() | {binary(), integer()}.
-type key()       :: binary().
-type attrs()     :: [{atom(), any()}].
-type value()     :: any().
-type conf_data() :: [conf_node() | conf_obj()].

-record(conf_tree, {root = <<>> :: key(),
		    tree = []   :: conf_data()
		   }).
-type conf_tree() :: #conf_tree{}.

-type node_key()  :: key() | integer().
-type conf_obj()  :: {node_key(), attrs(), value()}.
-type conf_node() :: {node_key(), attrs(), value(), conf_tree()}
		   | {node_key(), conf_tree()}.
-type shift_op()  :: up | down | top | bottom.


