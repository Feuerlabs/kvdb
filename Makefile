.PHONY:	test doc compile trans_test basic_test deps

all: compile

deps:
	rebar get-deps

compile: deps
	rebar compile

doc:
	rebar doc

test: compile
	rebar eunit skip_deps=true

trans_test: compile
	rebar eunit skip_deps=true suite=kvdb_trans

conf_test: compile
	rebar eunit skip_deps=true suite=kvdb_conf

basic_test: compile
	rebar eunit skip_deps=true suite=kvdb