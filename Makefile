.PHONY:	test doc compile trans_test basic_test

all: compile

compile:
	rebar compile

doc:
	rebar doc

test: compile
	rebar eunit skip_deps=true

trans_test: compile
	rebar eunit skip_deps=true suite=kvdb_trans

basic_test: compile
	rebar eunit skip_deps=true suite=kvdb