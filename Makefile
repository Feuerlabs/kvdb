.PHONY:	test doc

all: compile

compile:
	./rebar compile

doc:
	./rebar doc

test: compile
	./rebar eunit skip_deps=true