.PHONE:	doc

all: compile test doc

compile:
	./rebar compile

doc:
	./rebar doc

test: compile
	./rebar eunit skip_deps=true