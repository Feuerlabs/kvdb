%% -*- erlang -*-

Nonterminals
expr time_spec in_spec at_spec in_expr at_expr each_spec each_expr
repeat_spec until_spec time_unit.

Terminals
integer 'in' 'at' 'each' fixnum '{' '}' ';' ':' ',' '/' '-'
ms seconds hours minutes days months years times repeat until once forever
daily weekly monthly yearly last_day_of_month.

Rootsymbol expr.

expr -> integer : {{in, ?line('$1'), [{'$1',{ms,?line('$1')}}]},
		   {nil,?line('$1')}, {nil, ?line('$1')}}.
expr -> 'in' in_spec : {{in, ?line('$1'), '$2'},
			{nil,?line('$2')}, {nil, ?line('$2')}}.
expr -> 'at' at_spec : {{at, ?line('$1'), '$2'},
		       {nil,?line('$2')}, {nil, ?line('$2')}}.
expr -> '{' time_spec '}' : {'$2', {nil,?line('$2')}, {nil, ?line('$2')}}.
expr -> '{' time_spec ';' repeat_spec '}' : {'$2', '$4', {nil, ?line('$4')}}.
expr -> '{' time_spec ';' repeat_spec ';' until_spec '}' : {'$2', '$4', '$6'}.

time_spec -> 'in' in_spec : {in, ?line('$1'), '$2'}.
time_spec -> 'at' at_spec : {at, ?line('$1'), '$2'}.

repeat_spec -> repeat : {repeat, ?line('$1')}.
repeat_spec -> once : {times, ?line('$1'), {integer, ?line('$1'), 1}}.
repeat_spec -> integer 'times'  : {times, ?line('$1'), '$1'}.
repeat_spec -> 'each' each_spec : {each, ?line('$1'), '$2'}.
repeat_spec -> daily : '$1'.
repeat_spec -> weekly : '$1'.
repeat_spec -> monthly : '$1'.
repeat_spec -> yearly : '$1'.
repeat_spec -> last_day_of_month : '$1'.

until_spec -> forever : {nil, ?line('$1')}.
until_spec -> until forever : {nil, ?line('$1')}.
until_spec -> until at_spec : {at, ?line('$1'), '$2'}.
until_spec -> at_spec : {at, ?line('$1'), '$1'}.

in_spec -> integer               : [{'$1', {ms, ?line('$1')}}].
in_spec -> in_expr               : ['$1'].
in_spec -> in_expr ',' in_spec   : ['$1' | '$3'].

in_expr -> integer time_unit   : {'$1', '$2'}.
in_expr -> last_day_of_month   : '$1'.

at_spec -> at_expr             : ['$1'].
at_spec -> at_expr ',' at_spec : ['$1' | '$3'].

at_expr -> integer '/' integer '/' integer :
	       {date, ?line('$1'), {y('$5'),m('$3'),d('$1')}}.
at_expr -> integer '-' integer '-' integer :
	       {date, ?line('$1'), {y('$1'),m('$3'),d('$5')}}.
at_expr -> integer ':' integer ':' integer :
	       {time, ?line('$1'), {{h('$1'),mi('$3'),s('$5')}, 0}}.
at_expr -> integer ':' integer ':' fixnum  :
	       {time, ?line('$1'),
		{{h('$1'),mi('$3'),s(int('$5'))}, frac('$5')}}.

each_spec -> each_expr               : ['$1'].
each_spec -> each_expr ',' each_spec : ['$1' | '$3'].

each_expr -> integer time_unit : {'$1', '$2'}.


time_unit -> ms : '$1'.
time_unit -> seconds   : '$1'.
time_unit -> hours   : '$1'.
time_unit -> minutes   : '$1'.
time_unit -> days   : '$1'.
time_unit -> months   : '$1'.
time_unit -> years   : '$1'.





Erlang code.

-define(line(T), line_of(T)).

line_of([T|_]) ->
    line_of(T);
line_of(T) when is_tuple(T) ->
    element(2, T).


int({fixnum, L, {I,_}}) ->
    {integer, L, I}.

frac({fixnum, L, {_,F}}) ->
    {integer, L, F}.

y({integer,_,I} = T) when I >= 1000, I =< 9999 ->
    T;
y({integer,L,I}) when I >= 0, I =< 99 ->
    {Y,_,_} = date(),
    {integer, L, (Y div 100) * 100 + I};
y(T) ->
    {error, ?line(T), {invalid_year, T}}.



m({integer,_,I} = T) when I >= 0, I =< 12 ->
    T;
m(T) ->
    {error, ?line(T), {invalid_month, T}}.

d({integer, _, I} = T) when I >= 0, I =< 31 ->
    T;
d(T) ->
    {error, ?line(T), {invalid_day, T}}.

h({integer, _, I} = T) when I >= 0, I =< 23 ->
    T;
h(T) ->
    {error, ?line(T), {invalid_hour, T}}.

mi({integer, _, I} = T) when I >= 0, I =< 59 ->
    T;
mi(T) ->
    {error, ?line(T), {invalid_minute, T}}.

s({integer, _, I} = T) when I >= 0, I =< 59 ->
    T;
s(T) ->
    {error, ?line(T), {invalid_second, T}}.
