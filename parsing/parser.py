#!/usr/bin/env python

import string 
import types 

from pyparsing import *
from models    import *

# Domain parsing 
label	= Word(string.ascii_lowercase + string.digits + "-")
domain 	= OneOrMore(label + Literal(".")) + label 
domain.setParseAction(lambda s,l,t: QueryDomain(s,l,t))

# Keyword parsing 
raw_keyword 	= Word(string.letters + string.digits)
quoted_keyword  = QuotedString("\"")
potential_keywrd= raw_keyword | quoted_keyword
literal_modifier= Literal("+")
exclude_modifier= Literal("-")
keyword_modifier= literal_modifier | exclude_modifier
modified_keyword= keyword_modifier + potential_keywrd
keyword         = modified_keyword | potential_keywrd

def wrap_modified(s, l, t):
	modifier, modifying = t 
	if modifier == '-':
		return QueryKeywordExclusionModifier(modifying)
	elif modifier == '+':
		return QueryKeywordLiteralModifier(modifying)
	else:
		raise ValueError("That's not a supported modifier!")

modified_keyword.setParseAction(wrap_modified)
potential_keywrd.setParseAction(lambda s,l,t: QueryKeyword(s,l,t))
#literal_modifier.setParseAction(lambda s,l,t: QueryKeywordLiteralModifier())
#exclude_modifier.setParseAction(lambda s,l,t: QueryKeywordExclusionModifier())

# Query parsing 
query          = Forward()
query_element  = Or([domain, keyword])
and_condition  = Literal("AND")
or_condition   = Literal("OR")
join_condition = and_condition | or_condition
and_query_run  = query_element + Suppress(and_condition) + query
or_query_run   = query_element + Suppress(or_condition) + query 
query_run      = or_query_run | and_query_run
subquery       = Suppress(Literal('('))+query+Suppress(Literal(')'))
query         << Or([Group(query_run) + query, Group(query_run), query_element, query_element + query, subquery + query, subquery])

and_query_run.setParseAction(lambda s, l, t: AndQuery(t.asList()))
or_query_run.setParseAction(lambda s, l, t: OrQuery(t.asList()))
query.setParseAction(lambda s, l, t: AndQuery(t.asList()))