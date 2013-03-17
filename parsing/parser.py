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
modified_keyword= Group(keyword_modifier + potential_keywrd)
keyword         = modified_keyword | potential_keywrd

potential_keywrd.setParseAction(lambda s,l,t: QueryKeyword(s,l,t))
literal_modifier.setParseAction(lambda s,l,t: QueryKeywordLiteralModifier())
exclude_modifier.setParseAction(lambda s,l,t: QueryKeywordExclusionModifier())

# Query parsing 
query          = Forward()
query_element  = Or([domain, keyword])
and_condition  = Literal("AND")
or_condition   = Literal("OR")
join_condition = and_condition | or_condition
query_run      = query_element + join_condition
subquery       = Suppress(Literal('('))+query+Suppress(Literal(')'))
query         << Or([Group(query_run + query), query_element, query_element + query, subquery, Group(subquery + Optional(join_condition) +  query)])

def query_post_sort(*args, **kwargs):
	raw_input(args)
	if len(args) == 0:
		return 

	s, l, t = args
	return t.sort(key = lambda x: type(x))

#query.setParseAction(query_post_sort)

def recursively_quantify(query):

	if type(query) != types.ListType:
		return query 

	contains_and = "AND" in query 
	contains_or  = "OR"  in query 

	if contains_and and contains_or:
		raise ValueError("Ambiguous quantifiers")

	if contains_or:
		return QueryUnion([recursively_quantify(i) for i in query if i != "OR"])
	if contains_and:
		return QueryIntersection([recursively_quantify(i) for i in query if i != "AND"])
	return QueryJoinOperator([recursively_quantify(i) for i in query if i != "AND" and i != "OR"])
