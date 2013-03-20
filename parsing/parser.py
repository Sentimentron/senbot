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

def process_query(s, l, t):
	contains_and = False 
	contains_or  = False 
	for item in t.asList():
		print item, s
		contains_and = contains_and or item == "AND"
		contains_or  = contains_or  or item == "OR"



	raw_input((contains_and, contains_or, t.asList()))
	if contains_and and contains_or:
		raise ParseException("Ambiguous quantifiers")
	elif not (contains_and or contains_or):
		contains_and = True # Implicitly convert queries without a specification into AND 


	t = t.asList()
	t = [i for i in t if i != "AND"]
	t = [i for i in t if i != "OR"]

	if contains_or:
		return OrQuery(t)
	if contains_and:
		return AndQuery(t)

	raise AssertionError("Shouldn't be here")


#query.setParseAction(process_query)

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
