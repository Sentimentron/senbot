#!/usr/bin/env python

# Pyparsing query grammar for Sentimentron

import string 
import types 

from pyparsing import *
from parsing.models import *

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
keyword         = Optional(keyword_modifier) + potential_keywrd

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

	print query, type(query), type(query) == types.ListType

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


if __name__ == "__main__":

	for f in [domain.parseString(x) for x in ["guardian.co.uk",
		"blogs-04.huffingtonpost.com"
	]]:
		print f


	print keyword.parseString("-\"Huffington Post\"")

	for c, f in enumerate([keyword.parseString(x) for x in ["Huffington", "huffington",
	"\"Huffington Post\"", "+\"huffington post\"", "-\"huffinton post\"", "-huffington"]]):
		print c, f

	queries = ["Barack Obama",
	'"Barack Obama"',
	"Barack AND Obama",
	"+Barack AND Obama",
	"Barack Obama foxnews.com",
	"Barack OR Obama",
	"Barack Obama AND John McCain foxnews.com",
	"(Barack OR Obama) AND (John AND McCain) foxnews.com"]
	for c,f in enumerate([query.parseString(x) for x in queries]):
		print c, recursively_quantify(f.asList())