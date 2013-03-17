#!/usr/bin/env python

from parsing.models import *
from parsing.parser import *
from whitespace import test_whitespace_kw_expand 
from core import recursive_map
from celery.result import AsyncResult
from celery.exceptions import TimeoutError
queries = ["Barack", "McCain",
	"+Barack AND McCain foxnews.com",
	"+Barack AND -\"McCain Oven Chips\" foxnews.com"
]

def expand_keyword(keyword):
	print "EXPAND",type(keyword), type(keyword) == QueryKeyword
	if type(keyword) != QueryKeyword:
		return keyword
	kw = keyword.keyword
	return test_whitespace_kw_expand.delay(kw)

def resolve(result):
	print "RESOLVE",type(result), isinstance(result, AsyncResult)
	if not isinstance(result, AsyncResult):
		return result 
	try:
		return result.get(timeout=1)
	except TimeoutError:
		return None 

for c, q in enumerate(queries):
	
	# Parse query 
	parsed = query.parseString(q).asList()
	print c
	print "PARSED", parsed
	
	inter = recursive_map(parsed, lambda x: expand_keyword(x))
	print inter 
	inter = recursive_map(inter, lambda x: resolve(x))
	raw_input(inter)