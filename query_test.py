#!/usr/bin/env python

from parsing.models import *
from parsing.parser import *
from core import recursive_map, get_celery
from celery.result import AsyncResult
from celery.exceptions import TimeoutError
import types

celery = get_celery()

queries = ["Barack", "McCain",
	"+Barack AND McCain foxnews.com",
	"+Barack AND -\"McCain Oven Chips\" foxnews.com"
]

def expand_keyword(keyword):
	if type(keyword) != QueryKeyword:
		return keyword
	kw = keyword.keyword
	return celery.send_task("tasks.ProdWhiteSpaceKWExpand", [kw])

def resolve_keyword(keyword):
	kw = None
	if type(keyword) == type(set([])):
		return [resolve_keyword(i) for i in keyword]
	if type(keyword) == types.StringType:
		if keyword == "AND" or keyword == "OR":
			return keyword 
		kw = keyword
	elif type(keyword) == QueryKeyword:
		kw = keyword.keyword 
	else:
		return keyword 

	assert kw != None 
	return test_kw_id_resolve.delay(kw)

def resolve(result):
	print "RESOLVE",type(result), isinstance(result, AsyncResult)
	if not isinstance(result, AsyncResult):
		return result 
	try:
		return result.get(timeout=1)
	except TimeoutError:
		return None 

for c, q in enumerate(queries):

	if c != 2:
		continue
	
	# Parse query 
	parsed = query.parseString(q).asList()
	print c
	print "PARSED", parsed
	
	# Got a problem: ignores literal keyword modifiers
	inter = recursive_map(parsed, lambda x: expand_keyword(x))
	print inter 
	inter = recursive_map(inter, lambda x: resolve(x))
	print inter
	#inter = recursive_map(inter, lambda x: resolve_keyword(x))
	#inter = recursive_map(inter, lambda x: resolve(x))
	#print inter 
