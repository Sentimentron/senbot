#!/usr/bin/env python

from parsing.models import *
from parsing.parser import *
from core import recursive_map, get_celery
from celery import chain
from celery.result import AsyncResult
from celery.exceptions import TimeoutError
import types
from tasks import get_site_id, get_site_docs
celery = get_celery()

queries = ["Barack", "McCain",
	"+Barack AND McCain foxnews.com",
	"+Barack AND -\"McCain Oven Chips\" foxnews.com"
]

def expand_keyword(keyword):
	if type(keyword) != QueryKeyword:
		return keyword
	kw = keyword.keyword
	return celery.send_task("cache.ProdWhiteSpaceKWExpand", [kw])

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
	return celery.send_task("tasks.ProdKWIdentityResolve", [keyword])

def resolve_site(item):
	if type(item) == QueryDomain:
		return item 

	domain = item.domain
	return chain(get_site_id(domain), get_site_docs)

def resolve(result):
	print "RESOLVE",type(result), isinstance(result, AsyncResult)
	if not isinstance(result, AsyncResult):
		return result 
	try:
		return result.get()
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
	inter = parsed
	inter = recursive_map(inter, lambda x: expand_keyword(x))
	print inter 
	inter = recursive_map(inter, lambda x: resolve(x))
	print inter
	inter = recursive_map(inter, lambda x: resolve_keyword(x))
	inter = recursive_map(inter, lambda x: resolve(x))
	print inter 
	inter = recursive_map(inter, lambda x: resolve_site(x))
	inter = recursive_map(inter, lambda x: resolve(x))
	print inter
