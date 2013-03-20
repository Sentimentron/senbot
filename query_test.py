#!/usr/bin/env python

from parsing.models import *
from parsing.parser import *
from core import recursive_map, get_celery
from celery import chain
from celery.result import AsyncResult
from celery.exceptions import TimeoutError
import types
from tasks import get_site_id, get_site_docs, get_keyword_id, get_keyword_docs 
import itertools 
celery = get_celery()

queries = ["Barack", "McCain",
    "+Barack AND McCain foxnews.com",
    "+Barack AND -\"McCain Oven Chips\" foxnews.com"
]

class ResultPlaceholder(object):

    def __init__(self, result):
        self.result = result 

class AsyncPlaceholder(ResultPlaceholder):

    def __init__(self, result):
        if not isinstance(result, AsyncResult):
            raise TypeError(type(result))
        self.result = result

    def resolve(self):
        return self.result.get()

class SiteDocResolutionPlaceholder(AsyncPlaceholder):
    pass 

class KeywordDocResolutionPlaceholder(AsyncPlaceholder):
    pass 

class KeywordExpansionPlaceholder(AsyncPlaceholder):

    def __init__(self, result, original):
        super(KeywordExpansionPlaceholder, self).__init__(result)
        self.original = original 

    def resolve(self):
        expansions = super(KeywordExpansionPlaceholder, self).resolve() 
        expansions = [QueryKeyword.from_str(i) for i in expansions]
        expansions.append(self.original)
        return OrQuery(expansions)

def perform_keyword_expansions(keyword):
    if type(keyword) != QueryKeyword:
        return keyword
    kw = keyword.keyword
    result = celery.send_task("cache.ProdWhiteSpaceKWExpand", [kw])
    return KeywordExpansionPlaceholder(result, keyword)

def resolve_keyword_expansions(keyword):
    if type(keyword) != KeywordExpansionPlaceholder:
        return keyword 

    return keyword.resolve() 

def perform_keyword_docs_resolution(keyword):
    if type(keyword) != QueryKeyword:
        return keyword 

    kw = keyword.keyword 
    result = chain(get_keyword_id.subtask(args=(kw,)), get_keyword_docs.subtask())() 
    return KeywordDocResolutionPlaceholder(result)

def perform_keywordlt_docs_resolution(keyword):
    print type(keyword)
    if not isinstance(keyword, QueryKeywordModifier):
        return keyword 

    kw = keyword.item
    result = perform_keywordlt_docs_resolution(kw)

    return type(keyword)(result)

def perform_site_docs_resolution(item):
    if type(item) != QueryDomain:
        return item 

    domain = item.domain
    result = chain(get_site_id.subtask(args=(domain,)), get_site_docs.subtask())()
    return SiteDocResolutionPlaceholder(result)

def resolve_literal_documents(item):
    if not isinstance(item, QueryKeywordModifier):
        return item 
    return type(item)(resolve_all_documents(item.item)) 

def resolve_all_documents(item):
    if not isinstance(item, AsyncPlaceholder):
        return item 

    return item.resolve()

def _combine_retrieved_documents(item):
    if not isinstance(item, Query):
        return item 

    return item.aggregate()


def perform_keywordlt_docs_resolution(iterable):
    # If this is iterable, apply to all sublevels
    if hasattr(iterable, '__iter__'):
        iterable = [perform_keywordlt_docs_resolution(i) for i in iterable]

    # Pull together things at this level 
    if isinstance(iterable, QueryKeywordModifier):
        kw = iterable.item 
        iterable = type(iterable)(perform_keyword_docs_resolution(kw))
    
    return iterable 


def resolve_literal_documents(iterable):
    # If this is iterable, apply to all sublevels
    if hasattr(iterable, '__iter__'):
        iterable = [resolve_literal_documents(i) for i in iterable]

    # Pull together things at this level 
    if isinstance(iterable, QueryKeywordModifier):
        kw = iterable.item 
        iterable = type(iterable)(resolve_all_documents(kw))
    
    return iterable 

def combine_retrieved_documents(iterable):

    # If this is iterable, apply combine_retrieve_documents to all sublevels
    if hasattr(iterable, '__iter__'):
        # Need to check for literals
        iterable = [combine_retrieved_documents(i) for i in iterable]
        require  = [i for i in iterable if isinstance(i, QueryKeywordLiteralModifier)]
        exclude  = [i for i in iterable if isinstance(i, QueryKeywordExclusionModifier)]

        require = [j for j in [i.item for i in require]]
        exclude = [j for j in [i.item for j in exclude]]

        print len(requre), len(exclude), len(iterable)

        if len(require) > 0:
            iterable = [i for i in iterable if i in require]
        print len(iterable),
        if len(exclude) > 0:
          iterable = [i for i in iterable if i not in exclude]
        print len(iterable)

    # Pull together document identifiers if possible
    if isinstance(iterable, Query):
        iterable = iterable.aggregate()

    return iterable 

for c, q in enumerate(queries):

    if c != 2:
        continue
    
    # Parse query 
    parsed = query.parseString(q).asList()
    print c
    print "PARSED", parsed
    
    # Got a problem: ignores literal keyword modifiers
    inter = parsed
    inter = recursive_map(inter, perform_site_docs_resolution)
    inter = recursive_map(inter, perform_keyword_expansions)
    inter = recursive_map(inter, resolve_keyword_expansions)
    inter = recursive_map(inter, perform_keyword_docs_resolution)
    inter = recursive_map(inter, perform_keywordlt_docs_resolution)
    inter = recursive_map(inter, resolve_all_documents)
    inter = recursive_map(inter, resolve_literal_documents)
    print inter
    inter = combine_retrieved_documents(inter)
    print inter
