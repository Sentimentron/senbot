#!/usr/bin/env python

from parsing.models import *
from parsing.parser import *
from core import recursive_map, get_celery
from celery import chain, group
from celery.result import AsyncResult
from celery.exceptions import TimeoutError
import types
from tasks import get_site_id, get_site_docs, \
    get_keyword_id, get_keyword_docs, get_document_date, \
    get_document_links, get_phrase_relevance, get_document_sentiment
import itertools 
from collections import Counter 
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

def perform_document_date_resolution(documents):
    return [get_document_date.delay((d)) for d in documents]

def perform_document_link_resolution(documents):
    return group(get_document_links.subtask((d,)) for d in documents).apply_async()

def perform_document_sentiment_resolution(documents):
    return group(get_document_sentiment.subtask((d,)) for d in documents).apply_async()

def perform_document_coverage_estimation(documents):
    return group(get_coverage_estimate.subtask((d,)) for d in documents).apply_async()

def resolve_document_property(results):
    ret = {}
    for doc in results.iterate():
        doc_id, sen = doc 
        ret[doc_id] = sen 
    return ret 

def perform_phrase_relevance_resolution(documents, keywords_dict):
    return group(get_phrase_relevance.subtask((d, keywords_dict[d])) for d in documents).apply_async()

def resolve_document_links(results):
    ret = {}
    for item in results.iterate():
        _id, links, domain = item 
        if domain not in ret:
            ret[domain] = {}
        cur = ret[domain]
        for key in links:
            if key not in cur:
                cur[key] = 0
            cur[key] += links[key] 
    return ret 

def resolve_phrase_relevance(results):
    ret = {}
    for item in results.iterate():
        doc_id, count = item 
        ret[doc_id] = int(count)
    return ret 

def resolve_document_dates(result):
    ret = {}
    for _id, method, date in (r.get() for r in result):
        ret[_id] = (method, date)
        print _id, method, date
    return ret 

def perform_site_docs_resolution(item):
    if type(item) != QueryDomain:
        return item 

    domain = item.domain
    result = chain(get_site_id.subtask(args=(domain,)), get_site_docs.subtask())()
    return SiteDocResolutionPlaceholder(result)


def resolve_all_documents(item, doc_keywords_dict): 
    # TODO: modify this to return the keyword identifiers too
    # Write a neew method for site resul
    if not isinstance(item, AsyncPlaceholder):
        return item 

    result = item.resolve()
    if isinstance(item, KeywordDocResolutionPlaceholder):
        keyword_id, docs = result 
        for d in docs:
            if d not in doc_keywords_dict:
                doc_keywords_dict[d] = set([])
            doc_keywords_dict[d].add(keyword_id)
        return docs 

    return result

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


def resolve_literal_documents(iterable, doc_keywords_dict):
    # If this is iterable, apply to all sublevels
    if hasattr(iterable, '__iter__'):
        iterable = [resolve_literal_documents(i, doc_keywords_dict) for i in iterable]

    # Pull together things at this level 
    if isinstance(iterable, QueryKeywordModifier):
        kw = iterable.item 
        iterable = type(iterable)(resolve_all_documents(kw, doc_keywords_dict))
    
    return iterable 

def combine_retrieved_documents(iterable):

    # If this is iterable, apply combine_retrieve_documents to all sublevels
    if hasattr(iterable, '__iter__'):
        # Need to check for literals
        iterable = list(itertools.chain.from_iterable([combine_retrieved_documents(i) for i in iterable]))
        require  = [i for i in iterable if isinstance(i, QueryKeywordLiteralModifier)]
        exclude  = [i for i in iterable if isinstance(i, QueryKeywordExclusionModifier)]

        require = list(itertools.chain.from_iterable([i.item for i in require]))
        exclude = list(itertools.chain.from_iterable([i.item for i in exclude]))

        print len(require), len(exclude), len(iterable),

        if len(require) > 0:
            iterable = [i for i in iterable if i in require]
        print len(iterable),
        if len(exclude) > 0:
          iterable = [i for i in iterable if i not in exclude]
        print len(iterable)
    else:
        iterable = [iterable]

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
    
    doc_keywords_dict = {}
    # Got a problem: ignores literal keyword modifiers
    inter = parsed
    inter = recursive_map(inter, perform_site_docs_resolution)
    inter = recursive_map(inter, perform_keyword_expansions)
    inter = recursive_map(inter, resolve_keyword_expansions)
    inter = recursive_map(inter, perform_keyword_docs_resolution)
    inter = recursive_map(inter, perform_keywordlt_docs_resolution)
    inter = recursive_map(inter, lambda x: resolve_all_documents(x, doc_keywords_dict))
    inter = recursive_map(inter, lambda x: resolve_literal_documents(x, doc_keywords_dict))
    inter = combine_retrieved_documents(inter)

    # Build the document properties dict
    date_results = perform_document_date_resolution(inter)
    sen_results  = perform_document_sentiment_resolution(inter)
    link_results = perform_document_link_resolution(inter)
    phrase_results = perform_phrase_relevance_resolution(inter, doc_keywords_dict)
    coverage_res = perform_document_coverage_estimation(inter)
    print resolve_document_dates(date_results)
    print resolve_document_property(sen_results)
    print resolve_phrase_relevance(phrase_results)
    print resolve_document_links(link_results)
    print resolve_document_property(coverage_res)