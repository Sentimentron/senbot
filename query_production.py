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
    get_document_links, get_phrase_relevance, \
    get_document_sentiment, get_document_terms
import itertools 
from collections import Counter, defaultdict
import time 
import datetime
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import boto.s3
import json
celery = get_celery()

queries = ["Barack", "McCain",
    "Barack AND foxnews.com",
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

def perform_document_keyterm_extraction(documents):
    return group(get_document_terms.subtask((d,)) for d in documents).apply_async()

def resolve_document_property(results):
    ret = {}
    for doc in results.iterate():
        doc_id, sen = doc 
        ret[doc_id] = sen 
    return ret 

def perform_phrase_relevance_resolution(documents, keywords_dict):
    return group(get_phrase_relevance.subtask((d, keywords_dict[d])) for d in documents if d in keywords_dict).apply_async()

def resolve_document_links(results):
    ret = {}; dm_map = {}
    for item in results.iterate():
        _id, links, domain = item 
        dm_map[_id] = domain
        if domain not in ret:
            ret[domain] = {}
        cur = ret[domain]
        for key in links:
            if key not in cur:
                cur[key] = 0
            cur[key] += links[key] 
    return ret, dm_map 

def resolve_phrase_relevance(results):
    ret = {}
    for item in results.iterate():
        doc_id, (pos, neg) = item 
        ret[doc_id] = (int(pos), int(neg))
    return ret 

def resolve_document_dates(result):
    ret = {}
    for _id, method, date in (r.get() for r in result):
        ret[_id] = (method, date)
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
    print type(item)
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
        iterable = resolve_all_documents(kw, doc_keywords_dict)
    
    return iterable 

def _combine_retrieved_documents(iterable):
    prompt = False
    # If this is iterable, apply combine_retrieve_documents to all sublevels
    if isinstance(iterable, Query):
        iterable = iterable.aggregate()
    
    if hasattr(iterable, '__iter__'):
        prompt = True 
        iterable = [_combine_retrieved_documents(i) for i in iterable]
    else:
        prompt = False

    return iterable 

def combine_retrieved_documents(iterable):
    return itertools.chain.from_iterable(_combine_retrieved_documents(iterable))

def extract_keywords(iterable, output):
    if type(iterable) is QueryKeyword:
        output.add(iterable.keyword)
    return iterable 

def output_to_s3_key(keyname, dates, sentiment, phrases, 
    links, dm_map, keywords, expansions, input_text,
    time):

    # Collect the ids of documents within all returned domains
    ids = {}
    for _id in dm_map:
        d = dm_map[_id]
        if d not in ids:
            ids[d] = set([])
        ids[d].add(_id)

    # Build the auxillary section
    aux = {d : {} for d in ids}
    for key in aux:
        item = aux[key]
        item["coverage"]   = 0 # Backwards compatability
        item["links"]      = Counter()
        item["keywords"]   = set([])

        doc_keywords = Counter()

        for _id in ids[key]:
            if _id in links:
                doc_links      = links[_id]
                item["links"] += doc_links 
            if _id in keywords:
                doc_keywords.update(keywords[_id])

        item["keywords"] = [i for i, c in doc_keywords.most_common(5)]

    # Build the information section
    info = {
        'documents_returned' : len(dm_map),
        'keywords_returned'  : len(expansions),
        'keywords_set' : [word for word in expansions],
        'phrases_returned' : 0,
        'query_text': input_text, 
        'query_time': time,
        'result_version': 2,
        'sentences_returned': 0,
        'using_keywords': int(len(keywords) > 0)
    }

    for _id in sentiment:
        pos_phrases, neg_phrases, pos_sentences, neg_sentences, label = sentiment[_id]
        info['phrases_returned'] += pos_phrases + neg_phrases
        info['sentences_returned'] += pos_sentences + neg_sentences

    sites = {d: {'details': {}, 'docs':[]} for d in ids}

    def convert_date_method(method):
        if method == "Certain":
            return 0
        elif method == "Uncertain":
            return 1
        elif method == "Crawled":
            return 2
        return -1

    def convert_date(input_date):
        start = datetime.datetime(year=1970,month=1,day=1)
        diff = input_date - start
        return int(diff.total_seconds()*1000)

    for domain in ids:
        for _id in ids[domain]:
            method, date = dates[_id]
            method = convert_date_method(method)
            date   = convert_date(date)
            pos_phrases, neg_phrases, pos_sentences, neg_sentences, label = sentiment[_id]
            print phrases
            rel_pos, rel_neg = phrases[_id]
            entry = [
                method, 
                date,
                pos_phrases,
                neg_phrases,
                pos_sentences,
                neg_sentences,
                rel_pos,
                rel_neg,
                label,
                0, # Probability measure: backwards compatible
                _id 
            ]
            sites[domain]['docs'].append(entry)

    out = {'aux': aux, 'info': info, 'siteData': sites}
    out = json.dumps(out)
    con = S3Connection()
    bucket = con.get_bucket('results.sentimentron.co.uk')
    key = Key(bucket)
    key.key = 'results/%s' % (keyname)
    key.set_contents_from_string(out)


for c, q in enumerate(queries):

    if c != 2:
        continue
    
    start_time = time.time()

    # Parse query 
    parsed = query.parseString(q).asList()
    print c
    print "PARSED", parsed
    doc_keywords_dict = {}
    expansions = set([])

    # 
    # Document set resolution 
    # 
    inter = parsed
    inter = recursive_map(inter, perform_site_docs_resolution)
    inter = recursive_map(inter, perform_keyword_expansions)
    inter = recursive_map(inter, resolve_keyword_expansions)
    inter = recursive_map(inter, lambda x: extract_keywords(x, expansions))
    inter = recursive_map(inter, perform_keyword_docs_resolution)
    inter = recursive_map(inter, perform_keywordlt_docs_resolution)
    inter = recursive_map(inter, lambda x: resolve_all_documents(x, doc_keywords_dict))
    inter = recursive_map(inter, lambda x: resolve_literal_documents(x, doc_keywords_dict))
    #print inter
    inter = [i for i in itertools.chain.from_iterable(combine_retrieved_documents(inter))]
    #inter = [i for i in [j for j in combine_retrieved_documents(inter)]]
    print inter

    # 
    # Build the document properties dict
    #
    # Perform RPC calls
    date_results = perform_document_date_resolution(inter)
    sen_results  = perform_document_sentiment_resolution(inter)
    link_results = perform_document_link_resolution(inter)
    phrase_results = perform_phrase_relevance_resolution(inter, doc_keywords_dict)
    doc_terms    = perform_document_keyterm_extraction(inter)

    # Assemble output 
    dates     = resolve_document_dates(date_results)
    sentiment = resolve_document_property(sen_results)
    phrases   = resolve_phrase_relevance(phrase_results)
    links, dm_map     = resolve_document_links(link_results)
    keywords  = resolve_document_property(doc_terms)

    output_to_s3_key("test", dates, sentiment, phrases, links, dm_map, keywords, expansions, q, time.time() - start_time)

    print dm_map 
