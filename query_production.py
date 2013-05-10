#!/usr/bin/env python

import datetime
import itertools 
import json
import logging
import sys
import time 
import types

import boto.s3

from boto.s3.connection import S3Connection
from boto.s3.key import Key
from celery import chain, group
from celery.exceptions import TimeoutError
from celery.result import AsyncResult
from core import recursive_map, get_celery, get_database_engine_string, configure_logging
from collections import Counter, defaultdict
from parsing.models import *
from parsing.parser import *
from pyparsing import ParseException
from sqlalchemy import create_engine
from sqlalchemy.exc import *
from sqlalchemy.orm import * 
from sqlalchemy.orm.exc import *
from sqlalchemy.orm.session import Session 
from sqlalchemy.pool import SingletonThreadPool
from sqlalchemy.sql.functions import now

from models import UserQuery
from jobs.queue import QueryQueue
from jobs.messages import QueryException, QueryMessage
from jobs.mail import EmailProcessor
from tasks import get_site_id, get_site_docs, \
    get_keyword_id, get_keyword_docs, get_document_date, \
    get_document_links, get_phrase_relevance, \
    get_document_sentiment, get_document_terms

celery = get_celery()

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
        if expansions is not None:
            expansions = [QueryKeyword.from_str(i) for i in expansions]
        else:
            expansions = []
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
    if isinstance(item, KeywordDocResolutionPlaceholder):
        keyword_id, docs = result 
        for d in docs:
            if d not in doc_keywords_dict:
                doc_keywords_dict[d] = set([])
            doc_keywords_dict[d].add(keyword_id)
        return docs

    return result

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

def _combine_retrieved_documents(inter):
    prompt = False
    t = type(inter)
    # If this is iterable, apply combine_retrieve_documents to all sublevels
    if t is AndQuery or t is OrQuery or t is NotQuery:
        inter = [_combine_retrieved_documents(i) for i in inter]
        documents = inter
        if t is AndQuery:
            inter = set.intersection(*documents)
        elif t is OrQuery:
            inter = set.union(*documents)
        else:
            inter = set.difference(*documents)

    elif hasattr(inter, '__iter__'):
        prompt = True
        inter = [_combine_retrieved_documents(i) for i in inter]
        if len(inter) > 0: 
            inter = set.union(*inter)
        else:
            return set([])
    else:
        return set([inter])

    return inter 

def flatten(x):
    try:
        it = iter(x)
        for i in it:
            for j in flatten(i):
                yield j
    except TypeError:
        yield x

def combine_retrieved_documents(resolved_query):
    return flatten(_combine_retrieved_documents(resolved_query))

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
        'query_time': round(time,1),
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
            if _id in phrases:
                rel_pos, rel_neg = phrases[_id]
            else:
                rel_pos, rel_neg = 0, 0
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


def process_query(query_text, query_identifier):

        start_time = time.time()
        expansions = set([])
        doc_keywords_dict = {}

        # Parse query 
        parsed = None 
        try:
            parsed = query.parseString(query_text).asList()
        except ParseException as ex:
            raise QueryException("Parsing error: '%s'" % ex)

        logging.info(parsed)

        # 
        # Document set resolution 
        # 
        inter = parsed
        yield QueryMessage("Sending domain resolution request...")
        inter = recursive_map(inter, perform_site_docs_resolution)
        yield QueryMessage("Running keyword expansions...")
        inter = recursive_map(inter, perform_keyword_expansions)
        inter = recursive_map(inter, resolve_keyword_expansions)
        yield QueryMessage("Running document matching....")
        inter = recursive_map(inter, lambda x: extract_keywords(x, expansions))
        inter = recursive_map(inter, perform_keyword_docs_resolution)
        inter = recursive_map(inter, perform_keywordlt_docs_resolution)
        inter = recursive_map(inter, lambda x: resolve_all_documents(x, doc_keywords_dict))
        inter = recursive_map(inter, lambda x: resolve_literal_documents(x, doc_keywords_dict))
        yield QueryMessage("Combining retrieved documents...")
        docs = set([i for i in combine_retrieved_documents(inter)])
        logging.info(docs)

        if len(docs) == 0:
            raise QueryException("No documents returned.")

        # 
        # Build the document properties dict
        #
        # Perform RPC calls
        yield QueryMessage("Requesting dates...")
        date_results = perform_document_date_resolution(docs)
        yield QueryMessage("Requesting sentiment...")
        sen_results  = perform_document_sentiment_resolution(docs)
        yield QueryMessage("Generating link summary...")
        link_results = perform_document_link_resolution(docs)
        if len(expansions) > 0:
            yield QueryMessage("Requesting relevant phrases...")
            phrase_results = perform_phrase_relevance_resolution(docs, doc_keywords_dict)
        yield QueryMessage("Generating document summaries...")
        doc_terms    = perform_document_keyterm_extraction(docs)

        # Assemble output 
        yield QueryMessage("Assembling date information...")
        dates     = resolve_document_dates(date_results)
        yield QueryMessage("Assembling sentiment information...")
        sentiment = resolve_document_property(sen_results)
        if len(expansions) > 0:
            yield QueryMessage("Assembling relevant phrase information...")
            phrases   = resolve_phrase_relevance(phrase_results)
        else:
            phrases    = {} 
        yield QueryMessage("Assembling link information...")
        links, dm_map     = resolve_document_links(link_results)
        yield QueryMessage("Assembling document keywords...")
        keywords  = resolve_document_property(doc_terms)

        yield QueryMessage("Writing results...")
        output_to_s3_key(query_identifier, dates, sentiment, phrases, links, dm_map, keywords, expansions, query_text, time.time() - start_time)

        yield QueryMessage("Query completed!")

if __name__ == "__main__":

    configure_logging('info')
    engine = get_database_engine_string()
    logging.info("Using connection string '%s'" % (engine,))
    engine = create_engine(engine, encoding='utf-8', isolation_level = 'READ UNCOMMITTED', poolclass=SingletonThreadPool, echo = False)

    session = Session(bind = engine)

    qq = QueryQueue(engine)
    pm = EmailProcessor()
    for uq_id in qq:
        user_query = session.query(UserQuery).get(uq_id)
        if user_query is None:
            logging.error("Query with id '%d' not found!" % (uq_id,))
            sys.exit(1)
        if user_query.fulfilled is not None:
            logging.info("Query '%d' already fulfilled, skipping....", (user_query.id))
            continue 
        try:    
            for msg in process_query(user_query.text, uq_id):
                user_query.message = msg.message 
                logging.info(msg.message)
                session.commit()

            user_query.fulfilled = now()
            session.commit()
            if user_query.email is not None:
                pm.send_success(user_query.email, user_query.id)
            user_query.email = None 
            session.commit()
        except QueryException as ex:
            except_type, except_class, tb = sys.exc_info()
            logging.critical(traceback.extract_tb(tb))
            logging.critical(except_type)
            logging.critical(except_class)
            logging.critical(ex)
            user_query.message = ex.message
            user_query.cancelled = True
            if user_query.email is not None:
                pm.send_failure(user_query.email, ex.message)
            user_query.email = None
            session.commit() 
        except Exception as ex:
            except_type, except_class, tb = sys.exc_info()
            logging.critical(traceback.extract_tb(tb))
            logging.critical(except_type)
            logging.critical(except_class)
            if user_query.email is not None:
                pm.send_failure(user_query.email, "General query engine error.")
            user_query.email = None
            user_query.message = "Query engine error."
            user_query.cancelled = True
            session.commit() 
        qq.set_completed(uq_id)
        break
