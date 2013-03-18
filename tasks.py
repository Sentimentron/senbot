#!/usr/bin/env python

from celery import Task, registry
from core import get_celery, get_database_engine_string
from lookup.models import WhitespaceExpansionTrieNode, UnambiguousTrieNode
from models import Keyword, KeywordAdjacency, KeywordIncidence 
from sqlalchemy import create_engine
from sqlalchemy.exc import *
from sqlalchemy.orm import * 
from sqlalchemy.orm.exc import *
from sqlalchemy.orm.session import Session 
from sqlalchemy.pool import SingletonThreadPool

import logging

celery = get_celery()

class WhiteSpaceKWExpand(Task):

    def __init__(self):

        self.tree = None 

    def run(self, keyword):
        return self.tree.find(keyword)

class ProdWhiteSpaceKWExpand(object):

    def __init__(self):

        # Database connection
        engine = get_database_engine_string()
        logging.info("Using connection string '%s'" % (engine,))
        engine = create_engine(engine, encoding='utf-8', isolation_level = 'READ UNCOMMITTED', poolclass=SingletonThreadPool, echo = False)
        session = Session(bind=engine)

        # Query for keywords
        for k in session.query(Keyword):
            if ' ' not in k.word:
                continue 
            logging.debug(k.word)
            self.build(k.word)
        

class TestWhiteSpaceKWExpand(WhiteSpaceKWExpand):

    KEYWORD_EXPANSIONS = ["Barack Obama",
        "Senator Barack Obama", 
        "President Bush", 
        "Senator John McCain"
    ]

    def __init__(self):
        self.tree = WhitespaceExpansionTrieNode()
        for word in self.KEYWORD_EXPANSIONS:
            self.tree.build(word)

class IdentityResolve(Task):
    def __init__(self):
        self.tree = None 

    def run(self, item):
        return (item, self.tree.find(item))

class TestKWIdentityResolve(IdentityResolve):
    
    KEYWORD_IDENTITIES = [("Barack Obama", 47),
        ("Senator Barack Obama", 49),
        ("President Bush", 20),
        ("Senator John McCain", 76)
    ]

    def __init__(self):
        self.tree = UnambiguousTrieNode()
        for word, _id in self.KEYWORD_IDENTITIES:
            self.tree.build(word, _id)

class DatabaseTask(Task):

    def __init__(self):
        self.engine = get_database_engine_string()
        self.conn   = create_engine(self.engine)

class DocumentMatchFromKeyword(DatabaseTask):

    def run(self, keyword_id):
        session = Session(bind=self.conn)
        ret = set([])
        it = session.query(KeywordAdjacency).filter_by(key1_id = keyword_id)
        for thing in it:
            ret.add(thing.doc_id)

        return ret 

class PhraseMatchFromKeyword(DatabaseTask):

    def run(self, keyword_id):
        session = Session(bind=self.conn)
        ret = set([])
        it  = session.query(KeywordIncidence).filter_by(keyword_id = keyword_id)
        for thing in it:
            ret.add(thing.phrase_id)

        return ret 

class GetPhrasesFromDocID(DatabaseTask):

    def run(self, document_id):
        session = Session(bind=self.conn)
        ret     = set([])
        doc     = session.query(Document).get(document_id)
        
        for sentence in doc.sentences:
            for phrase in sentence.phrases:
                ret.add(phrase)

        return ret 

test_kw_id_resolve = registry.tasks[TestKWIdentityResolve.name]
test_whitespace_kw_expand = registry.tasks[TestWhiteSpaceKWExpand.name]
document_match_keyword_id = registry.tasks[DocumentMatchFromKeyword.name]
phrase_match_keyword_id   = registry.tasks[PhraseMatchFromKeyword.name]
phrase_from_document_id   = registry.tasks[GetPhrasesFromDocID.name]

whitespace_kw_expand = registry.tasks[ProdWhiteSpaceKWExpand.name]
whitespace_kw_expand.delay("Obama")