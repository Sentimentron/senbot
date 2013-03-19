#!/usr/bin/env python

from celery import Task, registry
from celery.utils.log import get_task_logger
from core import get_celery, get_database_engine_string
from lookup.models import WhitespaceExpansionTrieNode, UnambiguousTrieNode
from models import Keyword, KeywordAdjacency, KeywordIncidence 
from sqlalchemy import create_engine, MetaData
from sqlalchemy.exc import *
from sqlalchemy.orm import * 
from sqlalchemy.orm.exc import *
from sqlalchemy.orm.session import Session 
from sqlalchemy.pool import SingletonThreadPool
import cPickle as pickle
import MySQLdb.cursors
import string 

logging = get_task_logger(__name__)

celery = get_celery()

class IdentityResolve(Task):
    def __init__(self):
        self.tree = None 

    def run(self, item):
        return (item, self.tree.find(item))

class ProdKWIdentityResolve(Task):

    acks_late = True

    def __init__(self):
        self.engine = get_database_engine_string()
        self.engine = create_engine(self.engine)

    def run(self, keyword):
        kw = None
        session = Session(bind = self.engine)
        it = session.query(Keyword).filter_by(word = keyword)
        try:
            kw = it.one()
        except NoResultFound:
            return None 

        ret = kw.id
        session.close()
        return ret  

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

