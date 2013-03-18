#!/usr/bin/env python

from core import get_celery, get_database_engine_string
from lookup.models import WhitespaceExpansionTrieNode, UnambiguousTrieNode
from models import Keyword, KeywordAdjacency, KeywordIncidence 
from sqlalchemy import create_engine, MetaData
from sqlalchemy.exc import *
from sqlalchemy.orm import * 
from sqlalchemy.orm.exc import *
from sqlalchemy.orm.session import Session 
from sqlalchemy.pool import SingletonThreadPool
import MySQLdb.cursors
import string 
import cPickle as pickle 
import sys

class CachableItem(object):

    NAME = "cache.pickle"

    def __init__(self):
        pass 

    def pickle(self, filename=None):
        pass

class WhiteSpaceCache(CachableItem):
    
    NAME="whitespace.pickle"

    def __init__(self):
        self.tree = WhitespaceExpansionTrieNode()

        # Database connection
        engine = get_database_engine_string()
        logging.info("Using connection string '%s'" % (engine,))
        engine = create_engine(engine, encoding='utf-8', isolation_level = 'READ UNCOMMITTED', poolclass=SingletonThreadPool, echo = False, connect_args={'cursorclass': MySQLdb.cursors.SSCursor})
        meta = MetaData(engine, reflect=True)
        conn = engine.connect()
        session = Session(bind=conn)

        # Query for keywords
        sql = "SELECT word FROM keywords WHERE word collate latin1_general_cs REGEXP ('^([A-Z][a-z]+ ){1,2}([A-Z][a-z]+)$')"
        for word, in session.execute(sql):
            logging.debug(word)
            self.tree.build(word)

    def pickle(self, filename = None):
        if filename is None:
            filename = self.NAME 

        fp = open(filename, 'w')
        pickle.dump(self.tree, fp)

if __name__ == "__main__":

    if "--whitespace" in sys.argv:
        p = WhiteSpaceCache()
        p.pickle()