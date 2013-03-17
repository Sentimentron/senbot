#!/usr/bin/env python

from celery import Task, registry
from core import get_celery, get_database_engine_string
from lookup.models import WhitespaceExpansionTrieNode
from models import Keyword 
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

test_whitespace_kw_expand = registry.tasks[TestWhiteSpaceKWExpand.name]