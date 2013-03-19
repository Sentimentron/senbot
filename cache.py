#!/usr/bin/env python

from celery import Task, registry
from celery.utils.log import get_task_logger
from core import get_celery, get_database_engine_string
from lookup.models import WhitespaceExpansionTrieNode

import cPickle as pickle

logging = get_task_logger(__name__)
celery = get_celery()

class WhiteSpaceKWExpand(Task):

    acks_late = True

    def __init__(self):

        self.tree = None 

    def run(self, keyword):
        return self.tree.find(keyword)

class ProdWhiteSpaceKWExpand(WhiteSpaceKWExpand):

    def __init__(self):

        fp = open('whitespace.pickle', 'r')
        self.tree = pickle.load(fp)

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
