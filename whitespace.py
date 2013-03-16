#!/usr/bin/env python

from lookup.models import WhitespaceExpansionTrieNode

from celery import Task, registry
from core import get_celery
celery = get_celery()

class WhiteSpaceKWExpand(Task):

	def __init__(self):

		self.tree = None 

	def run(self, keyword):
		return self.tree.find(keyword)

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