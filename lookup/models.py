#!/usr/bin/env python

import logging

class TrieNode(object):

	def __init__(self):

		self._children 	= {} 
		self.values     = set([])

	def add_value(self, value):
		self.values.add(value)

	def get_values(self):
		return self.values

	def find(self, key):

		node = self 
		for char in key:
			if char not in node._children:
				return None 
			else:
				node = node._children[char]

		return node.get_values()

	def build(self, key, value):

		node = self 
		for char in key:
			if char not in node._children:
				tn = self.__class__
				node._children[char] = tn()
			node = node._children[char]

		node.add_value(value)

	def __repr__(self):
		if len(self.values) > 1:
			print_item = self.values 
		else:
			print_item = self._children
		return "TrieNode(%s)" % (print_item,)

class UnambiguousTrieNode(TrieNode):

	def __init__(self):
		self._children = {}
		self.value = None 

	def add_value(self, value):
		if self.value != None:
			raise Exception("Already has a value!")
		self.value = value 

	def get_values(self):
		return self.value 

	def __repr__(self):
		return "TrieNode(%s)" % (self.value)

class WhitespaceExpansionTrieNode(TrieNode):

	def build(self, word):
		import itertools

		if ' ' not in word:
			raise ValueError("WhitespaceExpansionTrieNode: only supports strings with spaces")

		def get_all_combinations(of):
			cur = 1
			while cur <= len(of):
				for item in itertools.combinations(of, cur):
					yield item 
				cur += 1

		subwords = word.split(' ')

		for perm in get_all_combinations(subwords):
			subword = ' '.join(perm)
			try:
				super(WhitespaceExpansionTrieNode, self).build(subword, word)
			except Exception as ex:
				logging.error(ex)

class CaseInsensitiveTrieNode(TrieNode):

	def build(self, word):
		word = word.lower()
		super(CaseInsensitiveTrieNode, self).build(word)
