#!/usr/bin/env python

import logging
from collections import defaultdict

class TrieNode(object):

    def __init__(self):

        self._children  = {} 
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

class UnambiguousTrieNode(object):

    def __init__(self):
        self._words = {}
        self._structure = {} 

    def _build_internal_structure(self, word, val):
        node = self._structure
        for char in word:
            if char not in node:
                node[char] = {}
            node = node[char]

        if '_VALUES' not in node:
            node['_VALUES'] = None 
        if node['_VALUES'] != None:
            raise ValueError("Shouldn't have another value at this node")
        node['_VALUES'] = val

    def find(self, key):
        node = self._structure 
        for char in key:
            if char not in node:
                return None 
            else:
                node = node[char]

        if '_VALUES' not in node:
            return None 

        return node['_VALUES']

    def build(self, word, value):
        self._build_internal_structure(word, value)

class WhitespaceExpansionTrieNode(object):

    def __init__(self):
        self._size  = -1
        self._words = {}
        self._structure = {} 

    def _get_internal_value(self, word):
        self._size += 1 
        self._words[self._size] = word 
        return self._size 

    def _build_internal_structure(self, word, val):
        node = self._structure
        for char in word:
            if char not in node:
                node[char] = {}
            node = node[char]

        if '_VALUES' not in node:
            node['_VALUES'] = set([])
        node['_VALUES'].add(val)

    def find(self, key):
        node = self._structure 
        key = key.lower()
        for char in key:
            if char not in node:
                return None 
            else:
                node = node[char]

        if '_VALUES' not in node:
            return None 

        return node['_VALUES']

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

        subwords = word.lower().split(' ')

        for subword in subwords:
            self._build_internal_structure(subword, word)

class CaseInsensitiveTrieNode(TrieNode):

    def build(self, word):
        word = word.lower()
        super(CaseInsensitiveTrieNode, self).build(word)
