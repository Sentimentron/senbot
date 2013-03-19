#!/usr/bin/env python

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
