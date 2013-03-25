#!/usr/bin/env python

class QueryException(Exception):

    def __init__(self, message):
        self.message = message

class QueryMessage(object):

    def __init__(self, message):
        self.message = message 

    def __str__(self):
        return self.message 
