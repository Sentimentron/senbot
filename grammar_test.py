#!/usr/bin/env python

from pprint import pprint

# Pyparsing query grammar for Sentimentron
from parsing.parser import *
from parsing.models import *

if __name__ == "__main__":

	for f in [domain.parseString(x) for x in ["guardian.co.uk",
		"blogs-04.huffingtonpost.com"
	]]:
		print f



	queries = ["Barack Obama",
	"Barack & Obama",
	"+Barack & Obama",
	"Barack Obama foxnews.com",
	"Barack & Obama",
	"Barack Obama & John McCain foxnews.com",
	"(Barack | Obama) & (John | McCain) foxnews.com",
	"+Barack & -\"McCain Oven Chips\" foxnews.com"]
	for c, q in enumerate(queries):
		print c, q, query.parseString(q).asList()
		print
