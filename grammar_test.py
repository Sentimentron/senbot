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


	print keyword.parseString("-\"Huffington Post\"")

	for c, f in enumerate([keyword.parseString(x) for x in ["Huffington", "huffington",
	"\"Huffington Post\"", "+\"huffington post\"", "-\"huffinton post\"", "-huffington"]]):
		print c, f

	queries = ["Barack Obama",
	'"Barack Obama"',
	"Barack AND Obama",
	"+Barack AND Obama",
	"Barack Obama foxnews.com",
	"Barack OR Obama",
	"Barack Obama AND John McCain foxnews.com",
	"(Barack OR Obama) AND (John AND McCain) foxnews.com",
	"+Barack AND -\"McCain Oven Chips\" foxnews.com"]
	for c, q in enumerate(queries):
		print c, q, query.parseString(q).asList()
		print