#!/usr/bin/env python

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
	"(Barack OR Obama) AND (John AND McCain) foxnews.com"]
	for c,f in enumerate([query.parseString(x) for x in queries]):
		print c, recursively_quantify(f.asList())