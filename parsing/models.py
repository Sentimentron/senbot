#!/usr/bin/env python

# Object specification 
class ParseContainer(object):

	def __init__(self, s, loc, toks):
		self.s, self.loc, self.toks = s, loc, toks

	def __repr__(self):
		return "%s(%s)" % (type(self), {'s': self.s, 'loc': self.loc, 'toks':self.toks})


class QueryDomain(ParseContainer):
	
	def __init__(self, s, loc, toks):
		self.domain = ''.join(toks) 

	def __repr__(self):
		return "QueryDomain(%s)" % (self.domain,)

class QueryKeyword(ParseContainer):

	def __init__(self, s, loc, toks):
		for keyword in toks:
			self.keyword = keyword 

	@classmethod
	def from_str(self, s):
		return QueryKeyword(None, None, [s])

	def __repr__(self):
		return "QueryKeyword(%s)" % (self.keyword,)

class QueryKeywordModifier(ParseContainer):

	def __init__(self, item):
		self.item = item

	def __repr__(self):
		return "%s(%s)" % (type(self), self.item)

class QueryKeywordExclusionModifier(QueryKeywordModifier):
	pass 

class QueryKeywordLiteralModifier(QueryKeywordModifier):
	pass 

class QueryContainer(object):

	def __init__(self, _list=[]):
		self._list = _list

	def __len__(self):
		return len(self._list)

	def __iter__(self):
		for item in self._list:
			yield item 

	def __reversed__(self):
		return reversed(self._list)

	def __repr__(self):
		return "%s(%s)" % (type(self), self._list)

	def append(self, item):
		self._list.append(item)

	def __getitem__(self, k):
		return self._list[k]

	def __setitem__(self, k, v):
		self._list[k] = v

class QueryJoinOperator(QueryContainer):
	def __repr__(self):
		return "%s(%s)" % ("QueryJoinOperator", self._list)

	def aggregate(self):
		pass 

class QueryIntersection(QueryJoinOperator):
	def __repr__(self):
		return "%s(%s)" % ("QueryIntersection", self._list)

	def aggregate(self):
		ret = None
		for i in self:
			if ret is None:
				ret = set(i)
				continue 
			ret = ret.intersection(i)
		return list(ret)

class QueryDifference(QueryJoinOperator):
	def __repr__(self):
		return "%s(%s)" % ("QueryIntersection", self._list)

	def aggregate(self):
		ret = None 
		for i in reversed(self):
			if ret is None:
				ret = set(i)
				continue 
			ret = ret.difference(i)
		return list(ret)

class QueryUnion(QueryJoinOperator):
	def __repr__(self):
		return "%s(%s)" % ("QueryUnion", self._list)

	def aggregate(self):
		ret = None 
		for i in self:
			if ret is None:
				ret = set(i)
				continue 
			ret = ret.union(i)
		return list(ret)

class Query(QueryJoinOperator):

	def aggregate(self):
		pass 

class OrQuery(Query):

	def aggregate(self):
		ret = None 
		for i in self:
			if ret is None:
				ret = set(i)
				continue 
			ret = ret.union(i)
		return list(ret)

class AndQuery(Query):
	def aggregate(self):
		ret = None
		for i in self:
			if ret is None:
				ret = set(i)
				continue 
			ret = ret.intersection(i)
		return list(ret)

class NotQuery(Query):

	def aggregate(self):
		ret = None 
		for i in reversed(self):
			if ret is None:
				ret = set(i)
				continue 
			ret = ret.difference(i)
		return list(ret)
