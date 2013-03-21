#!/usr/bin/env python

from celery import Task, registry
from celery.utils.log import get_task_logger
from core import get_celery, get_database_engine_string
from lookup.models import WhitespaceExpansionTrieNode, UnambiguousTrieNode
from models import Keyword, KeywordAdjacency, KeywordIncidence 
from sqlalchemy import create_engine, MetaData
from sqlalchemy.exc import *
from sqlalchemy.orm import * 
from sqlalchemy.orm.exc import *
from sqlalchemy.orm.session import Session 
from sqlalchemy.pool import *
import cPickle as pickle
import string 

logging = get_task_logger(__name__)

celery = get_celery()

class IdentityResolve(Task):
    def __init__(self):
        self.tree = None 

    def run(self, item):
        return (item, self.tree.find(item))

class DatabaseTask(Task):

    acks_late = True

    def __init__(self):
        self.engine = get_database_engine_string()
        self.engine = create_engine(self.engine, 
            pool_recycle=60*60,
            isolation_level="READ UNCOMMITTED")

class ProdKWIdentityResolve(DatabaseTask):

    def run(self, keyword):
        kw = None
        session = Session(bind = self.engine)
        it = session.query(Keyword).filter_by(word = keyword)
        try:
            kw = it.one()
        except NoResultFound:
            return None 

        ret = kw.id
        session.close()
        return ret

get_keyword_id = registry.tasks[ProdKWIdentityResolve.name]

class ProdSiteIdentityResolve(DatabaseTask):

    def run(self, domain):

        con = self.engine.connect()
        sql = """SELECT id FROM domains 
            WHERE `key` LIKE %s
        """
        ret = set([])
        for _id, in con.execute(sql, '%'+domain):
            ret.add(_id)

        con.close()
        return ret

get_site_id = registry.tasks[ProdSiteIdentityResolve.name]

class ProdSiteDocsResolve(DatabaseTask):

    def run(self, domain_ids):

        ret = set([])
        con = self.engine.connect()
        for domain_id in domain_ids:
            sql = """SELECT MAX(documents.id) FROM documents
                JOIN articles ON documents.article_id = articles.id 
                WHERE articles.domain_id = %d
                GROUP BY articles.id""" % (domain_id, )

            print sql
            for _id, in con.execute(sql):
                ret.add(_id)

        con.close()
        return ret

get_site_docs = registry.tasks[ProdSiteDocsResolve.name]

class ProdKeywordDocsResolve(DatabaseTask):

    def run(self, keyword_id):

        sql = """SELECT DISTINCT documents.id FROM documents 
        JOIN keyword_adjacencies ON keyword_adjacencies.doc_id = documents.id 
        WHERE key1_id = %d OR key2_id = %d""" % (keyword_id, keyword_id)

        con = self.engine.connect()
        ret = set([])
        for _id, in con.execute(sql):
            ret.add(_id)

        con.close()
        return keyword_id, ret 

get_keyword_docs = registry.tasks[ProdKeywordDocsResolve.name]

class ProdDocLinksSummary(DatabaseTask):

    def run(self, doc_id):

        sql = """SELECT domains.`key`, COUNT(*) FROM links_absolute
            JOIN domains ON links_absolute.domain_id = domains.id 
            WHERE links_absolute.document_id = %d
            GROUP BY (domains.id)""" % (doc_id,)

        ret = {}
        con = self.engine.connect()
        for key, count in con.execute(sql):
            ret[key] = count 

        logging.info("Fetching domain key for %d", doc_id)
        sql = """SELECT domains.`key` FROM domains 
        JOIN articles ON articles.domain_id = domains.id
        JOIN documents ON documents.article_id = articles.id
        WHERE documents.id = %d""" % (doc_id,)
        for domain, in con.execute(sql):
            pass 

        sql = """SELECT COUNT(*) FROM links_relative WHERE document_id = %d"""
        for count, in con.execute(sql % (doc_id,)):
            pass 

        assert domain != None 
        assert count  != None 

        if count > 0:
            if domain not in ret:
                ret[domain] = 0

            ret[domain] += count 

        con.close()
        return (doc_id, ret, domain)

get_document_links = registry.tasks[ProdDocLinksSummary.name]

class ProdDocPublished(DatabaseTask):

    def run(self, document_id):
        con = self.engine.connect() 
        # Certain date resolution
        sql = """SELECT certain_dates.date FROM certain_dates
        WHERE doc_id = %d""" % (document_id, ) 
        print sql 
        for date, in con.execute(sql):
            con.close()
            return document_id, "Certain", date 

        # Uncertain date resolution
        sql = """SELECT uncertain_dates.date FROM uncertain_dates 
        WHERE doc_id = %d""" % (document_id, )
        print sql 
        for date, in con.execute(sql):
            con.close()
            return document_id, "Uncertain", date 

        sql = """SELECT articles.crawled FROM articles 
            JOIN documents ON documents.article_id = articles.id 
            WHERE documents.id = %d""" % (document_id,)
        print sql
        for date, in con.execute(sql):
            con.close()
            return document_id, "Crawled", date 

        con.close()
        raise AssertionError("Shouldn't be here!")

get_document_date = registry.tasks[ProdDocPublished.name]

class PhraseRelevanceFromKeywordDocId(DatabaseTask):

    def run(self, doc_id, keyword_identifiers):
        ret = None 

        sql = """SELECT COUNT(*) FROM documents 
            JOIN sentences ON sentences.document = documents.id 
            JOIN phrases ON phrases.sentence = sentences.id 
            JOIN keyword_incidences ON keyword_incidences.phrase_id = phrases.id 
            WHERE keyword_incidences.keyword_id IN (%s)
            AND documents.id = %d""" % (','.join([str(i) for i in keyword_identifiers]), doc_id)

        con = self.engine.connect()

        for count, in con.execute(sql):
            ret = int(count)

        con.close()
        return (doc_id, ret)

get_phrase_relevance = registry.tasks[PhraseRelevanceFromKeywordDocId.name]

class DocumentSentimentFromId(DatabaseTask):

    def run(self, doc_id):

        con = self.engine.connect()
        ret = None 

        sql = """SELECT pos_phrases, neg_phrases, pos_sentences, neg_sentences
            FROM documents 
            WHERE documents.id = %d""" % (doc_id, )

        for pos_phrases, neg_phrases, pos_sentences, neg_sentences in con.execute(sql):
            ret = (pos_phrases, neg_phrases, pos_sentences, neg_sentences)

        return doc_id, ret 

get_document_sentiment = registry.tasks[DocumentSentimentFromId.name]

class GetDocumentKeywordSpans(DatabaseTask):

    def run(self, doc_id):

        con = self.engine.connect()
        sql = """SELECT COUNT(*) AS c, keyword_adjacencies.key1_id, keywords1.word, keyword_adjacencies.key2_id, keywords2.word FROM keyword_adjacencies 
        JOIN keywords AS keywords1 ON keywords1.id = key1_id 
        JOIN keywords AS keywords2 ON keywords2.id = key2_id
        WHERE keyword_adjacencies.doc_id = %d
        GROUP BY key1_id, key2_id 
        ORDER BY c DESC LIMIT 0,5""" % (doc_id,)

        word_forms = {}
        for c, key1, word1, key2, word2 in con.execute(sql):
            word1, word2 = [x.lower() for x in [word1, word2]]
            if word1 in word_forms:
                form = word_forms[word1]
                form.append(word2)
                word_forms.pop(word1, None)
                word_forms[word2] = form 
            else:
                word_forms[word2] = [word1, word2]

        terms = [' '.join(word_forms[w]) for w in word_forms]
        con.close()

        return doc_id, terms 

get_document_terms = registry.tasks[GetDocumentKeywordSpans.name]

