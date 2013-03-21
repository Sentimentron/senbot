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
from sqlalchemy.pool import SingletonThreadPool
import cPickle as pickle
import MySQLdb.cursors
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
            poolclass=SingletonThreadPool, 
            pool_recycle=5, 
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
        session = Session(bind = self.engine)

        sql = """SELECT id FROM domains 
            WHERE `key` LIKE "%%%s"
        """ % (domain,)
        ret = set([])
        for _id, in session.execute(sql):
            ret.add(_id)

        return ret 

get_site_id = registry.tasks[ProdSiteIdentityResolve.name]

class ProdSiteDocsResolve(DatabaseTask):

    def run(self, domain_ids):

        ret = set([])
        session = Session(bind = self.engine)
        for domain_id in domain_ids:
            sql = """SELECT MAX(documents.id) FROM documents
                JOIN articles ON documents.article_id = articles.id 
                WHERE articles.domain_id = %d
                GROUP BY articles.id""" % (domain_id, )

            print sql
            for _id, in session.execute(sql):
                ret.add(_id)

            session.close()
        return ret

get_site_docs = registry.tasks[ProdSiteDocsResolve.name]

class ProdKeywordDocsResolve(DatabaseTask):

    def run(self, keyword_id):

        sql = """SELECT DISTINCT documents.id FROM documents 
        JOIN keyword_adjacencies ON keyword_adjacencies.doc_id = documents.id 
        WHERE key1_id = %d OR key2_id = %d""" % (keyword_id, keyword_id)

        session = Session(bind = self.engine)
        ret = set([])
        for _id, in session.execute(sql):
            ret.add(_id)

        session.close()
        return keyword_id, ret 

get_keyword_docs = registry.tasks[ProdKeywordDocsResolve.name]

class ProdDocLinksSummary(DatabaseTask):

    def run(self, doc_id):

        sql = """SELECT domains.`key`, COUNT(*) FROM links_absolute
            JOIN domains ON links_absolute.domain_id = domains.id 
            WHERE links_absolute.document_id = %d
            GROUP BY (domains.id)""" % (doc_id,)

        ret = {}
        session = Session(bind=self.engine)
        for key, count in session.execute(sql):
            ret[key] = count 

        logging.info("Fetching domain key for %d", doc_id)
        sql = """SELECT domains.`key` FROM domains 
        JOIN articles ON articles.domain_id = domains.id
        JOIN documents ON documents.article_id = articles.id
        WHERE documents.id = %d""" % (doc_id,)
        for domain, in session.execute(sql):
            pass 

        sql = """SELECT COUNT(*) FROM links_relative WHERE document_id = %d"""
        for count, in session.execute(sql % (doc_id,)):
            pass 

        assert domain != None 
        assert count  != None 

        if count > 0:
            if domain not in ret:
                ret[domain] = 0

            ret[domain] += count 

        session.close()
        return (doc_id, ret, domain)

get_document_links = registry.tasks[ProdDocLinksSummary.name]

class ProdDocPublished(DatabaseTask):

    def run(self, document_id):
        session = Session(bind = self.engine)
        
        # Certain date resolution
        sql = """SELECT certain_dates.date FROM certain_dates
        WHERE doc_id = %d""" % (document_id, ) 
        print sql 
        for date, in session.execute(sql):
            return document_id, "Certain", date 

        # Uncertain date resolution
        sql = """SELECT uncertain_dates.date FROM uncertain_dates 
        WHERE doc_id = %d""" % (document_id, )
        print sql 
        for date, in session.execute(sql):
            return document_id, "Uncertain", date 

        sql = """SELECT articles.crawled FROM articles 
            JOIN documents ON documents.article_id = articles.id 
            WHERE documents.id = %d""" % (document_id,)
        print sql
        for date, in session.execute(sql):
            return document_id, "Crawled", date 

        raise AssertionError("Shouldn't be here!")

get_document_date = registry.tasks[ProdDocPublished.name]

class PhraseRelevanceFromKeywordDocId(DatabaseTask):

    def run(self, doc_id, keyword_identifiers):
        session = Session(bind = self.engine)
        ret = None 

        sql = """SELECT COUNT(*) FROM documents 
            JOIN sentences ON sentences.document = documents.doc_id 
            JOIN phrases ON phrases.sentence = sentences.id 
            JOIN keyword_incidences ON keyword_incidences.phrase_id = phrases.id 
            WHERE keyword_incidences.keyword_id IN (%s)
            AND documents.id = %d""" % (','.join([str(i) for i in keyword_identifiers]), doc_id)

        for count, in sql:
            ret = count 

        session.close()
        return (doc_id, ret)

get_phrase_relevance = registry.tasks[PhraseRelevanceFromKeywordDocId.name]

class PhraseMatchFromKeyword(DatabaseTask):

    def run(self, keyword_id):
        session = Session(bind=self.engine)
        ret = set([])
        it  = session.query(KeywordIncidence).filter_by(keyword_id = keyword_id)
        for thing in it:
            ret.add(thing.phrase_id)

        session.close()
        return ret 

class GetPhrasesFromDocID(DatabaseTask):

    def run(self, document_id):
        session = Session(bind=self.engine)
        ret     = set([])
        doc     = session.query(Document).get(document_id)
        
        for sentence in doc.sentences:
            for phrase in sentence.phrases:
                ret.add(phrase)

        session.close()
        return ret 

