#!/usr/bin/env python

from kombu import Queue

CELERY_ROUTES = {'cache.ProdWhiteSpaceKWExpand': {'queue': 'cache'}}
