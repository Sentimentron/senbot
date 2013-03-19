#!/usr/bin/env python

from kombu import Queue

CELERY_ROUTES = {'lookup.ProdWhiteSpaceKWExpand': {'queue': 'lookup'}}
