#!/usr/bin/env python

from kombu import Queue

CELERY_TASK_RESULT_EXPIRES = 18000
CELERY_ROUTES = {'cache.ProdWhiteSpaceKWExpand': {'queue': 'cache'}}
