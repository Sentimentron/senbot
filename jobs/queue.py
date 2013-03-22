#!/usr/bin/env python

import logging
import boto.sqs 

SQS_REGION = "us-east-1"
QUERY_QUEUE_NAME = "query-queue"

class QueryQueue(object):

    def __init__(self, engine):

        self._messages = {} 
        self.queue_name = QUERY_QUEUE_NAME
        logging.info("Using '%s' as the queue.", self.queue_name)
        self._conn  = boto.sqs.connect_to_region(SQS_REGION)
        self._queue = self._conn.lookup(QUERY_QUEUE_NAME)
        if self._queue is None:
            logging.info("Creating '%s'...", (QUERY_QUEUE_NAME,))
            self._queue = self._conn.create_queue(QUERY_QUEUE_NAME, 120)

        logging.info("Connection established.")

    def __iter__(self):
        while 1:
            rs = self._queue.get_messages()
            for item in rs:
                iden = int(item.get_body())
                self._messages[iden] = item 
                yield iden 

    def set_completed(self, identifier):
        logging.info("Marking %d as completed...", identifier)

        msg = self._messages[identifier]
        self._queue.delete_message(msg)
        self._messages.pop(msg, None)
