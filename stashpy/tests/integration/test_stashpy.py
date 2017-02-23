"""You need a running ElasticSearch instance bound to localhost:9200
for these tests"""
import socket
import json
import os
from urllib.parse import urlencode
import unittest
from unittest import mock
import copy

from tornado.testing import AsyncTestCase, gen_test
from tornado.iostream import IOStream
from tornado.httpclient import AsyncHTTPClient
import tornado.gen
import kombu
import requests

import stashpy.main

TORNADO_CONFIG = {
    'processor_spec': {'to_dict': ["My name is {name} and I'm {age:d} years old."]},
    'tcp_config': {'port': 8888,
                   'address': 'localhost'},
    'indexer_config': {'host': 'localhost',
                       'port': 9200,
                       'index_pattern': 'kita-indexer'
    }
}

QUEUE_CONFIG = copy.deepcopy(TORNADO_CONFIG)
QUEUE_CONFIG.pop('tcp_config')
QUEUE_CONFIG['queue_config'] = {
    'url': 'amqp://guest:guest@localhost:5672//',
    'queuename': 'logging',
    'exchange': 'logging'
}

SEARCH_URL = "http://localhost:9200/{}/_search?q=*:*".format(TORNADO_CONFIG['indexer_config']['index_pattern'])

#depending on the system, it takes a while for ES to create the index
#when the first document is indexed, and make it available for
#search. This timeout specifies how long the IOLoop should run before
#erroring with time out.
MAX_TIMEOUT = 8

def decode(resp):
    return json.loads(resp.body.decode('utf-8'))

class FindIn:
    def __init__(self, docs):
        if hasattr(docs, 'body'):
            docs = decode(docs)
        try:
            self.docs = docs['hits']['hits']
        except KeyError:
            self.docs = docs

    def by(self, **specs):
        sentinel = object()
        return [doc for doc in self.docs
                if all(doc['_source'].get(key, sentinel) == val for key,val in specs.items())]

class StashpyTornadoTests(AsyncTestCase):

    @gen_test(timeout=MAX_TIMEOUT)
    def test_indexing_line(self):
        client = AsyncHTTPClient(io_loop=self.io_loop)
        ping = yield client.fetch("http://localhost:9200/", raise_error=False)
        if ping.code != 200 or decode(ping)['tagline'] != "You Know, for Search":
            self.fail("This test requires an ES instance running on localhost")

        #delete if existing
        url = "http://localhost:9200/{}/".format(
            TORNADO_CONFIG['indexer_config']['index_pattern'])
        resp = yield client.fetch(url, method='DELETE', headers=None, raise_error=False)

        app = stashpy.main.TornadoApp(stashpy.main.load_processor(TORNADO_CONFIG), TORNADO_CONFIG)
        app.run()

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        stream = IOStream(s)
        yield stream.connect(("localhost", 8888))
        yield stream.write(b"My name is Yuri and I'm 6 years old.\n")

        yield tornado.gen.sleep(MAX_TIMEOUT-2)
        resp = yield client.fetch(SEARCH_URL, method='GET',
                                  headers=None, raise_error=False)
        resp_hits = json.loads(resp.body.decode('utf-8'))['hits']['hits']
        self.assertEqual(len(FindIn(resp).by(name='Yuri')), 1)
        doc = resp_hits[0]['_source']
        self.assertEqual(doc['@version'], 1)
        self.assertEqual(doc['message'], "My name is Yuri and I'm 6 years old.")


class ReplacementMessageConsumer(stashpy.main.MessageConsumer):

    def on_task(self, body, message):
        super().on_task(body, message)
        self.should_stop = True

class StashpyQueueAppTests(unittest.TestCase):

    def queue_message(self, line):
        connection = kombu.Connection('amqp://guest:guest@localhost:5672//')
        channel = connection.channel()
        exchange = kombu.Exchange(QUEUE_CONFIG['queue_config']['exchange'],
                                  'direct', durable=True, channel=channel)
        queue = kombu.Queue(QUEUE_CONFIG['queue_config']['queuename'],
                            exchange=exchange, routing_key='', channel=channel)
        exchange.declare()
        queue.declare()
        with kombu.Producer(connection) as producer:
            producer.publish(line, exchange=exchange, routing_key='')
        connection.close()

    def delete_existing(self):
        url = "http://localhost:9200/{}/".format(
            QUEUE_CONFIG['indexer_config']['index_pattern'])
        resp = requests.delete(url)
        assert resp.status_code in [404, 200]

    @mock.patch('stashpy.main.MessageConsumer', new=ReplacementMessageConsumer)
    def test_indexing_line(self):
        self.delete_existing()
        line = "My name is Pieter Hintjens and I'm 36 years old."
        self.queue_message(line)
        app = stashpy.main.RabbitApp(stashpy.main.load_processor(QUEUE_CONFIG), QUEUE_CONFIG)
        app.run()
        import time
        time.sleep(5)
        response = requests.get(SEARCH_URL)
        self.assertEqual(response.status_code, 200)
        docs = response.json()
        hintjens_list = FindIn(docs).by(name='Pieter Hintjens')
        self.assertEqual(len(hintjens_list), 1)
        hintjens = hintjens_list[0]['_source']
        self.assertEqual(hintjens['@version'], 1)
        self.assertEqual(hintjens['message'], "My name is Pieter Hintjens and I'm 36 years old.")
