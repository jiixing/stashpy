import unittest
from datetime import datetime, timedelta
import json
import copy

import stashpy
from stashpy.indexer import TornadoIndexer
from .common import TimeStampedMixin

class IndexerTests(unittest.TestCase, TimeStampedMixin):

    def request_body(self, request):
        return json.loads(request.body.decode('utf-8'))

    def test_simple_indexing(self):
        indexer = TornadoIndexer('localhost', 9200)
        doc = {'name':'Lilith', 'age': 4}
        request = indexer._create_request(copy.copy(doc))
        url_prefix = datetime.strftime(
            datetime.now(),
            'http://localhost:9200/stashpy-%Y-%m-%d/doc/'
        )
        self.assertTrue(request.url.startswith(url_prefix))
        self.assertDictEqualWithTimestamp(self.request_body(request), doc)

    def test_skip_timestamp(self):
        indexer = TornadoIndexer('localhost', 9200)
        doc = {'name':'Lilith', 'age': 4, '@timestamp': 'whatever'}
        request = indexer._create_request(copy.copy(doc))
        url_prefix = datetime.strftime(
            datetime.now(),
            'http://localhost:9200/stashpy-%Y-%m-%d/doc/'
        )
        self.assertTrue(request.url.startswith(url_prefix))
        self.assertDictEqual(self.request_body(request), doc)

    def test_index_pattern(self):
        indexer = TornadoIndexer('localhost', 9200,
                                    index_pattern='kita-{name}-%Y')
        doc = {'name':'Lilith', 'age': 4}
        request = indexer._create_request(copy.copy(doc))
        url_prefix = datetime.strftime(
            datetime.now(),
            'http://localhost:9200/kita-Lilith-%Y/doc/'
        )
        self.assertTrue(request.url.startswith(url_prefix))
        self.assertDictEqualWithTimestamp(self.request_body(request), doc)


    def test_index_pattern_in_doc(self):
        indexer = TornadoIndexer('localhost', 9200)
        doc = {'name':'Lilith', 'age': 4, '_index_':'Kita-{name}-%Y'}
        request = indexer._create_request(copy.copy(doc))
        url_prefix = datetime.strftime(
            datetime.now(),
            'http://localhost:9200/Kita-Lilith-%Y/doc/'
        )
        self.assertTrue(request.url.startswith(url_prefix))
        doc.pop('_index_')
        self.assertDictEqualWithTimestamp(self.request_body(request), doc)

    def test_index_pattern_in_doc_priority(self):
        indexer = TornadoIndexer('localhost', 9200, index_pattern='blah-%Y')
        doc = {'name':'Lilith', 'age': 4, '_index_':'Kita-{name}-%Y'}
        request = indexer._create_request(copy.copy(doc))
        url_prefix = datetime.strftime(
            datetime.now(),
            'http://localhost:9200/Kita-Lilith-%Y/doc/'
        )
        self.assertTrue(request.url.startswith(url_prefix))
        doc.pop('_index_')
        self.assertDictEqualWithTimestamp(self.request_body(request), doc)

    def test_index_pattern_not_date(self):
        now = datetime.now()
        doc = {'name':'Lilith',
               'age': 4,
               '_index_':datetime.strftime(now, 'Kita-{name}-%Y')}
        indexer = TornadoIndexer('localhost', 9200)
        request = indexer._create_request(copy.copy(doc))
        url_prefix = datetime.strftime(
            datetime.now(),
            'http://localhost:9200/Kita-Lilith-%Y/doc/'
        )
        self.assertTrue(request.url.startswith(url_prefix))
        doc.pop('_index_')
        self.assertDictEqualWithTimestamp(self.request_body(request), doc)

    def test_index_pattern_different_doc_type(self):
        #what year is it.jpg
        now = datetime.now()
        doc = {'name':'Lilith',
               'age': 4,
               '_index_':datetime.strftime(now, 'Kita-{name}-%Y')}
        indexer = TornadoIndexer('localhost', 9200, doc_type='alternative')
        request = indexer._create_request(copy.copy(doc))
        url_prefix = datetime.strftime(
            now,
            'http://localhost:9200/Kita-Lilith-%Y/alternative/'
        )
        self.assertTrue(request.url.startswith(url_prefix))
        doc.pop('_index_')
        self.assertDictEqualWithTimestamp(self.request_body(request), doc)
