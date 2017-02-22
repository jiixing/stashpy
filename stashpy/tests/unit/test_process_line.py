import unittest
from datetime import datetime, timedelta
from tornado.testing import AsyncTestCase, gen_test
from tornado import gen
import pytz

import stashpy.handler
from stashpy.processor import LineProcessor, FormatSpec
from stashpy.pattern_matching import is_named_re, LineParser
from .common import TimeStampedMixin

SAMPLE_PARSE = "My name is {name} and I'm {age:d} years old."
SAMPLE_REGEXP = "My name is (?P<name>\w*) and I'm (?P<age>\d*) years old\."
SAMPLE_GROK = "My name is %{USERNAME:name} and I'm %{INT:age:int} years old\."

def just_now(point: str):
    pointtime = datetime.strptime(point, "%Y-%m-%dT%H:%M:%S.%f+00:00")
    pointtime = pointtime.replace(tzinfo=pytz.utc)
    now = datetime.utcnow().replace(tzinfo=pytz.utc)
    return (now - pointtime) < timedelta(seconds=1)

class NamedReTests(unittest.TestCase):

    def test_is_re(self):
        self.assertTrue(is_named_re(SAMPLE_REGEXP))

    def test_is_parse(self):
        self.assertFalse(is_named_re(SAMPLE_PARSE))

class LineParserTests(unittest.TestCase):

    def test_re_parser(self):
        parser = LineParser(SAMPLE_REGEXP)
        self.assertDictEqual(parser("My name is Aaron and I'm 4 years old."),
                             {'name': 'Aaron', 'age': '4'})

    def test_parse_parser(self):
        parser = LineParser(SAMPLE_PARSE)
        self.assertDictEqual(parser("My name is Aaron and I'm 4 years old."),
                             {'name': 'Aaron', 'age': 4})

    def test_grok_parser(self):
        parser = LineParser(SAMPLE_GROK)
        self.assertDictEqual(parser("My name is Aaron and I'm 4 years old."),
                             {'name': 'Aaron', 'age': 4})


class SpecTests(unittest.TestCase):

    def test_format_spec(self):
        spec = FormatSpec(LineParser(SAMPLE_PARSE),
                                   {'name_and_age': '{name}_{age}'})
        self.assertDictEqual(spec("My name is Aaron and I'm 5 years old."),
                             dict({'name_and_age': 'Aaron_5'}))

class LineProcessorTests(unittest.TestCase):

    def test_line_processor_in_base_module(self):
        """Removing the LineProcessor import from stashpy.__init__.py breaks
        custom processors."""
        from stashpy import LineProcessor

    def test_to_dict(self):
        SPEC = {'to_dict':[SAMPLE_PARSE]}
        processor = LineProcessor(SPEC)
        msg = "My name is Valerian and I'm 3 years old."
        doc = processor.for_line(msg)
        self.assertTrue(just_now(doc.pop('@timestamp')))
        self.assertDictEqual(
            doc, {'name': 'Valerian', 'age': 3, '@version': 1, 'message': msg})


    def test_no_match(self):
        SPEC = {'to_dict':[SAMPLE_PARSE]}
        processor = LineProcessor(SPEC)
        line = "I'm not talking to you"
        doc = processor.for_line(line)
        self.assertTrue('@timestamp' in doc)
        self.assertTrue(just_now(doc.pop('@timestamp')))
        self.assertDictEqual(doc, {'message': line, '@version': 1})


    def test_formatted(self):
        SPEC = {'to_format': {SAMPLE_PARSE: dict(name_and_age="{name}_{age:d}")}}
        processor = LineProcessor(SPEC)
        formatted = processor.parse_line("My name is Jacob and I'm 3 years old.")
        self.assertDictEqual(formatted, {'name_and_age':'Jacob_3'})


    def test_regexp(self):
        SPEC = {'to_dict':["My name is (?P<name>\w*) and I'm (?P<age>\d*) years old."]}
        processor = LineProcessor(SPEC)
        dicted = processor.parse_line("My name is Valerian and I'm 3 years old.")
        self.assertDictEqual(dicted, {'name': 'Valerian', 'age': '3'})


class MockStream:
    def set_close_callback(*args, **kwargs):
        pass

class MockIndexer:
    @gen.coroutine
    def index(self, doc):
        if not hasattr(self, 'indexed'):
            self.indexed = []
        self.indexed.append(doc)
        return doc


class KitaHandler(LineProcessor):

    def for_line(self, line):
        return dict(val='test')

class KitaHandlerTwo(LineProcessor):

    TO_DICT = ["My name is {name} and I'm {age:d} years old."]
    TO_FORMAT = {"Her name is {name} and she's {age:d} years old.":
                 {'name_line':"Name is {name}", 'age_line':"Age is {age}"}}


class MainHandlerTests(unittest.TestCase):

    def test_custom_handler(self):
        main = stashpy.handler.MainHandler(dict(
            es_config=dict(host='localhost', port=9200),
            processor_class='stashpy.tests.unit.test_process_line.KitaHandler'))
        processor = main.load_processor()
        self.assertIsInstance(processor, KitaHandler)
        self.assertDictEqual(processor.for_line('This is a test line'), dict(val='test'))


    def test_spec_handler(self):
        main = stashpy.handler.MainHandler(dict(
            es_config=dict(host='localhost', port=9200),
            processor_spec={'to_dict': [SAMPLE_PARSE]}))
        processor = main.load_processor()
        self.assertIsInstance(processor, LineProcessor)
        self.assertDictEqual(processor.parse_line("My name is Julia and I'm 5 years old."),
                             dict(name='Julia', age=5))


    def test_custom_handler_with_specs(self):
        main = stashpy.handler.MainHandler(dict(
            es_config=dict(host='localhost', port=9200),
            processor_class='stashpy.tests.unit.test_process_line.KitaHandlerTwo'))
        processor = main.load_processor()
        self.assertIsInstance(processor, KitaHandlerTwo)
        self.assertDictEqual(processor.parse_line("My name is Juergen and I'm 4 years old."),
                             dict(name='Juergen', age=4))

    def test_no_indexer(self):
        main = stashpy.handler.MainHandler(dict(
            es_config=None,
            processor_class='stashpy.tests.unit.test_process_line.KitaHandler'))
        processor = main.load_processor()
        self.assertDictEqual(processor.for_line("blah"), dict(val='test'))
