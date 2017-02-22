import unittest
from unittest import mock
import stashpy.main

class InitTests(unittest.TestCase):

    @mock.patch('stashpy.main.read_config')
    def test_error_on_no_spec_config(self, mock_read_config):
        config = {'tcp_config': {'host': 'localhost', 'port': 1234}}
        mock_read_config.return_value = config
        with self.assertRaises(AssertionError):
            stashpy.main.run()

    @mock.patch('stashpy.main.read_config')
    def test_error_on_no_app_config(self, mock_read_config):
        config = {'processor_spec': {}}
        mock_read_config.return_value = config
        with self.assertRaises(AssertionError):
            stashpy.main.run()


    @mock.patch('stashpy.main.read_config')
    def test_error_on_two_app_configs(self, mock_read_config):
        config = {'processor_spec': {},
                  'tcp_config': {'host': 'localhost', 'port': 1234},
                  'queue_config': {'url': 'sthg', 'queuename':'sthg'}}
        mock_read_config.return_value = config
        with self.assertRaises(AssertionError):
            stashpy.main.run()


    @mock.patch('stashpy.main.TornadoApp')
    @mock.patch('stashpy.main.read_config')
    def test_tornado_app_on_correct_config(self, mock_read_config, mock_tornado_app):
        config = {'processor_spec': {},
                  'tcp_config': {'host': 'localhost', 'port': 1234}}
        mock_read_config.return_value = config
        stashpy.main.run()
        self.assertEqual(mock_tornado_app.call_count, 1)
        self.assertEqual(mock_tornado_app.return_value.run.call_count, 1)


    @mock.patch('stashpy.main.RabbitApp')
    @mock.patch('stashpy.main.read_config')
    def test_tornado_app_on_correct_config(self, mock_read_config, mock_rabbit_app):
        config = {'processor_spec': {},
                  'queue_config': {'url': 'sthg', 'port': 'sthg'}}
        mock_read_config.return_value = config
        stashpy.main.run()
        self.assertEqual(mock_rabbit_app.call_count, 1)
        self.assertEqual(mock_rabbit_app.return_value.run.call_count, 1)
