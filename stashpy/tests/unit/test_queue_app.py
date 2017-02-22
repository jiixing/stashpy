import unittest
from unittest import mock
from stashpy.main import RabbitApp

class QueueAppTests(unittest.TestCase):

    @mock.patch('kombu.Connection')
    def test_run(self, mock_connection_class):
        app = RabbitApp({'queue_config': {'url': 'amqp://thequeueurl', 'queuename': 'aq'}})
        app.run()
        self.assertEqual(mock_connection_class.call_count, 1)
        mock_connection_class.assert_called_once_with('amqp://thequeueurl')
        self.assertEqual(mock_connection_class.return_value.__enter__.call_count, 1)
        mock_connection = mock_connection_class.return_value.__enter__.return_value
        self.assertEqual(mock_connection.SimpleQueue.call_count, 1)
