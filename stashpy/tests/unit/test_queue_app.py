import unittest
from unittest import mock
from types import SimpleNamespace
from stashpy.main import RabbitApp, MessageConsumer
import stashpy.handler

class ConsumerStub:
    def __init__(self, queues, callbacks):
        self.queues = queues
        self.callbacks = callbacks

class MessageStub(SimpleNamespace):

    def ack(self):
        self.acked = True

class QueueAppTests(unittest.TestCase):

    @mock.patch('stashpy.main.MessageConsumer')
    @mock.patch('kombu.Connection')
    def test_run(self, mock_connection, mock_consumer):
        config = {'queue_config': {'url': 'amqp://thequeueurl',
                                   'queuename': 'aq',
                                   'exchange': 'logging'}}
        app = RabbitApp(None, config)
        self.assertEqual(mock_consumer.call_count, 1)
        _, args, _ = mock_consumer.mock_calls[0]
        self.assertEqual(args[0], mock_connection.return_value)
        self.assertIsNone(args[1])
        self.assertIsInstance(args[2], stashpy.handler.NullIndexer)
        self.assertEqual(args[3], 'aq')
        self.assertEqual(args[4], 'logging')


    @mock.patch('stashpy.main.kombu')
    def test_consumer(self, mock_kombu):
        consumer = MessageConsumer(mock.MagicMock(), None, None, 'aq', 'logging')
        self.assertEqual(mock_kombu.Queue.call_count, 1)
        self.assertEqual(mock_kombu.Exchange.call_count, 1)
        mock_kombu.Exchange.assert_called_once_with('logging')
        mock_kombu.Queue.assert_called_once_with(
            'aq', mock_kombu.Exchange.return_value, '')
        consumers = consumer.get_consumers(ConsumerStub, None)
        self.assertEqual(len(consumers), 1)
        self.assertTrue(consumers[0].queues[0] is consumer.task_queue)
        # is check does not work here. confusion.
        self.assertEqual(consumers[0].callbacks[0], consumer.on_task)


    @mock.patch('stashpy.main.kombu')
    def test_consumer_on_task(self, mock_kombu):
        indexer = mock.MagicMock()
        line_processor = mock.MagicMock()
        consumer = MessageConsumer(mock.MagicMock(), line_processor,
                                   indexer, 'aq', 'logging')
        message = MessageStub(payload='I am a string, yo')
        consumer.on_task(None, message)
        self.assertTrue(message.acked)
        line_processor.for_line.assert_called_once_with('I am a string, yo')
        indexer.index.assert_called_once_with(line_processor.for_line.return_value)
