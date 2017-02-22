import sys
import os
import logging
import logging.config

import tornado.ioloop
import yaml
try:
    import kombu
    import kombu.mixins
except ImportError:
    pass

from .handler import MainHandler
from stashpy import constants

logger = logging.getLogger(__name__)


class TornadoApp:
    def __init__(self, config):
        self.config = config
        self.main = MainHandler(config)

    def run(self):
        port = self.config.get('port', constants.DEFAULT_PORT)
        self.main.listen(port, address=constants.DEFAULT_ADDRESS)
        logger.info("Stashpy started, accepting connections on {}:{}".format(
            'localhost',
            port))
        io_loop = tornado.ioloop.IOLoop.current()
        if not io_loop._running:
            io_loop.start()


class MessageConsumer(kombu.mixins.ConsumerMixin):

    def __init__(self, connection, queue_name, exchange):
        self.connection = connection
        self.task_queue = kombu.Queue(queue_name,
                                      kombu.Exchange(exchange), '')

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=[self.task_queue],
                         callbacks=[self.on_task])]

    def on_task(self, body, message):
        #TODO here is where the magic happes
        logger.info('Processing new message on queue %s: body %s // payload %s',
                    self.task_queue.name,
                    body,
                    message.payload)
        message.ack()


class RabbitApp:

    def __init__(self, config):
        self.config = config
        connection = kombu.Connection(self.config['queue_config']['url'])
        self.consumer = MessageConsumer(
            connection,
            self.config['queue_config']['queuename'],
            self.config['queue_config']['exchange'])

    def run(self):
        logger.info("Stashpy started, listening to messages on {}:{}".format(
            self.config['queue_config']['url'],
            self.config['queue_config']['queuename']))
        self.consumer.run()


CONFIG_ERR_MSG = 'Either one of tcp_config or queue_config are allowed'

def read_config():
    config_path = os.path.abspath(sys.argv[1])
    with open(config_path, 'r') as config_file:
        config = yaml.load(config_file)
    return config

def run():
    config = read_config()
    assert 'processor_spec' in config or 'processor_class' in config
    #so much code for an xor
    if 'tcp_config' in config:
        assert 'queue_config' not in config, CONFIG_ERR_MSG
    elif 'queue_config' in config:
        assert 'tcp_config' not in config, CONFIG_ERR_MSG
    else:
        assert False, CONFIG_ERR_MSG
    logging.config.dictConfig(config.pop('logging', constants.DEFAULT_LOGGING))
    try:
        app = TornadoApp(config) if 'tcp_config' in config else RabbitApp(config)
        app.run()
    except:
        logging.exception('Exception: ')
        raise
