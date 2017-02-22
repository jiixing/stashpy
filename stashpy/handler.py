import importlib
import logging

from tornado import gen
import tornado.tcpserver

from .indexer import ESIndexer
from .processor import LineProcessor

logger = logging.getLogger(__name__)

class RotatingCounter:
    def __init__(self, maximum, log_message, logger_arg=None):
        self.maximum = maximum
        self.log_message = log_message
        self.logger = logger_arg or logger
        self.current = 0

    def inc(self):
        self.current += 1
        if self.current >= self.maximum:
            self.log()
            self.current = 0

    def log(self):
        self.logger.info(self.log_message, self.maximum)


class ConnectionHandler:

    def __init__(self, stream, address, indexer, line_processor, heartbeat_count=10):
        self.stream = stream
        self.address = address
        self.indexer = indexer
        self.line_processor = line_processor
        self.unparsed_counter = RotatingCounter(
            heartbeat_count,
            "Indexed %d unparsed documents")
        self.parsed_counter = RotatingCounter(
            heartbeat_count,
            "Parsed and indexed %d documents")
        self.stream.set_close_callback(self.on_close)
        logger.info("Accepted connection from {}".format(address))

    @gen.coroutine
    def on_connect(self):
        yield self.dispatch_client()

    @gen.coroutine
    def dispatch_client(self):
        try:
            while True:
                line = yield self.stream.read_until(b"\n")
                yield self.indexer.index(self.line_processor.for_line(line))
        except tornado.iostream.StreamClosedError:
            pass

    @gen.coroutine
    def on_close(self):
        #Close es connection?
        logger.info("Connection to %s closed", self.address)
        yield []

class NullIndexer:
    def index(self, doc):
        pass

DEFAULT_HEARTBEAT_COUNT = 200

class MainHandler(tornado.tcpserver.TCPServer):


    def __init__(self, config):
        self.config = config
        self.es_config = config.get('indexer_config')
        self.processor_spec = config.get('processor_spec')
        self.processor_class = config.get('processor_class')
        assert self.processor_spec is not None or self.processor_class is not None
        super().__init__()

    def load_processor(self):
        if self.processor_spec:
            line_processor = LineProcessor(self.processor_spec)
        else:
            module_name,class_name = self.processor_class.rsplit('.', 1)
            module = importlib.import_module(module_name)
            _class = getattr(module, class_name)
            line_processor = _class()
        return line_processor

    @gen.coroutine
    def handle_stream(self, stream, address):
        if self.es_config is None:
            indexer = NullIndexer()
        else:
            indexer = ESIndexer(**self.es_config)
        cn = ConnectionHandler(stream, address,
                               indexer,
                               self.load_processor(),
                               heartbeat_count=self.config.get('heartbeat_count',
                                                               DEFAULT_HEARTBEAT_COUNT))
        yield cn.on_connect()
