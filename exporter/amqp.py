from kombu import Queue, Exchange, Consumer, Connection
from kombu.mixins import ConsumerMixin
from concurrent.futures import ThreadPoolExecutor

from typing import List, Callable

from dataclasses import dataclass


@dataclass
class QueueConfig:
    name: str
    routing_key: str
    exchange: str
    exchange_type: str

@dataclass
class ConsumerConfig:
    handler: Callable
    queues: List[QueueConfig]
    tag: str


@dataclass
class AmqpConnConfig:
    host: str
    port: int

    def broker_url(self):
        return f'amqp://{self.host}:{str(self.port)}'


class _ConsumerMixin(ConsumerMixin):

    def __init__(self, connection: Connection, consumer_configs: List[ConsumerConfig]):
        self.connection = connection
        self.consumer_configs = consumer_configs

    def get_consumers(self, _, channel):
        return [Consumer(channel.connection.channel(),
                         queues=[_ConsumerMixin.queue_from_config(q) for q in consumer_config.queues],
                         callbacks=[consumer_config.handler],
                         tag_prefix=consumer_config.tag)
                for consumer_config in self.consumer_configs]



    @staticmethod
    def queue_from_config(queue_config: QueueConfig) -> Queue:
        exchange = Exchange(queue_config.exchange, queue_config.exchange_type)
        return Queue(queue_config.name, exchange, queue_config.routing_key)


class AsyncListener:
    """
    Delegates message handlers to a thread pool, so that long-running
    tasks don't block heartbeats.

    Maintains its own AMQP connections given AMQP connection config
    """

    def __init__(self, amqp_conn_config: AmqpConnConfig):
        self.amqp_conn_config = amqp_conn_config
        self._consumer_configs: List[ConsumerConfig] = list()

    def add_consumer(self, consumer_config: ConsumerConfig):
        """
        The handler function in the provided ConsumerConfig should take a
        :param consumer_config:
        :return:
        """
        self._consumer_configs.append(consumer_config)

    @staticmethod
    def async_on_message(on_message_fn: Callable, executor: ThreadPoolExecutor) -> Callable:
        """
        Returns an async version of a kombu on_message function
        :param on_message_fn:
        :param executor:
        :return: a function that, when called, executes the on_message_fn asynchronously
        """
        def async_fn(body, msg):
            return executor.submit(lambda: on_message_fn(body, msg))

        return async_fn

    def run(self):
        with ThreadPoolExecutor() as executor:
            async_consumer_configs = [ConsumerConfig(AsyncListener.async_on_message(c.handler, executor),
                                                     c.queues,
                                                     c.tag)
                                      for c in self._consumer_configs]
            with Connection(self.amqp_conn_config.broker_url()) as conn:
                consumer_mixin = _ConsumerMixin(conn, async_consumer_configs)
                consumer_mixin.run()
