from kombu import Queue, Exchange, Consumer, Connection
from kombu.mixins import ConsumerMixin

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
