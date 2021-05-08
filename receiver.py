from kombu.mixins import ConsumerProducerMixin
from kombu import Consumer
from typing import Type


class Worker(ConsumerProducerMixin):
    def __init__(self, connection, queues):
        self.connection = connection
        self.queues = queues

    def get_consumers(self, consumer: Type[Consumer], channel):
        return [consumer(queues=self.queues,
                         callbacks=[self.on_message])]


class Receiver(Worker):
    def notify_state_tracker(self, body_dict):
        self.producer.publish(body_dict, exchange=self.publish_config.get('exchange'),
                              routing_key=self.publish_config.get('routing_key'),
                              retry=self.publish_config.get('retry', True),
                              retry_policy=self.publish_config.get('retry_policy'))
        self.logger.info("Notified!")

