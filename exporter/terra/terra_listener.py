from kombu.mixins import ConsumerMixin
from kombu import Connection, Consumer, Message, Queue, Exchange

from exporter.terra.terra_exporter import TerraExporter
from exporter.amqp import QueueConfig, AmqpConnConfig

from typing import Type, List, Dict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import logging
import json


class ExperimentMessageParseExpection(Exception):
    pass


@dataclass
class ExperimentMessage:
    process_uuid: str
    submission_uuid: str
    experiment_uuid: str
    experiment_version: str

    @staticmethod
    def from_dict(data: Dict) -> 'ExperimentMessage':
        try:
            return ExperimentMessage(data["documentUuid"],
                                     data["submissionUuid"],
                                     data["bundleUuid"],
                                     data["versionTimestamp"])
        except (KeyError, TypeError) as e:
            raise ExperimentMessageParseExpection(e)

@dataclass
class SimpleUpdateMessage:
    pass


class _TerraListener(ConsumerMixin):

    def __init__(self,
                 connection: Connection,
                 terra_exporter: TerraExporter,
                 experiment_queue_config: QueueConfig,
                 update_queue_config: QueueConfig,
                 executor: ThreadPoolExecutor):
        self.connection = connection
        self.terra_exporter = terra_exporter
        self.experiment_queue_config = experiment_queue_config
        self.update_queue_config = update_queue_config
        self.executor = executor

        self.logger = logging.getLogger(__name__)

    def get_consumers(self, _consumer: Type[Consumer], channel) -> List[Consumer]:
        experiment_consumer = _consumer([_TerraListener.queue_from_config(self.experiment_queue_config)],
                                        callbacks=[self.experiment_message_handler])

        update_consumer = _consumer([_TerraListener.queue_from_config(self.update_queue_config)],
                                    callbacks=[self.update_message_handler])

        return [experiment_consumer, update_consumer]

    def experiment_message_handler(self, body: str, msg: Message):
        return self.executor.submit(lambda: self._experiment_message_handler(body, msg))

    def _experiment_message_handler(self, body: str, msg: Message):
        try:
            exp = ExperimentMessage.from_dict(json.loads(body))

            self.terra_exporter.export(exp.process_uuid, exp.submission_uuid, exp.experiment_uuid, exp.experiment_version)
            self.logger.info(f'Exported experiment for process uuid {exp.process_uuid}')

            msg.ack()
        except Exception as e:
            self.logger.error(f'Failed to export experiment message with body: {body}')
            self.logger.exception(e)

    def update_message_handler(self, body: str, msg: Message):
        pass

    def _update_message_handler(self, body: str, msg: Message):
        pass

    @staticmethod
    def queue_from_config(queue_config: QueueConfig) -> Queue:
        exchange = Exchange(queue_config.exchange, queue_config.exchange_type)
        return Queue(queue_config.name, exchange, queue_config.routing_key)


class TerraListener:

    def __init__(self,
                 amqp_conn_config: AmqpConnConfig,
                 terra_exporter: TerraExporter,
                 experiment_queue_config: QueueConfig,
                 update_queue_config: QueueConfig):
        self.amqp_conn_config = amqp_conn_config
        self.terra_exporter = terra_exporter
        self.experiment_queue_config = experiment_queue_config
        self.update_queue_config = update_queue_config

    def run(self):
        with Connection(self.amqp_conn_config.broker_url()) as conn:
            _terra_listener = _TerraListener(conn, self.terra_exporter, self.experiment_queue_config, self.update_queue_config, ThreadPoolExecutor())
            _terra_listener.run()
