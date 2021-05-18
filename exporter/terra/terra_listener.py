from kombu.mixins import ConsumerProducerMixin
from kombu import Connection, Consumer, Message, Queue, Exchange

from exporter.terra.terra_exporter import TerraExporter
from exporter.amqp import QueueConfig, AmqpConnConfig

from typing import Type, List, Dict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from exporter.terra.terra_export_job import TerraExportJobService

import logging
import json


class ExperimentMessageParseExpection(Exception):
    pass


@dataclass
class ExperimentMessage:
    process_id: str
    process_uuid: str
    submission_uuid: str
    experiment_index: int
    total: int
    job_id: str

    @staticmethod
    def from_dict(data: Dict) -> 'ExperimentMessage':
        try:
            return ExperimentMessage(data["documentId"],
                                     data["documentUuid"],
                                     data["envelopeUuid"],
                                     data["index"],
                                     data["total"],
                                     data["exportJobId"])
        except (KeyError, TypeError) as e:
            raise ExperimentMessageParseExpection(e)


class _TerraListener(ConsumerProducerMixin):

    def __init__(self,
                 connection: Connection,
                 terra_exporter: TerraExporter,
                 job_service: TerraExportJobService,
                 experiment_queue_config: QueueConfig,
                 publish_queue_config: QueueConfig,
                 executor: ThreadPoolExecutor):
        self.connection = connection
        self.terra_exporter = terra_exporter
        self.job_service = job_service
        self.experiment_queue_config = experiment_queue_config
        self.publish_queue_config = publish_queue_config
        self.executor = executor

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

    def get_consumers(self, _consumer: Type[Consumer], channel) -> List[Consumer]:
        experiment_consumer = _consumer([_TerraListener.queue_from_config(self.experiment_queue_config)],
                                        callbacks=[self.experiment_message_handler],
                                        prefetch_count=1)

        return [experiment_consumer]

    def experiment_message_handler(self, body: str, msg: Message):
        return self.executor.submit(lambda: self._experiment_message_handler(body, msg))

    def _experiment_message_handler(self, body: str, msg: Message):
        try:
            exp = ExperimentMessage.from_dict(json.loads(body))
            self.logger.info(f'Received experiment message for process {exp.process_uuid} (index {exp.experiment_index} for submission {exp.submission_uuid})')
            self.terra_exporter.export(exp.process_uuid, exp.submission_uuid, exp.job_id)
            self.logger.info(f'Exported experiment for process uuid {exp.process_uuid} (--index {exp.experiment_index} --total {exp.total} --submission {exp.submission_uuid})')
            self.log_complete_assay(exp.job_id, exp.process_id)

            self.producer.publish(json.loads(body),
                exchange=self.publish_queue_config.exchange,
                routing_key=self.publish_queue_config.routing_key,
                retry=self.publish_queue_config.retry,
                retry_policy=self.publish_queue_config.retry_policy)
            msg.ack()

        except Exception as e:
            self.logger.error(f'Failed to export experiment message with body: {body}')
            self.logger.exception(e)

    def log_complete_assay(self, job_id: str, assay_process_id: str):
        self.job_service.create_export_entity(job_id, assay_process_id)

    @staticmethod
    def queue_from_config(queue_config: QueueConfig) -> Queue:
        exchange = Exchange(queue_config.exchange, queue_config.exchange_type)
        return Queue(queue_config.name, exchange, queue_config.routing_key)


class TerraListener:

    def __init__(self,
                 amqp_conn_config: AmqpConnConfig,
                 terra_exporter: TerraExporter,
                 job_service: TerraExportJobService,
                 experiment_queue_config: QueueConfig,
                 publish_queue_config: QueueConfig):
        self.amqp_conn_config = amqp_conn_config
        self.terra_exporter = terra_exporter
        self.job_service = job_service
        self.experiment_queue_config = experiment_queue_config
        self.publish_queue_config = publish_queue_config

    def run(self):
        with Connection(self.amqp_conn_config.broker_url()) as conn:
            _terra_listener = _TerraListener(conn, self.terra_exporter, self.job_service, self.experiment_queue_config, self.publish_queue_config, ThreadPoolExecutor())
            _terra_listener.run()
