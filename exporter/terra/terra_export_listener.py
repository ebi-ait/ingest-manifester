from exporter.terra.terra_exporter import TerraExporter
from exporter.amqp import AsyncListener, ConsumerConfig, QueueConfig

from typing import Dict, Callable

import logging
import json


class TerraExportListener:

    def __init__(self,
                 terra_exporter: TerraExporter,
                 listener: AsyncListener,
                 experiment_queue_config: QueueConfig,
                 update_queue_config: QueueConfig):
        self.terra_exporter = terra_exporter
        self.listener = listener
        self.experiment_queue_config = experiment_queue_config
        self.update_queue_config = update_queue_config
        self.logger = logging.getLogger(__name__)

    def experiment_message_handler_fn(self) -> Callable:
        def fn(body: str, msg):
            body_dict = json.loads(body)
            process_uuid = body_dict["documentUuid"]
            submission_uuid = body_dict["submissionUuid"]
            experiment_uuid = body_dict["bundleUuid"]
            experiment_version = body_dict["versionTimestamp"]

            self.terra_exporter.export(process_uuid, submission_uuid, experiment_uuid, experiment_version)
            logging.info(f'Exported {process_uuid}')

            msg.ack()

        return fn


    @staticmethod
    def _experiment_message_handler_fn(terra_exporter: TerraExporter) -> Callable:
        def fn(body: Dict, msg):
            process_uuid = body["documentUuid"]
            submission_uuid = body["submissionUuid"]
            experiment_uuid = body["bundleUuid"]
            experiment_version = body["versionTimestamp"]

            terra_exporter.export(process_uuid, submission_uuid, experiment_uuid, experiment_version)

            msg.ack()

        return fn

    def simple_update_message_handler_fn(self) -> Callable:
        def fn(body: Dict, msg):
            msg.ack()
        return fn

    def start(self):
        experiment_consumer_config = ConsumerConfig(self.experiment_message_handler_fn(), [self.experiment_queue_config], "terra_experiment_export")
        update_consumer_config = ConsumerConfig(self.simple_update_message_handler_fn(), [self.update_queue_config], "terra_simple_update")

        self.listener.add_consumer(experiment_consumer_config)
        self.listener.add_consumer(update_consumer_config)

        self.listener.run()
