from exporter.terra.terra_exporter import TerraExporter
from exporter.amqp import AsyncListener, ConsumerConfig, QueueConfig

from typing import Dict


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

    def experiment_message_handler(self, body: Dict, msg):
        process_uuid = body["documentUuid"]
        submission_uuid = body ["submissionUuid"]
        experiment_uuid = body["bundleUuid"]
        experiment_version = body["versionTimestamp"]

        self.terra_exporter.export(process_uuid, submission_uuid, experiment_uuid, experiment_version)

        msg.ack()

    def simple_update_message_handler(self, body: Dict, msg):
        pass

    def start(self):
        experiment_consumer_config = ConsumerConfig(self.experiment_message_handler, [self.experiment_queue_config])
        update_consumer_confing = ConsumerConfig(self.simple_update_message_handler, [self.update_queue_config])

        self.listener.add_consumer(experiment_consumer_config)
        self.listener.add_consumer(update_consumer_confing)

        self.listener.run()
