import datetime
import json
import logging
import time

from ingest.api.ingestapi import IngestApi
from ingest.exporter.exporter import Exporter
from ingest.exporter.ingestexportservice import IngestExporter
from kombu.mixins import ConsumerProducerMixin

from exporter.manifest_exporter import ManifestExporter


class Worker(ConsumerProducerMixin):
    def __init__(self, connection, queues):
        self.connection = connection
        self.queues = queues

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=self.queues,
                         callbacks=[self.on_message])]


class BundleReceiver(Worker):
    def notify_state_tracker(self, body_dict):
        self.producer.publish(body_dict, exchange=self.publish_config.get('exchange'),
                              routing_key=self.publish_config.get('routing_key'),
                              retry=self.publish_config.get('retry', True),
                              retry_policy=self.publish_config.get('retry_policy'))
        self.logger.info("Notified!")

    def _convert_timestamp(self, timestamp):
        version_timestamp = datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
        bundle_version = version_timestamp.strftime("%Y-%m-%dT%H%M%S.%fZ")
        return bundle_version


class CreateBundleReceiver(BundleReceiver):
    def __init__(self, connection, queues, exporter: IngestExporter, publish_config):
        self.connection = connection
        self.queues = queues
        self.logger = logging.getLogger(f'{__name__}.CreateBundleReceiver')
        self.publish_config = publish_config
        self.exporter = exporter

    def run(self):
        self.logger.info("Running CreateBundleReceiver")
        super(CreateBundleReceiver, self).run()

    def on_message(self, body, message):
        self.exporter.dss_api.init_dss_client()  # TODO workaround to fix expiration of signature when using DSS client
        self.logger.info(f'Message received: {body}')

        self.logger.info('Ack-ing message...')
        message.ack()
        self.logger.info('Acked!')

        success = False
        start = time.perf_counter()
        body_dict = json.loads(body)

        try:
            self.logger.info('process received ' + body_dict["callbackLink"])
            self.logger.info('process index: ' + str(
                body_dict["index"]) + ', total processes: ' + str(
                body_dict["total"]))

            bundle_version = self._convert_timestamp(body_dict.get('versionTimestamp'))

            self.exporter.export_bundle(bundle_uuid=body_dict["bundleUuid"],
                                        bundle_version=bundle_version,
                                        submission_uuid=body_dict["envelopeUuid"],
                                        process_uuid=body_dict["documentUuid"])
            success = True
        except Exception as e1:
            self.logger.exception(str(e1))
            self.logger.error(f"Failed to process the exporter message: {body} due to error: {str(e1)}")

        if success:
            self.logger.info(f"Notifying state tracker of completed bundle: {body}")
            self.notify_state_tracker(body_dict)
            end = time.perf_counter()
            time_to_export = end - start
            self.logger.info('Finished! ' + str(message.delivery_tag))
            self.logger.info('Export time (ms): ' + str(time_to_export))


class UpdateBundleReceiver(BundleReceiver):
    def __init__(self, connection, queues, exporter: Exporter, ingest_client: IngestApi, publish_config):
        self.connection = connection
        self.queues = queues
        self.logger = logging.getLogger(f'{__name__}.UpdateBundleReceiver')
        self.exporter = exporter
        self.ingest_client = ingest_client
        self.publish_config = publish_config

    def run(self):
        self.logger.info("Running UpdateBundleReceiver")
        super(UpdateBundleReceiver, self).run()

    def on_message(self, body, message):
        self.exporter.bundle_service.dss_client.init_dss_client()  # TODO workaround for issue
        # https://app.zenhub.com/workspaces/dcp-5ac7bcf9465cb172b77760d9/issues/humancellatlas/data-store/2216
        self.logger.info(f'Message received: {body}')

        self.logger.info('Ack-ing message...')
        message.ack()
        self.logger.info('Acked!')

        body_dict = json.loads(body)
        submission = self.ingest_client.get_submission_by_uuid(body_dict.get('envelopeUuid'))
        bundle_version = self._convert_timestamp(body_dict.get('versionTimestamp'))
        success = False
        start = time.perf_counter()

        try:
            self.exporter.export_update(submission, body_dict.get('bundleUuid'),
                                        body_dict.get('callbackLinks'), bundle_version)
            success = True
        except Exception as e1:
            self.logger.exception(str(e1))
            self.logger.error(f"Failed to process the exporter message: {body} due to error: {str(e1)}")

        if success:
            self.logger.info(f"Notifying state tracker of completed bundle: {body}")
            self.notify_state_tracker(body_dict)
            end = time.perf_counter()
            time_to_export = end - start
            self.logger.info('Finished! ' + str(message.delivery_tag))
            self.logger.info('Export time (ms): ' + str(time_to_export))


class ManifestReceiver(BundleReceiver):
    def __init__(self, connection, queues, exporter: ManifestExporter, publish_config):
        self.connection = connection
        self.queues = queues
        self.logger = logging.getLogger(f'{__name__}.ManifestReceiver')
        self.publish_config = publish_config
        self.exporter = exporter

    def run(self):
        self.logger.info("Running ManifestReceiver")
        super(ManifestReceiver, self).run()

    def on_message(self, body, message):
        self.logger.info(f'Message received: {body}')

        self.logger.info('Ack-ing message...')
        message.ack()
        self.logger.info('Acked!')

        success = False
        start = time.perf_counter()
        body_dict = json.loads(body)

        try:
            self.logger.info('process received ' + body_dict["callbackLink"])
            self.logger.info('process index: ' + str(
                body_dict["index"]) + ', total processes: ' + str(
                body_dict["total"]))

            self.exporter.export(process_uuid=body_dict["documentUuid"], submission_uuid=body_dict["envelopeUuid"])

            success = True
        except Exception as e1:
            self.logger.exception(str(e1))
            self.logger.error(f"Failed to process the exporter message: {body} due to error: {str(e1)}")

        if success:
            self.logger.info(f"Notifying state tracker of completed manifest: {body}")
            self.notify_state_tracker(body_dict)
            end = time.perf_counter()
            time_to_export = end - start
            self.logger.info('Finished! ' + str(message.delivery_tag))
            self.logger.info('Export time (ms): ' + str(time_to_export))
