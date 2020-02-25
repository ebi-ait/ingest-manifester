import json
import logging
import time

from manifest.exporter import ManifestExporter
from receiver import Receiver


class ManifestReceiver(Receiver):
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