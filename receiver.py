import datetime
import json
import logging
import os

from ingest.exporter.ingestexportservice import IngestExporter

DEFAULT_RABBIT_URL=os.path.expandvars(os.environ.get('RABBIT_URL', 'amqp://localhost:5672'))
DEFAULT_QUEUE_NAME=os.environ.get('SUBMISSION_QUEUE_NAME', 'ingest.envelope.submitted.queue')
LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

EXCHANGE = 'ingest.bundle.exchange'



class IngestReceiver:

    def __init__(self):
        self.logger = LOGGER

    def run(self, message):
        self.logger.info('message received ' + json.dumps(message))
        self.logger.info('process received ' + message["callbackLink"])
        self.logger.info('process index: ' + str(message["index"]) + ', total processes: ' + str(message["total"]))

        ingest_exporter = IngestExporter()
        version_timestamp = datetime.datetime.strptime(message["versionTimestamp"], "%Y-%m-%dT%H:%M:%S.%fZ")
        bundle_version = version_timestamp.strftime("%Y-%m-%dT%H%M%S.%fZ")

        ingest_exporter.export_bundle(bundle_uuid=message["bundleUuid"],
                                      bundle_version=bundle_version,
                                      submission_uuid=message["envelopeUuid"],
                                      process_uuid=message["documentUuid"])
