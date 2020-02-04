#!/usr/bin/env python
import logging
import os
import sys
from multiprocessing.dummy import Process

from ingest.api.ingestapi import IngestApi
from kombu import Connection, Exchange, Queue

from manifest.exporter import ManifestExporter
from manifest.receiver import ManifestReceiver

DISABLE_MANIFEST = os.environ.get('DISABLE_MANIFEST', False)

DEFAULT_RABBIT_URL = os.path.expandvars(
    os.environ.get('RABBIT_URL', 'amqp://localhost:5672'))

EXCHANGE = 'ingest.bundle.exchange'
EXCHANGE_TYPE = 'topic'

ASSAY_QUEUE = 'ingest.bundle.assay.create'
ANALYSIS_QUEUE = 'ingest.bundle.analysis.create'

ASSAY_ROUTING_KEY = 'ingest.bundle.assay.submitted'
ANALYSIS_ROUTING_KEY = 'ingest.bundle.analysis.submitted'

ASSAY_COMPLETED_ROUTING_KEY = 'ingest.bundle.assay.completed'

RETRY_POLICY = {
    'interval_start': 0,
    'interval_step': 2,
    'interval_max': 30,
    'max_retries': 60
}


def setup_manifest_receiver():
    ingest_client = IngestApi()

    with Connection(DEFAULT_RABBIT_URL) as conn:
        bundle_exchange = Exchange(EXCHANGE, type=EXCHANGE_TYPE)
        bundle_queues = [
            Queue(ASSAY_QUEUE, bundle_exchange,
                  routing_key=ASSAY_ROUTING_KEY),
            Queue(ANALYSIS_QUEUE, bundle_exchange,
                  routing_key=ANALYSIS_ROUTING_KEY)
        ]

        conf = {
            'exchange': EXCHANGE,
            'routing_key': ASSAY_COMPLETED_ROUTING_KEY,
            'retry': True,
            'retry_policy': RETRY_POLICY
        }

        exporter = ManifestExporter(ingest_api=ingest_client)
        manifest_receiver = ManifestReceiver(conn, bundle_queues, exporter=exporter, publish_config=conf)
        manifest_process = Process(target=manifest_receiver.run)
        manifest_process.start()


if __name__ == '__main__':
    logging.getLogger('receiver').setLevel(logging.INFO)
    logging.getLogger('ingest').setLevel(logging.INFO)
    logging.getLogger('ingest.api.dssapi').setLevel(logging.DEBUG)

    format = ' %(asctime)s  - %(name)s - %(levelname)s in %(filename)s:' \
             '%(lineno)s %(funcName)s(): %(message)s'
    logging.basicConfig(stream=sys.stdout, level=logging.WARNING,
                        format=format)

    if not DISABLE_MANIFEST:
        setup_manifest_receiver()
