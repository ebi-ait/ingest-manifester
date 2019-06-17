#!/usr/bin/env python
import os
import sys
import logging

from ingest.api.dssapi import DssApi
from ingest.api.ingestapi import IngestApi
from ingest.api.stagingapi import StagingApi
from ingest.exporter.bundle import BundleService
from ingest.exporter.exporter import Exporter
from ingest.exporter.metadata import MetadataService
from ingest.exporter.staging import StagingService
from ingest.utils.s2s_token_client import S2STokenClient
from ingest.utils.token_manager import TokenManager
from kombu import Connection, Exchange, Queue
from multiprocessing.dummy import Process

from receiver import CreateBundleReceiver, UpdateBundleReceiver

DISABLE_BUNDLE_CREATE = os.environ.get('DISABLE_BUNDLE_CREATE', False)
DISABLE_BUNDLE_UPDATE = os.environ.get('DISABLE_BUNDLE_UPDATE', False)

DEFAULT_RABBIT_URL = os.path.expandvars(
    os.environ.get('RABBIT_URL', 'amqp://localhost:5672'))

EXCHANGE = 'ingest.bundle.exchange'
EXCHANGE_TYPE = 'topic'

ASSAY_QUEUE = 'ingest.bundle.assay.create'
ANALYSIS_QUEUE = 'ingest.bundle.analysis.create'

ASSAY_ROUTING_KEY = 'ingest.bundle.assay.submitted'
ANALYSIS_ROUTING_KEY = 'ingest.bundle.analysis.submitted'

BUNDLE_UPDATE_QUEUE = 'ingest.bundle.update.submitted'
BUNDLE_UPDATE_ROUTING_KEY = 'ingest.bundle.update.submitted'

ASSAY_COMPLETED_ROUTING_KEY = 'ingest.bundle.assay.completed'

RETRY_POLICY = {
    'interval_start': 0,
    'interval_step': 2,
    'interval_max': 30,
    'max_retries': 60
}

if __name__ == '__main__':
    logging.getLogger('receiver').setLevel(logging.INFO)
    logging.getLogger('ingest').setLevel(logging.INFO)
    logging.getLogger('ingest.api.dssapi').setLevel(logging.DEBUG)

    format = ' %(asctime)s  - %(name)s - %(levelname)s in %(filename)s:' \
             '%(lineno)s %(funcName)s(): %(message)s'
    logging.basicConfig(stream=sys.stdout, level=logging.WARNING,
                        format=format)

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
        create_bundle_receiver = CreateBundleReceiver(conn, bundle_queues,
                                                      publish_config=conf)

    with Connection(DEFAULT_RABBIT_URL) as conn:
        bundle_exchange = Exchange(EXCHANGE, type=EXCHANGE_TYPE)
        bundle_queues = [
            Queue(BUNDLE_UPDATE_QUEUE, bundle_exchange,
                  routing_key=BUNDLE_UPDATE_ROUTING_KEY)]

        upload_client = StagingApi()
        dss_client = DssApi()

        s2s_token_client = S2STokenClient()
        gcp_credentials_file = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
        s2s_token_client.setup_from_file(gcp_credentials_file)
        token_manager = TokenManager(token_client=s2s_token_client)
        ingest_client = IngestApi(token_manager=token_manager)

        metadata_service = MetadataService(ingest_client=ingest_client)
        bundle_service = BundleService(dss_client=dss_client)
        staging_service = StagingService(staging_client=upload_client)

        exporter = Exporter(ingest_api=ingest_client, metadata_service=metadata_service,
                            bundle_service=bundle_service, staging_service=staging_service)

        conf = {
            'exchange': EXCHANGE,
            'routing_key': ASSAY_COMPLETED_ROUTING_KEY,
            'retry': True,
            'retry_policy': RETRY_POLICY
        }
        update_bundle_receiver = UpdateBundleReceiver(connection=conn,
                                                      queues=bundle_queues,
                                                      exporter=exporter,
                                                      ingest_client=ingest_client,
                                                      publish_config=conf)
    if not DISABLE_BUNDLE_CREATE:
        create_process = Process(target=create_bundle_receiver.run)
        create_process.start()

    if not DISABLE_BUNDLE_UPDATE:
        update_process = Process(target=update_bundle_receiver.run)
        update_process.start()
