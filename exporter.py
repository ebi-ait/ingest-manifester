#!/usr/bin/env python
import os
import sys
import logging

from ingest.api.dssapi import DssApi
from ingest.api.ingestapi import IngestApi
from ingest.api.stagingapi import StagingApi
# from ingest.exporter.bundle_update_service import BundleUpdateService
from kombu import Connection, Exchange, Queue
from multiprocessing import Process

from receiver import CreateBundleReceiver, UpdateBundleReceiver

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

if __name__ == '__main__':
    logging.getLogger('receiver').setLevel(logging.INFO)
    logging.getLogger('ingest').setLevel(logging.INFO)

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
            'retry_policy': {
                'interval_start': 0,
                'interval_step': 2,
                'interval_max': 30,
                'max_retries': 60
            }
        }
        create_bundle_receiver = CreateBundleReceiver(conn, bundle_queues,
                                                      publish_config=conf)

    # with Connection(DEFAULT_RABBIT_URL) as conn:
    #     bundle_exchange = Exchange(EXCHANGE, type=EXCHANGE_TYPE)
    #     bundle_queues = [
    #         Queue(BUNDLE_UPDATE_QUEUE, bundle_exchange,
    #               routing_key=BUNDLE_UPDATE_ROUTING_KEY)]
    #
    #     upload_client = StagingApi()
    #     dss_client = DssApi()
    #     ingest_client = IngestApi()
    #     bundle_update_service = BundleUpdateService(
    #             staging_client=upload_client,
    #             dss_client=dss_client,
    #             ingest_client=ingest_client)
    #
    #     update_bundle_receiver = UpdateBundleReceiver(connection=conn,
    #                                                   queues=bundle_queues,
    #                                                   bundle_update_service=bundle_update_service,
    #                                                   ingest_client=ingest_client)

    create_process = Process(target=create_bundle_receiver.run)
    create_process.start()

    # TODO disable update for now
    # update_process = Process(target=update_bundle_receiver.run)
    # update_process.start()
