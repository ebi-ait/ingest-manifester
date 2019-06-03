#!/usr/bin/env python
"""
This script listens on a ingest submission queue and as submission are completed will
call the ingest export service to generate the bundles and submit bundles to datastore
"""
__author__ = "jupp"
__license__ = "Apache 2.0"

from optparse import OptionParser
import os, sys, json, time
import logging
from kombu import Connection, Exchange, Queue
from kombu.mixins import ConsumerProducerMixin

from receiver import IngestReceiver

DEFAULT_RABBIT_URL = os.path.expandvars(os.environ.get('RABBIT_URL', 'amqp://localhost:5672'))
EXCHANGE = 'ingest.bundle.exchange'
EXCHANGE_TYPE = 'topic'
ASSAY_QUEUE = 'ingest.bundle.assay.create'
ANALYSIS_QUEUE = 'ingest.bundle.analysis.create'

ASSAY_ROUTING_KEY = 'ingest.bundle.assay.submitted'
ANALYSIS_ROUTING_KEY = 'ingest.bundle.analysis.submitted'
ASSAY_COMPLETED_ROUTING_KEY = 'ingest.bundle.assay.completed'


class Worker(ConsumerProducerMixin):
    def __init__(self, connection, queues):
        self.connection = connection
        self.queues = queues
        self.logger = logging.getLogger(__name__)
        self.receiver = IngestReceiver()

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=self.queues,
                         callbacks=[self.on_message])]

    def on_message(self, body, message):
        self.logger.info(f'Message received: {body}')
        self.logger.info('Ack-ing message...')
        message.ack()
        self.logger.info('Acked!')
        success = False
        start = time.clock()
        try:
            self.receiver.run(json.loads(body))
            success = True
        except Exception as e1:
            self.logger.exception(str(e1))
            self.logger.error(f"Failed to process the exporter message: {body} due to error: {str(e1)}")

        if success:
            self.logger.info(f"Notifying state tracker of completed bundle: {body}")
            self.producer.publish(
                json.loads(body),
                exchange=EXCHANGE,
                routing_key=ASSAY_COMPLETED_ROUTING_KEY,
                retry=True,
                retry_policy={
                    'interval_start': 0,
                    'interval_step': 2,
                    'interval_max': 30,
                    'max_retries': 60
                }
            )
            self.logger.info("Notified!")
            end = time.clock()
            time_to_export = end - start
            self.logger.info('Finished! ' + str(message.delivery_tag))
            self.logger.info('Export time (ms): ' + str(time_to_export * 1000))


if __name__ == '__main__':
    format = ' %(asctime)s  - %(name)s - %(levelname)s in %(filename)s:%(lineno)s %(funcName)s(): %(message)s'
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=format)

    bundle_exchange = Exchange(EXCHANGE, type=EXCHANGE_TYPE)
    bundle_queues = [Queue(ASSAY_QUEUE, bundle_exchange, routing_key=ASSAY_ROUTING_KEY),
                     Queue(ANALYSIS_QUEUE, bundle_exchange, routing_key=ANALYSIS_ROUTING_KEY)]

    with Connection(DEFAULT_RABBIT_URL, heartbeat=1200) as conn:
        worker = Worker(conn, bundle_queues)
        worker.run()
