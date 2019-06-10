import datetime
import json

from unittest import TestCase
from mock import MagicMock, Mock, mock, patch
from receiver import CreateBundleReceiver, UpdateBundleReceiver


class TestReceiver(TestCase):

    @patch('ingest.exporter.ingestexportservice.IngestExporter.export_bundle')
    def test_create_bundle_receiver_on_mesage(self, mock_export_bundle):
        # given
        conf = {'exchange': 'exchange', 'routing_key': 'routing_key', 'retry_policy': {}}

        create_receiver = CreateBundleReceiver(MagicMock(), MagicMock(), conf)
        body = '''{
                    "bundleUuid":"bundle-uuid",
                    "versionTimestamp":"2018-03-26T14:27:53.360Z",
                    "messageProtocol": null,
                    "documentId": "5bbc8fc8109b8300069546cf",
                    "documentUuid": "doc-uuid",
                    "callbackLink": "/processes/5bbc8fc8109b8300069546cf",
                    "documentType": "Process",
                    "envelopeId": "submission-id",
                    "envelopeUuid": "submission-uuid",
                    "index": 1278,
                    "total": 1733
                }'''
        version_timestamp = datetime.datetime.strptime(
            "2018-03-26T14:27:53.360Z", "%Y-%m-%dT%H:%M:%S.%fZ")
        bundle_version = version_timestamp.strftime("%Y-%m-%dT%H%M%S.%fZ")
        message = MagicMock(name='message')
        message.ack = Mock()
        create_receiver.notify_state_tracker = Mock()

        # when
        create_receiver.on_message(body, message)

        # then
        mock_export_bundle.assert_called_with(bundle_uuid='bundle-uuid',
                                              bundle_version=bundle_version,
                                              submission_uuid='submission-uuid',
                                              process_uuid='doc-uuid')

        create_receiver.notify_state_tracker.assert_called_with(json.loads(body))

        message.ack.assert_called_once()

    @patch('ingest.exporter.ingestexportservice.IngestExporter.export_bundle')
    def test_create_bundle_receiver_on_mesage_exception(self, mock_export_bundle):
        # given
        conf = {'exchange': 'exchange', 'routing_key': 'routing_key', 'retry_policy': {}}

        create_receiver = CreateBundleReceiver(MagicMock(), MagicMock(), conf)

        body = '''{
                    "bundleUuid":"bundle-uuid",
                    "versionTimestamp":"2018-03-26T14:27:53.360Z",
                    "messageProtocol": null,
                    "documentId": "5bbc8fc8109b8300069546cf",
                    "documentUuid": "doc-uuid",
                    "callbackLink": "/processes/5bbc8fc8109b8300069546cf",
                    "documentType": "Process",
                    "envelopeId": "submission-id",
                    "envelopeUuid": "submission-uuid",
                    "index": 1278,
                    "total": 1733
                }'''

        version_timestamp = datetime.datetime.strptime(
            "2018-03-26T14:27:53.360Z", "%Y-%m-%dT%H:%M:%S.%fZ")
        bundle_version = version_timestamp.strftime("%Y-%m-%dT%H%M%S.%fZ")

        message = MagicMock(name='message')
        message.ack = Mock()
        create_receiver.notify_state_tracker = Mock()

        # when
        mock_export_bundle.side_effect = Exception('unhandled')

        create_receiver.on_message(body, message)

        # then
        mock_export_bundle.assert_called_with(bundle_uuid='bundle-uuid',
                                              bundle_version=bundle_version,
                                              submission_uuid='submission-uuid',
                                              process_uuid='doc-uuid')
        message.ack.assert_called_once()
        create_receiver.notify_state_tracker.assert_not_called()

    def test_update_bundle_receiver_on_mesage(self):
        # given
        body = '''{
                "bundleUuid": "72de15ec-93fa-4421-b01e-59a4f9b32b46",
                "versionTimestamp": "2018-03-26T14:27:53.360Z",
                "callbackLinks": ["/processes/5c2dfb171603f500078b2922", "/protocols/5c2dfb171603f500078b2915", "/files/5c2dfb161603f500078b28e9", "/processes/5c2dfb171603f500078b2917", "/biomaterials/5c2dfb161603f500078b28fc", "/protocols/5c2dfb171603f500078b2914", "/processes/5c2dfb171603f500078b2924", "/biomaterials/5c2dfb171603f500078b28ff", "/biomaterials/5c2dfb161603f500078b28f8", "/processes/5c2dfb171603f500078b291f", "/processes/5c2dfb171603f500078b2921", "/biomaterials/5c2dfb161603f500078b28f7", "/processes/5c2dfb171603f500078b2923", "/biomaterials/5c2dfb161603f500078b28fb", "/processes/5c2dfb171603f500078b2930", "/biomaterials/5c2dfb171603f500078b2902", "/biomaterials/5c2dfb171603f500078b28fd", "/files/5c2dfb161603f500078b28e1", "/files/5c2dfb161603f500078b28e3", "/biomaterials/5c2dfb161603f500078b28fa", "/biomaterials/5c2dfb171603f500078b2905", "/processes/5c2dfb171603f500078b2920", "/biomaterials/5c2dfb171603f500078b290b", "/protocols/5c2dfb171603f500078b2912", "/files/5c2dfb161603f500078b28e7", "/biomaterials/5c2dfb171603f500078b2900", "/files/5c2dfb161603f500078b28e8", "/biomaterials/5c2dfb171603f500078b2901", "/biomaterials/5c2dfb171603f500078b2908", "/files/5c2dfb161603f500078b28e2", "/processes/5c2dfb171603f500078b292d", "/projects/5c2dfb161603f500078b28e0", "/biomaterials/5c2dfb171603f500078b290e", "/biomaterials/5c2dfb161603f500078b28f9", "/processes/5c2dfb171603f500078b291d", "/protocols/5c2dfb171603f500078b2913", "/processes/5c2dfb171603f500078b291c", "/biomaterials/5c2dfb171603f500078b28fe", "/protocols/5c2dfb171603f500078b2911", "/processes/5c2dfb171603f500078b291e", "/processes/5c2dfb171603f500078b2927", "/biomaterials/5c2dfb161603f500078b28f6", "/processes/5c2dfb171603f500078b292a"],
                "envelopeId": "5c2dfb101603f500078b28de",
                "envelopeUuid": "4e474a77-1489-42ef-b0a7-6290c2cfce29",
                "index": 0,
                "total": 12,
                "messageProtocol": null
            }'''
        version_timestamp = datetime.datetime.strptime(
                "2018-03-26T14:27:53.360Z", "%Y-%m-%dT%H:%M:%S.%fZ")
        bundle_version = version_timestamp.strftime("%Y-%m-%dT%H%M%S.%fZ")
        bundle_update_service = MagicMock('bundle_update_service')
        bundle_update_service.export_update = Mock()
        ingest_client = MagicMock('ingest_client')
        ingest_client.getSubmissionByUuid = Mock()
        update_receiver = UpdateBundleReceiver(MagicMock(), MagicMock(), bundle_update_service, ingest_client)

        message = MagicMock(name='message')
        message.ack = Mock()
        update_receiver.notify_state_tracker = Mock()

        # when
        update_receiver.on_message(body, message)

        # then
        bundle_update_service.export_update.assert_called_once()
        message.ack.assert_called_once()
        update_receiver.notify_state_tracker.assert_called_with(json.loads(body))
