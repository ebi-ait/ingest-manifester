import datetime
import json

from unittest import TestCase
from mock import MagicMock, Mock, mock, patch
from receiver import CreateBundleReceiver, UpdateBundleReceiver


class TestReceiver(TestCase):
    def setUp(self):
        self.publish_config = {
            'exchange': 'exchange',
            'routing_key': 'routing_key',
            'retry_policy': {}
        }
        self.create_message_body = '''{
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

        self.update_message_body = '''{
            "bundleUuid": "bundle-uuid",
            "versionTimestamp": "2018-03-26T14:27:53.360Z",
            "callbackLinks": ["/link1", "/link1"],
            "envelopeId": "5c2dfb101603f500078b28de",
            "envelopeUuid": "4e474a77-1489-42ef-b0a7-6290c2cfce29",
            "index": 0,
            "total": 12,
            "messageProtocol": null
        }'''

    def test_create_bundle_receiver_on_message(self):
        mock_exporter = MagicMock()
        mock_exporter.export_bundle = Mock()
        # given
        create_receiver = CreateBundleReceiver(MagicMock(), MagicMock(), mock_exporter, self.publish_config)
        version_timestamp = datetime.datetime.strptime(
            "2018-03-26T14:27:53.360Z", "%Y-%m-%dT%H:%M:%S.%fZ")
        bundle_version = version_timestamp.strftime("%Y-%m-%dT%H%M%S.%fZ")
        message = MagicMock(name='message')
        message.ack = Mock()
        create_receiver.notify_state_tracker = Mock()

        # when
        create_receiver.on_message(self.create_message_body, message)

        # then
        mock_exporter.export_bundle.assert_called_with(bundle_uuid='bundle-uuid',
                                              bundle_version=bundle_version,
                                              submission_uuid='submission-uuid',
                                              process_uuid='doc-uuid')

        create_receiver.notify_state_tracker.assert_called_with(json.loads(self.create_message_body))

        message.ack.assert_called_once()

    def test_create_bundle_receiver_on_message_exception(self):
        # given
        mock_exporter = MagicMock()
        mock_exporter.export_bundle = Mock()

        version_timestamp = datetime.datetime.strptime(
            "2018-03-26T14:27:53.360Z", "%Y-%m-%dT%H:%M:%S.%fZ")
        bundle_version = version_timestamp.strftime("%Y-%m-%dT%H%M%S.%fZ")

        message = MagicMock(name='message')
        message.ack = Mock()
        create_receiver = CreateBundleReceiver(MagicMock(), MagicMock(),
                                               mock_exporter,
                                               self.publish_config)

        create_receiver.notify_state_tracker = Mock()

        # when
        mock_exporter.export_bundle.side_effect = Exception('unhandled exception')

        create_receiver.on_message(self.create_message_body, message)

        # then
        mock_exporter.export_bundle.assert_called_with(bundle_uuid='bundle-uuid',
                                              bundle_version=bundle_version,
                                              submission_uuid='submission-uuid',
                                              process_uuid='doc-uuid')
        message.ack.assert_called_once()
        create_receiver.notify_state_tracker.assert_not_called()

    def test_update_bundle_receiver_on_message(self):
        # given

        version_timestamp = datetime.datetime.strptime(
                "2018-03-26T14:27:53.360Z", "%Y-%m-%dT%H:%M:%S.%fZ")
        bundle_version = version_timestamp.strftime("%Y-%m-%dT%H%M%S.%fZ")
        exporter = MagicMock('exporter')
        exporter.export_update = Mock()
        ingest_client = MagicMock('ingest_client')
        ingest_client.getSubmissionByUuid = Mock(return_value={'submission-uuid': 'uuid'})
        update_receiver = UpdateBundleReceiver(MagicMock(), MagicMock(), exporter, ingest_client, self.publish_config)

        message = MagicMock(name='message')
        message.ack = Mock()
        update_receiver.notify_state_tracker = Mock()

        # when
        update_receiver.on_message(self.update_message_body, message)

        # then
        exporter.export_update.assert_called_with(
                                        update_submission={'submission-uuid': 'uuid'},
                                        bundle_uuid='bundle-uuid',
                                        update_version=bundle_version,
                                        metadata_urls=["/link1", "/link1"])

        message.ack.assert_called_once()
        update_receiver.notify_state_tracker.assert_called_with(json.loads(self.update_message_body))

    def test_update_bundle_receiver_on_message_exception(self):
        # given
        version_timestamp = datetime.datetime.strptime(
                "2018-03-26T14:27:53.360Z", "%Y-%m-%dT%H:%M:%S.%fZ")
        bundle_version = version_timestamp.strftime("%Y-%m-%dT%H%M%S.%fZ")
        bundle_update_service = MagicMock('bundle_update_service')
        bundle_update_service.export_update = Mock()
        bundle_update_service.export_update.side_effect = Exception('unhandled exception')
        ingest_client = MagicMock('ingest_client')
        ingest_client.getSubmissionByUuid = Mock()
        update_receiver = UpdateBundleReceiver(MagicMock(), MagicMock(),
                                               bundle_update_service,
                                               ingest_client, self.publish_config)

        message = MagicMock(name='message')
        message.ack = Mock()
        update_receiver.notify_state_tracker = Mock()

        # when
        update_receiver.on_message(self.update_message_body, message)

        # then
        bundle_update_service.export_update.assert_called_once()
        message.ack.assert_called_once()
        update_receiver.notify_state_tracker.assert_not_called()
