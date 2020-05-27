from unittest import TestCase

from mock import Mock

from exporter.metadata import MetadataResource, MetadataService, MetadataParseException, MetadataProvenance, DataFile


class MetadataResourceTest(TestCase):

    def test_provenance_from_dict(self):
        # given:
        uuid_value = '3f3212da-d5d0-4e55-b31d-83243fa02e0d'
        data = {
            'uuid': {'uuid': uuid_value},
            'submissionDate': 'a submission date',
            'updateDate': 'an update date',
            'dcpVersion': '2019-12-02T13:40:50.520Z',
            'content': {
                'describedBy': 'https://some-schema/1.2.3'
            }
        }

        # when:
        metadata_provenance = MetadataResource.provenance_from_dict(data)

        # then:
        self.assertIsNotNone(metadata_provenance)
        self.assertEqual(uuid_value, metadata_provenance.document_id)
        self.assertEqual('a submission date', metadata_provenance.submission_date)
        self.assertEqual('an update date', metadata_provenance.update_date)

    def test_provenance_from_dict_fail_fast(self):
        # given:
        uuid_value = '3f3212da-d5d0-4e55-b31d-83243fa02e0d'
        data = {'uuid': uuid_value,  # unexpected structure structure
                'submissionDate': 'a submission date',
                'updateDate': 'an update date'}

        # then:
        with self.assertRaises(MetadataParseException):
            # when
            MetadataResource.provenance_from_dict(data)

    def test_from_dict(self):
        # given:
        uuid_value = '3f3212da-d5d0-4e55-b31d-83243fa02e0d'
        data = self._create_test_data(uuid_value)

        # when:
        metadata = MetadataResource.from_dict(data)

        # then:
        self.assertIsNotNone(metadata)
        self.assertEqual('biomaterial', metadata.metadata_type)
        self.assertEqual(data['content'], metadata.metadata_json)
        self.assertEqual(data['dcpVersion'], metadata.dcp_version)

        # and:
        self.assertEqual(uuid_value, metadata.uuid)

    def test_from_dict_fail_fast_with_missing_info(self):
        # given:
        data = {}
        # then:
        with self.assertRaises(MetadataParseException):
            # when
            MetadataResource.from_dict(data)

    @staticmethod
    def _create_test_data(uuid_value):
        return {'type': 'Biomaterial',
                'uuid': {'uuid': uuid_value},
                'content': {'describedBy': "http://some-schema/1.2.3",
                            'some': {'content': ['we', 'are', 'agnostic', 'of']}},
                'dcpVersion': '6.9.1',
                'submissionDate': 'a date',
                'updateDate': 'another date'}


class MetadataServiceTest(TestCase):

    def test_fetch_resource(self):
        # given:
        ingest_client = Mock(name='ingest_client')
        uuid = '301636f7-f97b-4379-bf77-c5dcd9f17bcb'
        raw_metadata = {'type': 'Biomaterial',
                        'uuid': {'uuid': uuid},
                        'content': {'describedBy': "http://some-schema/1.2.3",
                                    'some': {'content': ['we', 'are', 'agnostic', 'of']}},
                        'dcpVersion': '2019-12-02T13:40:50.520Z',
                        'submissionDate': 'a submission date',
                        'updateDate': 'an update date'
                        }
        ingest_client.get_entity_by_callback_link = Mock(return_value=raw_metadata)

        # and:
        metadata_service = MetadataService(ingest_client)

        # when:
        metadata_resource = metadata_service.fetch_resource(
            'hca.domain.com/api/cellsuspensions/301636f7-f97b-4379-bf77-c5dcd9f17bcb')

        # then:
        self.assertEqual('biomaterial', metadata_resource.metadata_type)
        self.assertEqual(uuid, metadata_resource.uuid)
        self.assertEqual(raw_metadata['content'], metadata_resource.metadata_json)
        self.assertEqual(raw_metadata['dcpVersion'], metadata_resource.dcp_version)
        self.assertEqual(raw_metadata['submissionDate'], metadata_resource.provenance.submission_date)
        self.assertEqual(raw_metadata['updateDate'], metadata_resource.provenance.update_date)


class DataFileTest(TestCase):

    def test_parse_bucket_from_cloud_url(self):
        test_cloud_url = "s3://test-bucket/somefile.txt"
        test_data_file = DataFile("mock_uuid", "mock_version", "mock_file_name", test_cloud_url)
        self.assertEqual(test_data_file.source_bucket(), "test-bucket")

    def test_parse_key_from_cloud_url(self):
        test_cloud_url = "s3://test-bucket/somefile.txt"
        test_data_file = DataFile("mock_uuid", "mock_version", "mock_file_name", test_cloud_url)
        self.assertEqual(test_data_file.source_key(), "somefile.txt")

    def test_parse_nested_key_from_cloud_url(self):
        test_cloud_url = "s3://test-bucket/somedir/somesubdir/somefile.txt"
        test_data_file = DataFile("mock_uuid", "mock_version", "mock_file_name", test_cloud_url)
        self.assertEqual(test_data_file.source_key(), "somedir/somesubdir/somefile.txt")
