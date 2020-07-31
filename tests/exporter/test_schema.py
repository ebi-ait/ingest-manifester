from unittest import TestCase

from mock import Mock, MagicMock

from exporter.schema import SchemaService
from ingest.api.ingestapi import IngestApi

from time import sleep


class SchemaServiceTest(TestCase):

    def test_cached_schema_retrieval(self):
        mock_ingest_api = MagicMock(spec=IngestApi)
        mock_ingest_api.get_schemas = Mock()
        mock_ingest_api.get_schemas.return_value = [{
            "_links": {"json-schema": {"href": "https://some-schema-url"}},
            "schemaVersion": "1.2.3"
        }]

        test_ttl_seconds = 3
        test_schema_service = SchemaService(mock_ingest_api, ttl=test_ttl_seconds)

        # call a couple of times, assert that the ingest-api is called only once within the ttl
        test_schema_service.cached_latest_file_descriptor_schema()
        test_schema_service.cached_latest_file_descriptor_schema()

        self.assertEqual(mock_ingest_api.get_schemas.call_count, 1)

        # sleep and assert the ingest-api is called a second time after ttl expiry
        sleep(test_ttl_seconds)
        test_schema_service.cached_latest_file_descriptor_schema()
        test_schema_service.cached_latest_file_descriptor_schema()

        self.assertEqual(mock_ingest_api.get_schemas.call_count, 2)





