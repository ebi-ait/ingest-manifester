from unittest import TestCase

from ingest.api.ingestapi import IngestApi
from exporter.graph.graph_crawler import GraphCrawler
from exporter.metadata import MetadataService
from mock import MagicMock

from manifest.generator import ManifestGenerator
from tests.mocks.ingest import MockIngestAPI
from tests.mocks.files import MockEntityFiles


class TestGenerator(TestCase):
    def setUp(self):
        # Setup Entity Files Utility
        self.files = MockEntityFiles(base_uri='http://mock-ingest-api/')

        # Setup Mocked APIs
        self.ingest = MagicMock(spec=IngestApi, wraps=MockIngestAPI(mock_entity_retriever=self.files))

    def test_generate_manifest(self):
        # given:
        generator = ManifestGenerator(ingest_client=self.ingest,
                                      graph_crawler=GraphCrawler(MetadataService(self.ingest)))
        example_manifest = self.files.get_entity('bundleManifests', 'example-assay-manifest')

        # when:
        actual_manifest = generator.generate_manifest(process_uuid='mock-assay-process',
                                                      submission_uuid='mock-submission')

        # then:
        self.assertEqual(example_manifest, actual_manifest.__dict__)
