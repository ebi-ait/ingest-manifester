import os
from unittest import TestCase

from ingest.api.ingestapi import IngestApi
from mock import MagicMock

from manifest.exporter import ManifestExporter
from manifest.generator import ManifestGenerator
from tests.mocks.ingest import MockIngestAPI

BASE_PATH = os.path.dirname(__file__)


class TestExporter(TestCase):
    def setUp(self) -> None:
        # Setup mocked APIs
        self.mock_ingest_api = MagicMock(spec=IngestApi, wraps=MockIngestAPI())
        self.mock_manifest_generator = MagicMock(spec=ManifestGenerator)
        self.pre_manifest = self.mock_ingest_api.get_entity_by_uuid('bundleManifests', 'pre-submitted-manifest')
        self.created_manifest = self.mock_ingest_api.get_entity_by_uuid('bundleManifests', 'mock-input-manifest')

        self.mock_manifest_generator.generate_manifest = MagicMock(return_value=self.pre_manifest)
        self.mock_ingest_api.create_bundle_manifest = MagicMock(return_value=self.created_manifest)

    def test_exporter_export(self):
        # given:
        exporter = ManifestExporter(ingest_api=self.mock_ingest_api)
        exporter.manifest_generator = self.mock_manifest_generator

        # when:
        exporter.export(process_uuid='process-uuid', submission_uuid='submission-uuid')

        # then:
        exporter.manifest_generator.generate_manifest.assert_called_with('process-uuid', 'submission-uuid')
        exporter.ingest_api.create_bundle_manifest.assert_called_with(self.pre_manifest)
