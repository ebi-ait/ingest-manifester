import os
from unittest import TestCase

from ingest.api.ingestapi import IngestApi
from mock import MagicMock

from manifest.exporter import ManifestExporter
from manifest.generator import ManifestGenerator
from tests.mocks.ingest import MockIngestAPI
from tests.mocks.files import MockEntityFiles


class TestExporter(TestCase):
    def setUp(self) -> None:
        # Setup Entity Files Utility
        self.files = MockEntityFiles(base_uri='http://mock-ingest-api/')

        # Setup Mocked APIs
        self.ingest = MagicMock(spec=IngestApi, wraps=MockIngestAPI(mock_entity_retriever=self.files))

    def test_exporter_export(self):
        # given:
        exporter = ManifestExporter(ingest_api=self.ingest)
        generator = MagicMock(spec=ManifestGenerator)
        generated_manifest = self.files.get_entity('bundleManifests', 'generated-manifest')
        generator.generate_manifest = MagicMock(return_value=generated_manifest)
        exporter.manifest_generator = generator

        # and:
        submitted_manifest = self.files.get_entity('bundleManifests', 'mock-input-manifest')
        exporter.ingest_api.create_bundle_manifest = MagicMock(return_value=submitted_manifest)

        # when:
        exporter.export(process_uuid='process-uuid', submission_uuid='submission-uuid')

        # then:
        exporter.manifest_generator.generate_manifest.assert_called_with('process-uuid', 'submission-uuid')
        exporter.ingest_api.create_bundle_manifest.assert_called_with(generated_manifest)
