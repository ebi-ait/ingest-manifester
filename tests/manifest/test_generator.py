from unittest import TestCase

from ingest.api.ingestapi import IngestApi
from mock import MagicMock

from manifest.generator import ManifestGenerator
from manifest.manifests import ProcessInfo
from tests.mocks.ingest import MockIngestAPI
from tests.mocks.files import MockEntityFiles


def process_info_converter(process_info: dict) -> ProcessInfo:
    convert = ProcessInfo()
    convert.project = process_info.get('project')
    convert.input_biomaterials = process_info.get('input_biomaterials')
    convert.derived_by_processes = process_info.get('derived_by_processes')
    convert.input_files = process_info.get('input_files')
    convert.derived_files = process_info.get('derived_files')
    convert.protocols = process_info.get('protocols')
    convert.supplementary_files = process_info.get('supplementary_files')
    convert.input_bundle = process_info.get('input_bundle')
    return convert


class TestGenerator(TestCase):
    def setUp(self):
        # Setup Entity Files Utility
        self.files = MockEntityFiles(base_uri='http://mock-ingest-api/')

        # Setup Mocked APIs
        self.ingest = MagicMock(spec=IngestApi, wraps=MockIngestAPI(mock_entity_retriever=self.files))

    def test_get_all_process_info(self):
        # given:
        generator = ManifestGenerator(ingest_api=self.ingest)
        input_assay_process = self.files.get_entity('processes', 'mock-assay-process')
        example_process_info = self.files.get_entity('processes', 'example-process-info')

        # when:
        actual_process_info = generator.get_all_process_info(input_assay_process)

        # then:
        self.assertEqual(example_process_info, actual_process_info.__dict__)

    def test_build_assay_manifest(self):
        # given:
        generator = ManifestGenerator(ingest_api=self.ingest)
        input_process_info = self.files.get_entity('processes', 'example-process-info')
        example_manifest = self.files.get_entity('bundleManifests', 'example-assay-manifest')

        # when:
        converted_process_info = process_info_converter(input_process_info)
        actual_manifest = generator.build_assay_manifest(converted_process_info, 'mock-submission')

        # then:
        self.assertEqual(example_manifest, actual_manifest.__dict__)

    def test_generate_manifest(self):
        # given:
        generator = ManifestGenerator(ingest_api=self.ingest)
        example_manifest = self.files.get_entity('bundleManifests', 'example-assay-manifest')

        # when:
        actual_manifest = generator.generate_manifest(process_uuid='mock-assay-process',
                                                      submission_uuid='mock-submission')

        # then:
        self.assertEqual(example_manifest, actual_manifest.__dict__)
