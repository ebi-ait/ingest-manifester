import json
from unittest import TestCase

from ingest.api.ingestapi import IngestApi
from exporter.graph.graph_crawler import GraphCrawler
from exporter.metadata import MetadataResource, MetadataService

from tests.mocks.ingest import MockIngestAPI
from tests.mocks.files import MockEntityFiles

from mock import MagicMock


class GraphCrawlerTest(TestCase):
    def setUp(self) -> None:
        # Setup Entity Files Utility
        self.mock_files = MockEntityFiles(base_uri='http://mock-ingest-api/')

        # Setup Mocked APIs
        self.ingest = MagicMock(spec=IngestApi, wraps=MockIngestAPI(mock_entity_retriever=self.mock_files))

    def test_generate_experiment_process_graph(self):
        ingest_client = self.ingest
        crawler = GraphCrawler(MetadataService(ingest_client))

        test_assay_process = MetadataResource.from_dict(self.mock_files.get_entity('processes', 'mock-assay-process'))
        experiment_graph = crawler.experiment_graph_for_process(test_assay_process)

        self.assertEqual(len(experiment_graph.nodes.get_nodes()), 12)
        self.assertEqual(len(experiment_graph.links.get_links()), 3)

    def test_generate_supplementary_files_graph(self):
        ingest_client = self.ingest
        crawler = GraphCrawler(MetadataService(ingest_client))

        test_project = MetadataResource.from_dict(self.mock_files.get_entity('projects', 'mock-project'))
        experiment_graph = crawler.supplementary_files_graph(test_project)

        self.assertEqual(len(experiment_graph.nodes.get_nodes()), 3)
        self.assertEqual(len(experiment_graph.links.get_links()), 1)  # project, and 2 supplementary files

    def test_generate_complete_experiment_graph(self):
        # given
        ingest_client = self.ingest
        crawler = GraphCrawler(MetadataService(ingest_client))

        test_assay_process = MetadataResource.from_dict(self.mock_files.get_entity('processes', 'mock-assay-process'))
        test_project = MetadataResource.from_dict(self.mock_files.get_entity('projects', 'mock-project'))

        # when
        experiment_graph = crawler.generate_experiment_graph(test_assay_process, test_project)

        expected_links = self.mock_files.get_links_json()

        self.assertEqual(len(experiment_graph.nodes.get_nodes()), 18)
        self.assertEqual(len(experiment_graph.links.get_links()), 5)  # 4 process links and 1 supplementary files link
        self.assertEqual(experiment_graph.links.to_dict(), expected_links)
