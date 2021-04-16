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
        self.mock_ingest = MagicMock(spec=IngestApi, wraps=MockIngestAPI(mock_entity_retriever=self.mock_files))

    def test_generate_experiment_process_graph(self):
        # given
        ingest_client = self.mock_ingest
        crawler = GraphCrawler(MetadataService(ingest_client))

        test_assay_process = MetadataResource.from_dict(self.mock_files.get_entity('processes', 'mock-assay-process'))

        # when
        experiment_graph = crawler.generate_experiment_graph(test_assay_process)

        # then
        self.assertEqual(len(experiment_graph.nodes.get_nodes()), 15)
        self.assertEqual(len(experiment_graph.links.get_links()), 4)

    def test_generate_supplementary_files_graph(self):
        # given
        ingest_client = self.mock_ingest
        crawler = GraphCrawler(MetadataService(ingest_client))

        test_project = MetadataResource.from_dict(self.mock_files.get_entity('projects', 'mock-project'))

        # when
        experiment_graph = crawler.generate_supplementary_files_graph(test_project)

        # then
        self.assertEqual(len(experiment_graph.nodes.get_nodes()), 3)
        self.assertEqual(len(experiment_graph.links.get_links()), 1)  # project, and 2 supplementary files

    def test_generate_complete_experiment_graph(self):
        # given
        ingest_client = self.mock_ingest
        crawler = GraphCrawler(MetadataService(ingest_client))

        test_assay_process = MetadataResource.from_dict(self.mock_files.get_entity('processes', 'mock-assay-process'))
        test_project = MetadataResource.from_dict(self.mock_files.get_entity('projects', 'mock-project'))

        # when
        experiment_graph = crawler.generate_complete_experiment_graph(test_assay_process, test_project)

        # then
        expected_links = self.mock_files.get_links_json()

        nodes = self._get_nodes(expected_links)

        self.assertEqual(set([node.uuid for node in experiment_graph.nodes.get_nodes()]), nodes)
        self.assertEqual(len(experiment_graph.links.get_links()), len(expected_links.get('links', [])))
        self.assertEqual(experiment_graph.links.to_dict(), expected_links)

    def _get_nodes(self, expected_links):
        nodes = set()
        for link in expected_links.get('links', []):
            link_type = link.get('link_type')
            entity = link.get('entity', {})
            if link_type == 'supplementary_file_link' and entity:
                nodes.add(entity.get('entity_id'))
                nodes.update([file.get('file_id') for file in link.get('files', [])])
            else:
                nodes.add(link.get('process_id'))
                nodes.update([input.get('input_id') for input in link.get('inputs', [])])
                nodes.update([output.get('output_id') for output in link.get('outputs', [])])
                nodes.update([protocol.get('protocol_id') for protocol in link.get('protocols', [])])
        return nodes
