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
        self.files = MockEntityFiles(base_uri='http://mock-ingest-api/')

        # Setup Mocked APIs
        self.ingest = MagicMock(spec=IngestApi, wraps=MockIngestAPI(mock_entity_retriever=self.files))

    def test_generate_experiment_graph(self):
        ingest_client = self.ingest
        crawler = GraphCrawler(MetadataService(ingest_client))

        test_assay_process = MetadataResource.from_dict(self.files.get_entity('processes', 'mock-assay-process'), False)
        experiment_graph = crawler.experiment_graph_for_process(test_assay_process)

        self.assertEquals(len(experiment_graph.nodes.get_nodes()), 12)
        self.assertEquals(len(experiment_graph.links.get_links()), 3)
