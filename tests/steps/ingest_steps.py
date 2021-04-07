from behave import *
from ingest.api.ingestapi import IngestApi
from mock import MagicMock

from exporter.graph.graph_crawler import GraphCrawler
from exporter.metadata import MetadataResource, MetadataService
from tests.mocks.files import MockEntityFiles
from tests.mocks.ingest import MockIngestAPI


@given('a submission')
def step_impl(context):
    context.files = MockEntityFiles(base_uri='http://mock-ingest-api/')
    context.ingest = MagicMock(spec=IngestApi,
                               wraps=MockIngestAPI(mock_entity_retriever=context.files))
    context.crawler = GraphCrawler(MetadataService(context.ingest))


@given('a {process_type} process')
def step_impl(context, process_type):
    context.process = MetadataResource.from_dict(context.files.get_entity('processes', f'mock-{process_type}-process'))


@given('a project')
def step_impl(context):
    context.project = MetadataResource.from_dict(context.files.get_entity('projects', f'mock-project'))


@when('experiment graph for process is generated')
def step_impl(context):
    context.graph = context.crawler.experiment_graph_for_process(context.process)


@when('supplementary files graph for project is generated')
def step_impl(context):
    context.graph = context.crawler.supplementary_files_graph(context.project)


@when('experiment graph is generated')
def step_impl(context):
    context.graph = context.crawler.generate_experiment_graph(context.process, context.project)


@then('graph has {count:d} nodes')
def step_impl(context, count):
    assert len(context.graph.nodes.get_nodes()) == count


@then('graph has {count:d} links')
def step_impl(context, count):
    assert len(context.graph.links.get_links()) == count
