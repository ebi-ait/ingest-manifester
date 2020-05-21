from ingest.api.ingestapi import IngestApi
from exporter.metadata import MetadataResource
from exporter.graph.experiment_graph import ExperimentGraph, Link, Input, Output, ProtocolLink
from typing import List, Iterable, Dict
from functools import reduce
from _operator import iconcat
from dataclasses import dataclass


@dataclass
class ProcessInfo:
    process: MetadataResource
    inputs: List[MetadataResource]
    outputs: List[MetadataResource]
    protocols: List[MetadataResource]


class GraphCrawler:
    def __init__(self, ingest_client: IngestApi):
        self.ingest_client = ingest_client

    def experiment_graph_for_process(self, process: MetadataResource) -> ExperimentGraph:
        return self._crawl(process)

    def _crawl(self, process: MetadataResource) -> ExperimentGraph:
        partial_graph = ExperimentGraph()

        process_info = self.process_info(process)
        partial_graph.nodes.add_nodes(process_info.inputs + process_info.outputs + process_info.protocols + [process])
        partial_graph.links.add_link(GraphCrawler.link_for(process_info))

        processes_to_crawl = GraphCrawler.flatten([self.get_derived_by_processes(i) for i in process_info.inputs])

        return reduce(lambda g1, g2: g1.extend(g2),
                      map(lambda proc: self._crawl(proc), processes_to_crawl),
                      partial_graph)

    @staticmethod
    def link_for(process_info: ProcessInfo):
        link_inputs = [Input(i.concrete_type(), i.uuid) for i in process_info.inputs]
        link_outputs = [Output(o.concrete_type(), o.uuid) for o in process_info.outputs]
        link_protocols = [ProtocolLink(p.concrete_type(), p.uuid) for p in process_info.protocols]

        process_type = process_info.process.concrete_type()
        process_uuid = process_info.process.uuid
        return Link(process_uuid, process_type, link_inputs, link_outputs, link_protocols)

    @staticmethod
    def flatten(list_of_lists: Iterable[Iterable]) -> List:
        return reduce(iconcat, list_of_lists, [])

    def get_derived_by_processes(self, experiment_material: MetadataResource) -> List[MetadataResource]:
        return GraphCrawler.parse_metadata_resources(self.ingest_client.get_related_entities('derivedByProcesses', experiment_material.full_resource, 'processes'))

    def get_derived_biomaterials(self, process: MetadataResource) -> List[MetadataResource]:
        return GraphCrawler.parse_metadata_resources(self.ingest_client.get_related_entities('derivedBiomaterials', process.full_resource, 'biomaterials'))

    def get_derived_files(self, process: MetadataResource) -> List[MetadataResource]:
        return GraphCrawler.parse_metadata_resources(self.ingest_client.get_related_entities('derivedFiles', process.full_resource, 'files'))

    def get_input_biomaterials(self, process: MetadataResource):
        return GraphCrawler.parse_metadata_resources(self.ingest_client.get_related_entities('inputBiomaterials', process.full_resource, 'biomaterials'))

    def get_input_files(self, process: MetadataResource):
        return GraphCrawler.parse_metadata_resources(self.ingest_client.get_related_entities('inputFiles', process.full_resource, 'files'))

    def get_protocols(self, process: MetadataResource) -> List[MetadataResource]:
        return GraphCrawler.parse_metadata_resources(self.ingest_client.get_related_entities('protocols', process.full_resource, 'protocols'))

    def process_info(self, process: MetadataResource) -> ProcessInfo:
        inputs = self.get_input_biomaterials(process) + self.get_input_files(process)
        outputs = self.get_derived_biomaterials(process) + self.get_derived_files(process)
        protocols = self.get_protocols(process)
        return ProcessInfo(process, inputs, outputs, protocols)

    @staticmethod
    def parse_metadata_resources(metadata_resources: List[Dict]) -> List[MetadataResource]:
        return [MetadataResource.from_dict(m, True) for m in metadata_resources]
