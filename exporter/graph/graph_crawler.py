from exporter.metadata import MetadataResource, MetadataService
from exporter.graph.experiment_graph import ExperimentGraph, Link, Input, Output, ProtocolLink
from typing import List, Iterable
from functools import reduce
from operator import iconcat
from dataclasses import dataclass

from concurrent.futures import ThreadPoolExecutor


@dataclass
class ProcessInfo:
    process: MetadataResource
    inputs: List[MetadataResource]
    outputs: List[MetadataResource]
    protocols: List[MetadataResource]


class GraphCrawler:
    def __init__(self, metadata_service: MetadataService):
        self.metadata_service = metadata_service

    def experiment_graph_for_process(self, process: MetadataResource) -> ExperimentGraph:
        return self._crawl(process)

    def _crawl(self, process: MetadataResource) -> ExperimentGraph:
        partial_graph = ExperimentGraph()

        process_info = self.process_info(process)
        partial_graph.nodes.add_nodes(process_info.inputs + process_info.outputs + process_info.protocols + [process])
        partial_graph.links.add_link(GraphCrawler.link_for(process_info))

        processes_to_crawl = GraphCrawler.flatten([self.metadata_service.get_derived_by_processes(i) for i in process_info.inputs])

        return reduce(lambda g1, g2: g1.extend(g2),
                      map(lambda proc: self._crawl(proc), processes_to_crawl),
                      partial_graph)

    @staticmethod
    def link_for(process_info: ProcessInfo) -> Link:
        link_inputs = [Input(i.concrete_type(), i.uuid) for i in process_info.inputs]
        link_outputs = [Output(o.concrete_type(), o.uuid) for o in process_info.outputs]
        link_protocols = [ProtocolLink(p.concrete_type(), p.uuid) for p in process_info.protocols]

        process_type = process_info.process.concrete_type()
        process_uuid = process_info.process.uuid
        return Link(process_uuid, process_type, link_inputs, link_outputs, link_protocols)

    @staticmethod
    def flatten(list_of_lists: Iterable[Iterable]) -> List:
        return reduce(iconcat, list_of_lists, [])

    def process_info(self, process: MetadataResource) -> ProcessInfo:
        with ThreadPoolExecutor() as executor:
            _input_biomaterials = executor.submit(lambda: self.metadata_service.get_input_biomaterials(process))
            _input_files = executor.submit(lambda: self.metadata_service.get_input_files(process))
            _output_biomaterials = executor.submit(lambda: self.metadata_service.get_derived_biomaterials(process))
            _output_files = executor.submit(lambda: self.metadata_service.get_derived_files(process))
            _protocols = executor.submit(lambda: self.metadata_service.get_protocols(process))

            inputs = _input_biomaterials.result() + _input_files.result()
            outputs = _output_biomaterials.result() + _output_files.result()
            protocols = _protocols.result()
            return ProcessInfo(process, inputs, outputs, protocols)
