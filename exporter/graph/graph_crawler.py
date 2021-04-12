from exporter.metadata import MetadataResource, MetadataService
from exporter.graph.experiment_graph import ExperimentGraph, ProcessLink, Input, Output, ProtocolLink, SupplementaryFileLink, SupplementedEntity, SupplementaryFile
from typing import List, Iterable, Optional, Callable
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


@dataclass
class SupplementaryFilesInfo:
    for_entity: MetadataResource
    files: List[MetadataResource]


class GraphCrawler:
    def __init__(self, metadata_service: MetadataService):
        self.metadata_service = metadata_service

    def generate_complete_experiment_graph(self, process: MetadataResource, project: MetadataResource) -> ExperimentGraph:
        experiment_process_graph = self.generate_experiment_graph(process)
        supplementary_files_graph = self.generate_supplementary_files_graph(project)

        return experiment_process_graph.extend(supplementary_files_graph)

    def generate_experiment_graph(self, process: MetadataResource) -> ExperimentGraph:
        upward_graph = self._crawl(process, self._crawl_inputs)
        downward_graph = self._crawl(process, self._crawl_outputs)
        initial_graph = ExperimentGraph()

        return reduce(lambda g1, g2: g1.extend(g2),
               [upward_graph, downward_graph],
               initial_graph)

    def generate_supplementary_files_graph(self, project: MetadataResource) -> ExperimentGraph:
        """
        Finds supplementary files for this project, if any, and generates corresponding links and inserts
        the project node + supplementary file links into a small graph
        :param project:
        :return: an ExperimentGraph containing the project and, if any, supplementary file links
        """
        suppl_files_info = self.supplementary_files_info(project)
        if suppl_files_info:
            graph = ExperimentGraph()
            graph.nodes.add_nodes(suppl_files_info.files + [project])
            suppl_files_link = GraphCrawler.supplementary_file_link_for(suppl_files_info)
            graph.links.add_link(suppl_files_link)

            return graph
        else:
            graph = ExperimentGraph()
            graph.nodes.add_node(project)
            return graph

    def _crawl(self, process: MetadataResource, crawl_strategy_func: Callable) -> ExperimentGraph:
        partial_graph = ExperimentGraph()

        process_info = self.process_info(process)
        partial_graph.nodes.add_nodes(process_info.inputs + process_info.outputs + process_info.protocols + [process])
        partial_graph.links.add_link(GraphCrawler.process_link_for(process_info))

        processes_to_crawl = crawl_strategy_func(process_info)

        return reduce(lambda g1, g2: g1.extend(g2),
                      map(lambda proc: self._crawl(proc, crawl_strategy_func), processes_to_crawl),
                      partial_graph)

    def _crawl_inputs(self, process_info: ProcessInfo) -> ExperimentGraph:
        return GraphCrawler.flatten([self.metadata_service.get_derived_by_processes(i) for i in process_info.inputs])

    def _crawl_outputs(self, process_info: ProcessInfo) -> ExperimentGraph:
        return GraphCrawler.flatten([self.metadata_service.get_input_to_processes(i) for i in process_info.outputs])


    @staticmethod
    def process_link_for(process_info: ProcessInfo) -> ProcessLink:
        link_inputs = [Input(i.concrete_type(), i.uuid) for i in process_info.inputs]
        link_outputs = [Output(o.concrete_type(), o.uuid) for o in process_info.outputs]
        link_protocols = [ProtocolLink(p.concrete_type(), p.uuid) for p in process_info.protocols]

        process_type = process_info.process.concrete_type()
        process_uuid = process_info.process.uuid
        return ProcessLink(process_uuid, process_type, link_inputs, link_outputs, link_protocols)

    @staticmethod
    def supplementary_file_link_for(supplementary_files_info: SupplementaryFilesInfo) -> SupplementaryFileLink:
        for_entity = supplementary_files_info.for_entity
        supplementary_files = [SupplementaryFile(file.concrete_type(), file.uuid) for file in supplementary_files_info.files]

        return SupplementaryFileLink(SupplementedEntity(for_entity.concrete_type(), for_entity.uuid),
                                     supplementary_files)

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

    def supplementary_files_info(self, metadata: MetadataResource) -> Optional[SupplementaryFilesInfo]:
        files = self.metadata_service.get_supplementary_files(metadata)
        if len(files) > 0:
            return SupplementaryFilesInfo(metadata, files)
        else:
            return None
