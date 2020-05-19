from ingest.api.ingestapi import IngestApi
from exporter.metadata import MetadataResource
from exporter.experimentgraph.experiment_graph import ExperimentGraph, Link, ProtocolLink
from typing import List, Iterable, Dict
from functools import reduce
from _operator import iconcat
from dataclasses import dataclass


@dataclass
class ProcessInputsAndProtocols:
    process: MetadataResource
    inputs: List[MetadataResource]
    protocols: List[MetadataResource]


class GraphCrawler:
    def __init__(self, ingest_client: IngestApi):
        self.ingest_client = ingest_client

    def experiment_graph_for_process(self, process: MetadataResource) -> ExperimentGraph:
        derived_biomaterials = self.get_derived_biomaterials(process)
        derived_files = self.get_derived_files(process)
        derived_entites = derived_biomaterials + derived_files

        empty_graph = ExperimentGraph()

        return reduce(lambda pg1, pg2: pg1.extend(pg2),
                      map(lambda entity: self._crawl(entity, empty_graph), derived_entites),
                      ExperimentGraph())

    def _crawl(self, derived_node: MetadataResource, partial_graph: ExperimentGraph) -> ExperimentGraph:
        if derived_node in partial_graph.nodes:
            return partial_graph
        else:
            derived_by_processes = self.get_derived_by_processes(derived_node)
            processes_protocols_and_inputs = [self.inputs_and_protocols_for_process(process) for process in derived_by_processes]
            processes_links = GraphCrawler.links_for_processes(processes_protocols_and_inputs, derived_node)

            partial_graph.links.add_links(processes_links)
            partial_graph.nodes.add_nodes([derived_node] +
                                          GraphCrawler.flatten([p.protocols + [p.process] for p in processes_protocols_and_inputs]))

            all_process_inputs = GraphCrawler.flatten([p.inputs for p in processes_protocols_and_inputs])
            return reduce(lambda pg1, pg2: pg1.extend(pg2),
                          map(lambda process_input: self._crawl(process_input, partial_graph), all_process_inputs),
                          ExperimentGraph())

    @staticmethod
    def link_for(process_uuid: str, input_uuids: List[str], input_type: str,
                 output_uuids: List[str], output_type: str, protocols: List[MetadataResource]) -> Link:
        protocol_links = [ProtocolLink.from_metadata_resource(protocol) for protocol in protocols]
        return Link(process_uuid, set(input_uuids), input_type, set(output_uuids), output_type, protocol_links)

    @staticmethod
    def flatten(list_of_lists: Iterable[Iterable]) -> List:
        return reduce(iconcat, list_of_lists, [])

    @staticmethod
    def links_for_processes(processes_protocols_and_inputs: List[ProcessInputsAndProtocols],
                            output: MetadataResource) -> List[Link]:
        links = []
        for process_protocol_and_inputs in processes_protocols_and_inputs:
            process_uuid = process_protocol_and_inputs.process.uuid
            links += GraphCrawler.links_for_process(process_uuid, output, process_protocol_and_inputs.protocols,
                                                    process_protocol_and_inputs.inputs)
        return links

    @staticmethod
    def links_for_process(process_uuid: str, output: MetadataResource,
                          protocols_for_process: List[MetadataResource],
                          inputs_for_process: List[MetadataResource]) -> List[Link]:
        output_uuid = output.uuid
        output_type = output.metadata_type
        file_input_uuids = [input.uuid for input in inputs_for_process if input.metadata_type == "file"]
        biomaterial_input_uuids = [input.uuid for input in inputs_for_process if input.metadata_type == "biomaterial"]

        links = []
        if len(file_input_uuids) > 0:
            links.append(GraphCrawler.link_for(process_uuid, file_input_uuids, "file",
                                               [output_uuid], output_type, protocols_for_process))

        if len(biomaterial_input_uuids) > 0:
            links.append(GraphCrawler.link_for(process_uuid, biomaterial_input_uuids, "biomaterial",
                                               [output_uuid], output_type, protocols_for_process))
        return links

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

    def inputs_and_protocols_for_process(self, process: MetadataResource) -> ProcessInputsAndProtocols:
        return ProcessInputsAndProtocols(process,
                                         self.get_input_biomaterials(process) + self.get_input_files(process),
                                         self.get_protocols(process))

    @staticmethod
    def parse_metadata_resources(metadata_resources: List[Dict]) -> List[MetadataResource]:
        return [MetadataResource.from_dict(m, True) for m in metadata_resources]
