from ingest.exporter.metadata import MetadataResource

from copy import deepcopy
from typing import List, Set, Dict
from dataclasses import dataclass


@dataclass
class ProtocolLink:
    protocol_type: str
    protocol_uuid: str

    @staticmethod
    def from_metadata_resource(metadata: MetadataResource) -> 'ProtocolLink':
        return ProtocolLink(metadata.concrete_type(), metadata.uuid)

    def to_dict(self) -> Dict:
        return {
            'protocol_type': self.protocol_type,
            'protocol_id': self.protocol_uuid
        }

@dataclass
class Link:
    process_uuid: str
    inputs: Set[str]
    input_type: str
    outputs: Set[str]
    output_type: str
    protocols: List[ProtocolLink]

    def add_output(self, output_uuid: str):
        self.outputs.add(output_uuid)

    def combine_partial_link(self, link: 'Link'):
        self.inputs = self.inputs.union(link.inputs)
        self.outputs = self.outputs.union(link.outputs)

    def to_dict(self) -> Dict:
        return {
            'process': self.process_uuid,
            'inputs': self.inputs,
            'input_type': self.input_type,
            'outputs': self.outputs,
            'output_type': self.output_type,
            'protocols': [protocol.to_dict() for protocol in self.protocols]
        }


class LinkSet:

    def __init__(self):
        self.links: Dict[str, Link] = dict()

    def add_links(self, links: List[Link]):
        for link in links:
            self.add_link(link)

    def add_link(self, link: Link):
        link_uuid = link.process_uuid
        if link_uuid in self.links:
            self.links[link_uuid].combine_partial_link(link)
        else:
            self.links[link_uuid] = link

    def get_links(self) -> List[Link]:
        return list(self.links.values())


class MetadataNodeSet:

    def __init__(self):
        self.obj_uuids = set()  # the uuid of a link is just the uuid of the process denoting the link
        self.objs = []

    def __contains__(self, item: MetadataResource):
        return item.uuid in self.obj_uuids

    def add_node(self, node: MetadataResource):
        if node.uuid in self.obj_uuids:
            pass
        else:
            self.obj_uuids.add(node.uuid)
            self.objs.append(node)

    def add_nodes(self, nodes: List[MetadataResource]):
        for node in nodes:
            self.add_node(node)

    def get_nodes(self) -> List[MetadataResource]:
        return [deepcopy(obj) for obj in self.objs]


class ExperimentGraph:
    links: LinkSet
    nodes: MetadataNodeSet

    def __init__(self):
        self.links = LinkSet()
        self.nodes = MetadataNodeSet()

    def extend(self, graph: 'ExperimentGraph'):
        for link in graph.links.get_links():
            self.links.add_link(link)

        for node in graph.nodes.get_nodes():
            self.nodes.add_node(node)

        return self
