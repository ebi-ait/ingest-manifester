from exporter.metadata import MetadataResource

from copy import deepcopy
from typing import List, Set, Dict, Iterable
from dataclasses import dataclass


@dataclass
class ProtocolLink:
    protocol_type: str
    protocol_uuid: str

    @staticmethod
    def from_metadata_resource(metadata: MetadataResource) -> 'ProtocolLink':
        return ProtocolLink(metadata.concrete_type(), metadata.uuid)

    def to_dict(self) -> Dict:
        return dict(
            protocol_type=self.protocol_type,
            protocol_id=self.protocol_uuid
        )


@dataclass
class Input:
    input_type: str
    input_uuid: str

    def to_dict(self) -> Dict[str, str]:
        return dict(
            input_type=self.input_type,
            input_id=self.input_uuid
        )


@dataclass
class Output:
    output_type: str
    output_uuid: str

    def to_dict(self) -> Dict[str, str]:
        return dict(
            output_type=self.output_type,
            output_id=self.output_uuid
        )


class Link:

    def __init__(self, process_uuid: str, process_type: str,
                 inputs: Iterable[Input], outputs: Iterable[Output], protocols: Iterable[ProtocolLink]):
        self._input_uuids: Set[str] = set()
        self._outputs_uuids: Set[str] = set()
        self._protocol_uuids: Set[str] = set()

        self.process_uuid = process_uuid
        self.process_type = process_type
        self.inputs: List[Input] = list()
        self.outputs: List[Output] = list()
        self.protocols: List[ProtocolLink] = list()

        for i in inputs:
            self.add_input(i)

        for o in outputs:
            self.add_output(o)

        for p in protocols:
            self.add_protocol(p)

    def add_input(self, i: Input):
        if i.input_uuid not in self._input_uuids:
            self._input_uuids.add(i.input_uuid)
            self.inputs.append(i)

    def add_output(self, o: Output):
        if o.output_uuid not in self._outputs_uuids:
            self._outputs_uuids.add(o.output_uuid)
            self.outputs.append(o)

    def add_protocol(self, p: ProtocolLink):
        if p.protocol_uuid not in self._protocol_uuids:
            self._protocol_uuids.add(p.protocol_uuid)
            self.protocols.append(p)

    def combine_partial_link(self, link: 'Link'):
        if self.process_type == link.process_type:
            for i in link.inputs:
                self.add_input(i)
            for o in link.outputs:
                self.add_output(o)
            for p in link.protocols:
                self.add_protocol(p)
        else:
            raise Exception(f'Cannot combine link (process_type: {self.process_type} with link of process_type {link.process_type}')

    def to_dict(self) -> Dict:
        return dict(
            process_id=self.process_uuid,
            process_type=self.process_type,
            inputs=[i.to_dict() for i in self.inputs],
            outputs=[o.to_dict() for o in self.outputs],
            protocols=[p.to_dict() for p in self.protocols]
        )


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

    def to_dict(self):
        return dict(
            links=[link.to_dict() for link in self.get_links()]
        )


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
