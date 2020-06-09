from ingest.api.ingestapi import IngestApi

from manifest.manifests import AssayManifest
from exporter.graph.experiment_graph import ExperimentGraph
from exporter.graph.graph_crawler import GraphCrawler
from exporter.metadata import MetadataResource, DataFile
from typing import Dict, List


class ManifestGenerator:
    def __init__(self, ingest_client: IngestApi, graph_crawler: GraphCrawler):
        self.ingest_client = ingest_client
        self.graph_crawler = graph_crawler

    def generate_manifest(self, process_uuid: str, submission_uuid: str) -> AssayManifest:
        process = self.get_process(process_uuid)
        project = self.project_for_process(process)

        experiment_graph = self.graph_crawler.generate_experiment_graph(process, project)
        assay_manifest = ManifestGenerator.assay_manifest_from_experiment_graph(experiment_graph, submission_uuid)

        return assay_manifest

    def get_process(self, process_uuid: str) -> MetadataResource:
        return MetadataResource.from_dict(self.ingest_client.get_entity_by_uuid('processes', process_uuid))

    def project_for_process(self, process: MetadataResource) -> MetadataResource:
        return MetadataResource.from_dict(list(self.ingest_client.get_related_entities("projects", process.full_resource, "projects"))[0])

    @staticmethod
    def assay_manifest_from_experiment_graph(experiment_graph: ExperimentGraph, submission_uuid: str) -> AssayManifest:
        assay_manifest = AssayManifest()
        assay_manifest.envelopeUuid = submission_uuid

        assay_manifest.fileProjectMap.update(ManifestGenerator.metadata_uuid_map_from_graph(experiment_graph, "project"))
        assay_manifest.fileBiomaterialMap.update(ManifestGenerator.metadata_uuid_map_from_graph(experiment_graph, "biomaterial"))
        assay_manifest.fileProcessMap.update(ManifestGenerator.metadata_uuid_map_from_graph(experiment_graph, "process"))
        assay_manifest.fileProtocolMap.update(ManifestGenerator.metadata_uuid_map_from_graph(experiment_graph, "protocol"))
        assay_manifest.fileFilesMap.update(ManifestGenerator.metadata_uuid_map_from_graph(experiment_graph, "file"))

        assay_manifest.dataFiles = [DataFile.from_file_metadata(m).uuid for m in experiment_graph.nodes.get_nodes()
                                    if m.metadata_type == "file"]

        return assay_manifest

    @staticmethod
    def metadata_uuid_map_from_graph(experiment_graph: ExperimentGraph, metadata_type: str) -> Dict[str, List[str]]:
        return dict([(m.uuid, [m.uuid]) for m in experiment_graph.nodes.get_nodes() if m.metadata_type == metadata_type])
