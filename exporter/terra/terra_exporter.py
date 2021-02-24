from ingest.api.ingestapi import IngestApi
from exporter.metadata import MetadataResource, MetadataService, DataFile
from exporter.graph.graph_crawler import GraphCrawler
from exporter.terra.dcp_staging_client import DcpStagingClient
from typing import Iterable


class TerraExporter:
    def __init__(self,
                 ingest_client: IngestApi,
                 metadata_service: MetadataService,
                 graph_crawler: GraphCrawler,
                 dcp_staging_client: DcpStagingClient):
        self.ingest_client = ingest_client
        self.metadata_service = metadata_service
        self.graph_crawler = graph_crawler
        self.dcp_staging_client = dcp_staging_client

    def export(self, process_uuid, submission_uuid, experiment_uuid, experiment_version, export_job_id):
        process = self.get_process(process_uuid)
        project = self.project_for_process(process)
        submission = self.get_submission(submission_uuid)
        
        self.dcp_staging_client.transfer_data_files(submission, project.uuid, export_job_id)
        
        experiment_graph = self.graph_crawler.generate_experiment_graph(process, project)
        
        self.dcp_staging_client.write_metadatas(experiment_graph.nodes.get_nodes(), project.uuid)
        self.dcp_staging_client.write_links(experiment_graph.links, experiment_uuid, experiment_version, project.uuid)

    def export_data(self, submission_uuid, project_uuid, export_job_id):
        submission = self.get_submission(submission_uuid)        
        self.dcp_staging_client.transfer_data_files(submission, project_uuid, export_job_id)

    def export_update(self, metadata_urls: Iterable[str]):
        metadata_to_update = [self.metadata_service.fetch_resource(url) for url in metadata_urls]
        self.dcp_staging_client.write_metadatas(metadata_to_update)

    def get_process(self, process_uuid) -> MetadataResource:
        return MetadataResource.from_dict(self.ingest_client.get_entity_by_uuid('processes', process_uuid))

    def get_submission(self, submission_uuid):
        return self.ingest_client.get_entity_by_uuid('submissionEnvelopes', submission_uuid)

    def project_for_process(self, process: MetadataResource) -> MetadataResource:
        return MetadataResource.from_dict(list(self.ingest_client.get_related_entities("projects", process.full_resource, "projects"))[0])