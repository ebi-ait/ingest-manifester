from ingest.api.ingestapi import IngestApi
from exporter.metadata import MetadataResource, DataFile
from exporter.graph.graph_crawler import GraphCrawler
from exporter.terra.gcs_staging_client import GcsStagingClient


class TerraExporter:
    def __init__(self, ingest_client: IngestApi, graph_crawler: GraphCrawler, gcs_staging_client: GcsStagingClient):
        self.ingest_client = ingest_client
        self.graph_crawler = graph_crawler
        self.gcs_staging_client = gcs_staging_client

    def export(self, process_uuid, submission_uuid, experiment_uuid, experiment_version):
        process = self.get_process(process_uuid)
        submission = self.get_submission(submission_uuid)
        project = self.project_for_submission(submission)

        experiment_graph = self.graph_crawler.experiment_graph_for_process(process)
        experiment_data_files = [DataFile.from_file_metadata(m) for m in experiment_graph.nodes.get_nodes() if m.metadata_type == "file"]

        self.gcs_staging_client.write_metadata(project)
        self.gcs_staging_client.write_metadatas(experiment_graph.nodes.get_nodes())
        self.gcs_staging_client.write_links(experiment_graph.links, experiment_uuid, experiment_version)
        self.gcs_staging_client.write_data_files(experiment_data_files)

    def get_process(self, process_uuid) -> MetadataResource:
        return MetadataResource.from_dict(self.ingest_client.get_entity_by_uuid('processes', process_uuid))

    def get_submission(self, submission_uuid):
        return self.ingest_client.get_entity_by_uuid('submissionEnvelopes', submission_uuid)

    def project_for_submission(self, submission) -> MetadataResource:
        return MetadataResource.from_dict(list(self.ingest_client.get_related_entities("projects", submission, "projects"))[0])