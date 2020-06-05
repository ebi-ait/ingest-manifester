from unittest import TestCase
from exporter.terra.terra_exporter import TerraExporter

from ingest.api.ingestapi import IngestApi
from exporter.graph.graph_crawler import GraphCrawler
from exporter.terra.dcp_staging_client import DcpStagingClient
from exporter.metadata import MetadataResource, MetadataService
from exporter.schema import SchemaService


class TerraExporterTest(TestCase):

    def test_export_experiment(self):
        ingest_client = IngestApi(url="http://api.ingest.dev.archive.data.humancellatlas.org")
        metadata_service = MetadataService(ingest_client)
        schema_service = SchemaService(ingest_client)
        crawler = GraphCrawler(metadata_service)
        access_key = "AKIA4WBQCFL3D2GHSFYQ"
        access_key_secret = "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"
        svc_credentials_path = "/Users/rolando/Downloads/broad_svc_account.json"
        dcp_staging_client = (DcpStagingClient
                              .Builder()
                              .aws_access_key(access_key, access_key_secret)
                              .with_gcs_info(svc_credentials_path, "broad-dsp-monster-hca-dev", "broad-dsp-monster-hca-dev-ebi-staging", "testing").with_schema_service(schema_service).build())

        terra_exporter = TerraExporter(ingest_client, metadata_service, crawler, dcp_staging_client)

        process_url = "https://api.ingest.dev.archive.data.humancellatlas.org/processes/5eda252aedbbef1c8f203e3e"
        p_uuid = MetadataResource.from_dict(ingest_client.get_process(process_url)).uuid

        ex = terra_exporter.export(p_uuid, "21da5f2b-7ac4-440e-b11c-5f0996d4dd21", "mock_experiment_uuid", "mock_experiment_version")

