from ingest.api.ingestapi import IngestApi
from exporter import utils
from exporter.metadata import MetadataResource, DataFile, FileChecksums
from exporter.graph.experiment_graph import LinkSet
from exporter.schema import SchemaService
from exporter.terra.gcs import GcsXferStorage, GcsStorage, Streamable
from typing import Iterable, Dict, Tuple

from io import StringIO

from google.cloud import storage
from google.oauth2.service_account import Credentials
import json

from dataclasses import dataclass


@dataclass
class FileDescriptor:
    file_uuid: str
    file_version: str
    file_name: str
    content_type: str
    size: int
    checksums: FileChecksums

    def to_dict(self) -> Dict:
        sha1 = self.checksums.sha1.lower() if self.checksums.sha1 else self.checksums.sha1
        sha256 = self.checksums.sha256.lower() if self.checksums.sha256 else self.checksums.sha256
        crc32c = self.checksums.crc32c.lower() if self.checksums.crc32c else self.checksums.crc32c

        return dict(
            file_id=self.file_uuid,
            file_version=self.file_version,
            file_name=self.file_name,
            content_type=self.content_type,
            size=self.size,
            sha1=sha1,
            sha256=sha256,
            crc32c=crc32c,
            s3_etag=self.checksums.s3_etag,
            schema_type='file_descriptor'
        )

    @staticmethod
    def from_file_metadata(file_metadata: MetadataResource) -> 'FileDescriptor':
        data_file = DataFile.from_file_metadata(file_metadata)
        return FileDescriptor(data_file.uuid, file_metadata.dcp_version, data_file.file_name,
                              data_file.content_type, data_file.size, data_file.checksums)


class DcpStagingException(Exception):
    pass


class DcpStagingClient:

    def __init__(self, gcs_storage: GcsStorage, gcs_xfer: GcsXferStorage, schema_service: SchemaService, ingest_client: IngestApi):
        self.gcs_storage = gcs_storage
        self.gcs_xfer = gcs_xfer
        self.schema_service = schema_service
        self.ingest_client = ingest_client

    def transfer_data_files(self, submission: Dict, project_uuid, export_job_id: str):
        upload_area = submission["stagingDetails"]["stagingAreaLocation"]["value"]
        bucket_and_key = self.bucket_and_key_for_upload_area(upload_area)
        transfer_job_spec, success = self.gcs_xfer.transfer_upload_area(bucket_and_key[0], bucket_and_key[1], project_uuid, export_job_id)
        return transfer_job_spec, success

    def write_metadatas(self, metadatas: Iterable[MetadataResource], project_uuid: str):
        for metadata in metadatas:
            self.write_metadata(metadata, project_uuid)

    def write_metadata(self, metadata: MetadataResource, project_uuid: str):

        # TODO1: only proceed if lastContentModified > last

        dest_object_key = f'{project_uuid}/metadata/{metadata.concrete_type()}/{metadata.uuid}_{metadata.dcp_version}.json'

        metadata_json = metadata.get_content(with_provenance=True)
        data_stream = DcpStagingClient.dict_to_json_stream(metadata_json)
        self.write_to_staging_bucket(dest_object_key, data_stream)

        # TODO2: patch dcpVersion        
        #patch_url = metadata.metadata_json['_links']['self']['href']
        #self.ingest_client.patch(patch_url, {"dcpVersion": metadata.dcp_version})

        if metadata.metadata_type == "file":
            self.write_file_descriptor(metadata, project_uuid)

    def write_links(self, link_set: LinkSet, process_uuid: str, process_version: str, project_uuid: str):
        dest_object_key = f'{project_uuid}/links/{process_uuid}_{process_version}_{project_uuid}.json'
        links_json = self.generate_links_json(link_set)
        data_stream = DcpStagingClient.dict_to_json_stream(links_json)
        self.write_to_staging_bucket(dest_object_key, data_stream)

    def write_file_descriptor(self, file_metadata: MetadataResource, project_uuid: str):
        dest_object_key = f'{project_uuid}/descriptors/{file_metadata.concrete_type()}/{file_metadata.uuid}_{file_metadata.dcp_version}.json'
        file_descriptor_json = self.generate_file_desciptor_json(file_metadata)
        data_stream = DcpStagingClient.dict_to_json_stream(file_descriptor_json)
        self.write_to_staging_bucket(dest_object_key, data_stream)

    def generate_file_desciptor_json(self, file_metadata) -> Dict:
        latest_file_descriptor_schema = self.schema_service.cached_latest_file_descriptor_schema()
        file_descriptor = FileDescriptor.from_file_metadata(file_metadata)

        file_descriptor_dict = file_descriptor.to_dict()
        file_descriptor_dict["describedBy"] = latest_file_descriptor_schema.schema_url
        file_descriptor_dict["schema_version"] = latest_file_descriptor_schema.schema_version

        return file_descriptor_dict

    def write_to_staging_bucket(self, object_key: str, data_stream: Streamable):
        self.gcs_storage.write(object_key, data_stream)

    def generate_links_json(self, link_set: LinkSet) -> Dict:
        latest_links_schema = self.schema_service.cached_latest_links_schema()

        links_json = link_set.to_dict()
        links_json["describedBy"] = latest_links_schema.schema_url
        links_json["schema_version"] = latest_links_schema.schema_version
        links_json["schema_type"] = "links"

        return links_json

    @staticmethod
    def dict_to_json_stream(d: Dict) -> StringIO:
        return StringIO(json.dumps(d))

    @staticmethod
    def bucket_and_key_for_upload_area(upload_area: str) -> Tuple[str, str]:
        bucket_and_key_str = upload_area.split("//")[1]
        bucket_and_key_list = bucket_and_key_str.split("/", 1)

        return bucket_and_key_list[0], bucket_and_key_list[1].split("/")[0]

    class Builder:
        def __init__(self):
            self.schema_service = None
            self.gcs_storage = None
            self.gcs_xfer = None

        def with_gcs_info(self, service_account_credentials_path: str, gcp_project: str, bucket_name: str,
                          bucket_prefix: str) -> 'DcpStagingClient.Builder':
            with open(service_account_credentials_path) as source:
                info = json.load(source)
                storage_credentials: Credentials = Credentials.from_service_account_info(info)
                gcs_client = storage.Client(project=gcp_project, credentials=storage_credentials)
                self.gcs_storage = GcsStorage(gcs_client, bucket_name, bucket_prefix)

                return self

        def with_gcs_xfer(self, service_account_credentials_path: str, gcp_project: str, bucket_name: str, bucket_prefix: str, aws_access_key_id: str, aws_access_key_secret: str):
            with open(service_account_credentials_path) as source:
                info = json.load(source)
                credentials: Credentials = Credentials.from_service_account_info(info)
                self.gcs_xfer = GcsXferStorage(aws_access_key_id, aws_access_key_secret, gcp_project, bucket_name, bucket_prefix, credentials)

                return self

        def with_ingest_client(self, ingest_client: IngestApi) -> 'DcpStagingClient.Builder':
            self.ingest_client = ingest_client
            return self

        def with_schema_service(self, schema_service: SchemaService) -> 'DcpStagingClient.Builder':
            self.schema_service = schema_service
            return self

        def build(self) -> 'DcpStagingClient':
            if not self.gcs_xfer:
                raise Exception("gcs_xfer must be set")
            elif not self.gcs_storage:
                raise Exception("gcs_storage must be set")
            elif not self.schema_service:
                raise Exception("schema_service must be set")
            elif not self.ingest_client:
                raise Exception("ingest_client must be set")
            else:
                return DcpStagingClient(self.gcs_storage, self.gcs_xfer, self.schema_service, self.ingest_client)
