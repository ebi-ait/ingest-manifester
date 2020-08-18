from exporter.metadata import MetadataResource, DataFile, FileChecksums
from exporter.graph.experiment_graph import LinkSet
from exporter.schema import SchemaService
from typing import Iterable, IO, Dict, Any, Union

from io import StringIO, BufferedReader

from google.cloud import storage
from google.oauth2.service_account import Credentials
from google.api_core.exceptions import PreconditionFailed
from mypy_boto3_s3 import S3Client
from mypy_boto3_s3.type_defs import GetObjectOutputTypeDef as S3Object
from mypy_boto3.boto3_session import Session
from smart_open import open
import boto3
import json
import logging

from time import sleep
from dataclasses import dataclass


Streamable = Union[BufferedReader, StringIO, IO[Any]]


class UploadPollingException(Exception):
    pass


@dataclass
class FileDescriptor:
    file_uuid: str
    file_version: str
    file_name: str
    content_type: str
    size: int
    checksums: FileChecksums

    def to_dict(self) -> Dict:
        return dict(
            file_id=self.file_uuid,
            file_version=self.file_version,
            file_name=self.file_name,
            content_type=self.content_type,
            size=self.size,
            sha1=self.checksums.sha1,
            sha256=self.checksums.sha256,
            crc32c=self.checksums.crc32c,
            s3_etag=self.checksums.s3_etag
        )

    @staticmethod
    def from_file_metadata(file_metadata: MetadataResource) -> 'FileDescriptor':
        data_file = DataFile.from_file_metadata(file_metadata)
        name = f'{data_file.uuid}_{data_file.dcp_version}_{data_file.file_name}'
        return FileDescriptor(data_file.uuid, file_metadata.dcp_version, name,
                              data_file.content_type, data_file.size, data_file.checksums)


class GcsStorage:
    def __init__(self, gcs_client: storage.Client, bucket_name: str, storage_prefix: str):
        self.gcs_client = gcs_client
        self.bucket_name = bucket_name
        self.storage_prefix = storage_prefix

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

    def file_exists(self, object_key: str) -> bool:
        dest_key = f'{self.storage_prefix}/{object_key}'
        staging_bucket: storage.Bucket = self.gcs_client.bucket(self.bucket_name)
        blob: storage.Blob = staging_bucket.blob(dest_key)

        return blob.exists() and \
               blob.metadata is not None and \
               blob.metadata.get("export_completed", False)

    def write(self, object_key: str, data_stream: Streamable):
        try:
            dest_key = f'{self.storage_prefix}/{object_key}'
            staging_bucket: storage.Bucket = self.gcs_client.bucket(self.bucket_name)
            blob: storage.Blob = staging_bucket.blob(dest_key, chunk_size=1024 * 256 * 20)

            if not blob.exists():
                blob.upload_from_file(data_stream, if_generation_match=0)
                blob.metadata = {"export_completed": True}
                blob.patch()
            else:
                self.assert_file_uploaded(object_key)
        except PreconditionFailed as e:
            # With if_generation_match=0, this pre-condition failure indicates that another
            # export instance has began uploading this file. We should not attempt to upload
            # and instead poll for its completion
            self.assert_file_uploaded(object_key)

    def assert_file_uploaded(self, object_key):
        dest_key = f'{self.storage_prefix}/{object_key}'
        staging_bucket: storage.Bucket = self.gcs_client.bucket(self.bucket_name)
        blob = staging_bucket.blob(dest_key)

        one_hour_in_seconds = 60 * 60
        one_hundred_milliseconds = 0.1
        return self._assert_file_uploaded(blob, one_hundred_milliseconds, one_hour_in_seconds)

    def _assert_file_uploaded(self, blob: storage.Blob, sleep_time: float, max_sleep_time: float):
        if sleep_time > max_sleep_time:
            raise UploadPollingException(f'Could not verify completed upload for blob {blob.name} within maximum '
                                         f'wait time of {str(max_sleep_time)} seconds')
        else:
            sleep(sleep_time)
            blob.reload()

            export_completed = blob.metadata is not None and blob.metadata.get("export_completed")
            if export_completed:
                return
            else:
                new_sleep_time = sleep_time * 2
                self.logger.info(f'Verifying upload of blob {blob.name}. Waiting for {str(new_sleep_time)} seconds...')
                return self._assert_file_uploaded(blob, new_sleep_time, max_sleep_time)


class DcpStagingException(Exception):
    pass


class DcpStagingClient:

    def __init__(self, boto_session: Session, gcs_storage: GcsStorage, schema_service: SchemaService):
        self.boto_session = boto_session
        self.gcs_storage = gcs_storage
        self.schema_service = schema_service

    def write_metadatas(self, metadatas: Iterable[MetadataResource]):
        for metadata in metadatas:
            self.write_metadata(metadata)

    def write_metadata(self, metadata: MetadataResource):
        dest_object_key = f'metadata/{metadata.concrete_type()}/{metadata.uuid}_{metadata.dcp_version}.json'

        metadata_json = metadata.get_content(with_provenance=True)
        data_stream = DcpStagingClient.dict_to_json_stream(metadata_json)
        self.write_to_staging_bucket(dest_object_key, data_stream)

        if metadata.metadata_type == "file":
            self.write_file_descriptor(metadata)

    def write_links(self, link_set: LinkSet, experiment_uuid: str, experiment_version: str, project_uuid: str):
        dest_object_key = f'links/{experiment_uuid}_{experiment_version}_{project_uuid}.json'
        links_json = self.generate_links_json(link_set)
        data_stream = DcpStagingClient.dict_to_json_stream(links_json)
        self.write_to_staging_bucket(dest_object_key, data_stream)

    def write_file_descriptor(self, file_metadata: MetadataResource):
        dest_object_key = f'descriptors/{file_metadata.concrete_type()}/{file_metadata.uuid}_{file_metadata.dcp_version}.json'
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

    def write_data_files(self, data_files: Iterable[DataFile]):
        for data_file in data_files:
            self.write_data_file(data_file)

    def write_data_file(self, data_file: DataFile):
        dest_object_key = DcpStagingClient.data_file_obj_key(data_file)
        if self.gcs_storage.file_exists(dest_object_key):
            return
        else:
            s3_object_stream = open(data_file.cloud_url, 'rb', transport_params=dict(session=self.boto_session), ignore_ext=True)
            self.write_to_staging_bucket(dest_object_key, s3_object_stream)

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
    def data_file_obj_key(data_file: DataFile) -> str:
        return f'data/{data_file.uuid}_{data_file.dcp_version}_{data_file.file_name}'

    class Builder:
        def __init__(self):
            self.gcs_storage = None
            self.session = None
            self.schema_service = None

        def with_gcs_info(self, service_account_credentials_path: str, gcp_project: str, bucket_name: str, bucket_prefix: str) -> 'DcpStagingClient.Builder':

            with open(service_account_credentials_path) as source:
                info = json.load(source)
                storage_credentials: Credentials = Credentials.from_service_account_info(info)
                gcs_client = storage.Client(project=gcp_project, credentials=storage_credentials)
                self.gcs_storage = GcsStorage(gcs_client, bucket_name, bucket_prefix)

                return self

        def aws_access_key(self, aws_access_key_id: str, aws_access_key_secret: str) -> 'DcpStagingClient.Builder':
            self.session = boto3.Session(aws_access_key_id=aws_access_key_id,
                                         aws_secret_access_key=aws_access_key_secret)
            return self

        def with_schema_service(self, schema_service: SchemaService) -> 'DcpStagingClient.Builder':
            self.schema_service = schema_service
            return self

        def build(self) -> 'DcpStagingClient':
            if not self.gcs_storage:
                raise Exception("gcs_storage must be set")
            elif not self.session:
                raise Exception("s3_client must be set")
            elif not self.schema_service:
                raise Exception("schema_service must be set")
            else:
                return DcpStagingClient(self.session, self.gcs_storage, self.schema_service)

