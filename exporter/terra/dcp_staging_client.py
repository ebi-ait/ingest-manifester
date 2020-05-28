from exporter.metadata import MetadataResource, DataFile
from exporter.graph.experiment_graph import LinkSet
from exporter.schema import SchemaResource, SchemaService
from typing import Iterable, IO, Dict, Any, Union

from copy import deepcopy
from io import StringIO, BufferedReader

from google.cloud import storage
from mypy_boto3_s3 import S3Client
from mypy_boto3_s3.type_defs import GetObjectOutputTypeDef as S3Object
import boto3
import json


Streamable = Union[BufferedReader, StringIO, IO[Any]]


class DcpStagingException(Exception):
    pass


class DcpStagingClient:

    def __init__(self, s3_client: S3Client, gcs_client: storage.Client, schema_service: SchemaService):
        self.s3_client = s3_client
        self.gcs_client = gcs_client
        self.schema_service = schema_service

    def write_metadatas(self, metadatas: Iterable[MetadataResource]):
        for metadata in metadatas:
            self.write_metadata(metadata)

    def write_metadata(self, metadata: MetadataResource):
        dest_object_key = f'metadata/{metadata.concrete_type()}/{metadata.uuid}_{metadata.dcp_version}.json'

        if metadata.metadata_type != "file":
            metadata_json = metadata.get_content(with_provenance=True)
            data_stream = DcpStagingClient.dict_to_json_stream(metadata_json)
            self.write_to_staging_bucket(dest_object_key, data_stream)
        else:
            converted_file_metadata = DcpStagingClient.convert_file_metadata(metadata)
            data_stream = DcpStagingClient.dict_to_json_stream(converted_file_metadata.get_content())
            self.write_to_staging_bucket(dest_object_key, data_stream)

    def write_links(self, link_set: LinkSet, experiment_uuid: str, experiment_version: str, project_uuid: str):
        dest_object_key = f'links/{experiment_uuid}_{experiment_version}_{project_uuid}.json'
        links_json = self.generate_links_json(link_set)
        data_stream = DcpStagingClient.dict_to_json_stream(links_json)
        self.write_to_staging_bucket(dest_object_key, data_stream)

    def write_data_files(self, data_files: Iterable[DataFile]):
        for data_file in data_files:
            self.write_data_file(data_file)

    def write_data_file(self, data_file: DataFile):
        dest_object_key = f'data/{data_file.uuid}_{data_file.dcp_version}_{data_file.file_name}'
        source_bucket = data_file.source_bucket()
        source_key = data_file.source_key()

        s3_object = self.s3_client.get_object(Bucket=source_bucket, Key=source_key)
        s3_object_stream = DcpStagingClient.s3_download_stream(s3_object)

        self.write_to_staging_bucket(dest_object_key, s3_object_stream)

    def write_to_staging_bucket(self, object_key: str, data_stream: Streamable):
        staging_bucket: storage.Bucket = self.gcs_client.bucket("test-ingest-terra", "terra-ingest")
        blob: storage.Blob = staging_bucket.blob(object_key)
        if not blob.exists():
            blob.upload_from_file(data_stream)

    def generate_links_json(self, link_set: LinkSet) -> Dict:
        latest_links_schema = self.schema_service.latest_links_schema()

        links_json = link_set.to_dict()
        links_json["describedBy"] = latest_links_schema.schema_url
        links_json["schema_version"] = latest_links_schema.schema_version
        links_json["schema_type"] = "links"

        return links_json

    @staticmethod
    def dict_to_json_stream(d: Dict) -> StringIO:
        return StringIO(json.dumps(d))

    @staticmethod
    def s3_download_stream(s3_object: S3Object) -> Streamable:
        """
        The boto3 StreamingBody isn't really a file-like stream as purported in the documentation.
        This function returns a BufferedReader using the underlying protected _raw_stream of a StreamingBody
        :param s3_object:
        :return: the S3 Object data as a BufferedReader stream
        """
        return BufferedReader(s3_object["Body"]._raw_stream, buffer_size=8192)  # 8kb

    @staticmethod
    def convert_file_metadata(file_metadata: MetadataResource) -> MetadataResource:
        """
        This function is file-schema aware, and must transform the filename property to
        the form of {dataFileUuid}_{dataFileVersion}_{filename}.

        The dataFileVersion isn't defined in ingest, so we'll re-use the dcpVersion
        of the file_metadata here
        :return: DCPv2 MVP-ready file metadata
        """
        if file_metadata.full_resource is not None:
            data_file_uuid = file_metadata.full_resource["dataFileUuid"]
            data_file_version = file_metadata.dcp_version
            filename = file_metadata.full_resource["content"]["file_core"]["file_name"]

            new_file_metadata = deepcopy(file_metadata)
            new_name = f'{data_file_uuid}_{data_file_version}_{filename}'
            new_file_metadata.full_resource = file_metadata.full_resource
            new_file_metadata.full_resource["content"]["file_core"]["file_name"] = new_name

            return new_file_metadata
        else:
            raise DcpStagingException(f'Error: File metadata must contain full resource information (dataFileUuid in '
                                      f'particular) for generating valid DCP2 filenames. Metadata resource:\n\n '
                                      f'{file_metadata.metadata_json}')

    class Builder:
        def __init__(self):
            self.gcs_client = None
            self.s3_client = None
            self.schema_service = None

        def with_gcs_service_credentials(self, service_account_credentials_path: str, gcp_project: str):
            self.gcs_client = storage.Client.from_service_account_json(service_account_credentials_path, project=gcp_project)
            return self

        def aws_access_key(self, aws_access_key_id: str, aws_access_key_secret: str):
            self.s3_client = boto3.client('s3',
                                          aws_access_key_id=aws_access_key_id,
                                          aws_secret_access_key=aws_access_key_secret,
                                          region_name="us-east-1"
                                          )
            return self

        def with_schema_service(self, schema_service: SchemaService):
            self.schema_service = schema_service
            return self

        def build(self) -> 'DcpStagingClient':
            if not self.gcs_client:
                raise Exception("gcs_client must be set")
            elif not self.s3_client:
                raise Exception("s3_client must be set")
            elif not self.schema_service:
                raise Exception("schema_service must be set")
            else:
                return DcpStagingClient(self.s3_client, self.gcs_client, self.schema_service)

