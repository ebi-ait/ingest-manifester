from exporter.metadata import MetadataResource, DataFile
from exporter.graph.experiment_graph import LinkSet
from typing import Iterable

from copy import deepcopy


class GcsStagingException(Exception):
    pass


class GcsStagingClient:

    def __init__(self):
        pass

    def write_metadatas(self, metadatas: Iterable[MetadataResource]):
        for metadata in metadatas:
            self.write_metadata(metadata)

    def write_metadata(self, metadata: MetadataResource):
        object_key = f'metadata/{metadata.concrete_type()}/{metadata.uuid}_{metadata.dcp_version}.json'
        if metadata.metadata_type == "file":
            pass
        else:
            pass

    def write_links(self, link_set: LinkSet, experiment_uuid: str, experiment_version: str):
        object_key = f'links/{experiment_uuid}_{experiment_version}.json'
        pass

    def write_data_files(self, data_files: Iterable[DataFile]):
        for data_file in data_files:
            self.write_data_file(data_file)

    def write_data_file(self, data_file: DataFile):
        object_key = f'data/{data_file.uuid}_{data_file.dcp_version}_{data_file.file_name}'
        pass

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
            raise GcsStagingException(f'Error: File metadata must contain full resource information (dataFileUuid in '
                                      f'particular) for generating valid DCP2 filenames. Metadata resource:\n\n '
                                      f'{file_metadata.metadata_json}')
