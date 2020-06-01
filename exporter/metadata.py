import re
from copy import deepcopy
from typing import List, Dict
from dataclasses import dataclass


class MetadataParseException(Exception):
    pass


class MetadataException(Exception):
    pass


class MetadataProvenance:
    def __init__(self, document_id: str, submission_date: str, update_date: str,
                 schema_major_version: int, schema_minor_version: int):
        self.document_id = document_id
        self.submission_date = submission_date
        self.update_date = update_date
        self.schema_major_version = schema_major_version
        self.schema_minor_version = schema_minor_version

    def to_dict(self):
        return deepcopy(self.__dict__)


class MetadataResource:

    def __init__(self, metadata_type, metadata_json, uuid, dcp_version,
                 provenance: MetadataProvenance, full_resource: dict):
        self.metadata_json = metadata_json
        self.uuid = uuid
        self.dcp_version = dcp_version
        self.metadata_type = metadata_type  # TODO: use an enum type instead of string
        self.provenance = provenance
        self.full_resource = full_resource

    def get_content(self, with_provenance=False) -> Dict:
        content = deepcopy(self.full_resource["content"])
        if with_provenance:
            content["provenance"] = self.provenance.to_dict()
            return content
        else:
            return content

    @staticmethod
    def from_dict(data: dict):
        try:
            metadata_json = data['content']
            uuid = data['uuid']['uuid']
            dcp_version = data['dcpVersion']
            metadata_type = data['type'].lower()
            provenance = MetadataResource.provenance_from_dict(data)
            return MetadataResource(metadata_type, metadata_json, uuid, dcp_version, provenance, full_resource=data)
        except (KeyError, TypeError) as e:
            raise MetadataParseException(e)

    @staticmethod
    def provenance_from_dict(data: dict):
        try:
            uuid = data['uuid']['uuid']
            submission_date = data['submissionDate']
            update_date = data['updateDate']

            # Populate the major and minor schema versions from the URL in the describedBy field
            schema_semver = re.findall(r'\d+\.\d+\.\d+', data["content"]["describedBy"])[0]
            schema_major_version = int(schema_semver.split(".")[0])
            schema_minor_version = int(schema_semver.split(".")[1])

            return MetadataProvenance(uuid, submission_date, update_date, schema_major_version,
                                      schema_minor_version)
        except (KeyError, TypeError) as e:
            raise MetadataParseException(e)

    def concrete_type(self) -> str:
        return self.metadata_json["describedBy"].rsplit('/', 1)[-1]


class MetadataService:

    def __init__(self, ingest_client):
        self.ingest_client = ingest_client

    def fetch_resource(self, resource_link: str) -> MetadataResource:
        raw_metadata = self.ingest_client.get_entity_by_callback_link(resource_link)
        return MetadataResource.from_dict(raw_metadata)

    def get_derived_by_processes(self, experiment_material: MetadataResource) -> List[MetadataResource]:
        return MetadataService.parse_metadata_resources(self.ingest_client.get_related_entities('derivedByProcesses', experiment_material.full_resource, 'processes'))

    def get_derived_biomaterials(self, process: MetadataResource) -> List[MetadataResource]:
        return MetadataService.parse_metadata_resources(self.ingest_client.get_related_entities('derivedBiomaterials', process.full_resource, 'biomaterials'))

    def get_derived_files(self, process: MetadataResource) -> List[MetadataResource]:
        return MetadataService.parse_metadata_resources(self.ingest_client.get_related_entities('derivedFiles', process.full_resource, 'files'))

    def get_input_biomaterials(self, process: MetadataResource) -> List[MetadataResource]:
        return MetadataService.parse_metadata_resources(self.ingest_client.get_related_entities('inputBiomaterials', process.full_resource, 'biomaterials'))

    def get_input_files(self, process: MetadataResource) -> List[MetadataResource]:
        return MetadataService.parse_metadata_resources(self.ingest_client.get_related_entities('inputFiles', process.full_resource, 'files'))

    def get_protocols(self, process: MetadataResource) -> List[MetadataResource]:
        return MetadataService.parse_metadata_resources(self.ingest_client.get_related_entities('protocols', process.full_resource, 'protocols'))

    @staticmethod
    def parse_metadata_resources(metadata_resources: List[Dict]) -> List[MetadataResource]:
        return [MetadataResource.from_dict(m) for m in metadata_resources]


@dataclass
class DataFile:
    uuid: str
    dcp_version: str
    file_name: str
    cloud_url: str

    def source_bucket(self) -> str:
        return self.cloud_url.split("//")[1].split("/")[0]

    def source_key(self) -> str:
        return self.cloud_url.split("//")[1].split("/", 1)[1]

    @staticmethod
    def from_file_metadata(file_metadata: MetadataResource) -> 'DataFile':
        if file_metadata.full_resource is not None:
            return DataFile(file_metadata.full_resource["dataFileUuid"],
                            file_metadata.dcp_version,
                            file_metadata.full_resource["fileName"],
                            file_metadata.full_resource["cloudUrl"])
        else:
            raise MetadataParseException(f'Error: parsing DataFile from file MetadataResources requires non-empty'
                                         f'"full_resource" field. Metadata:\n\n {file_metadata.metadata_json}')

