from ingest.api.ingestapi import IngestApi
from typing import Optional, Callable

from cachetools.func import ttl_cache


class SchemaParseException(Exception):
    pass


class SchemaResource:

    def __init__(self, schema_url: str, schema_version: str):
        self.schema_url = schema_url
        self.schema_version = schema_version

    @staticmethod
    def from_dict(data: dict) -> 'SchemaResource':
        try:
            schema_url = data["_links"]["json-schema"]["href"]
            schema_version = data["schemaVersion"]
            return SchemaResource(schema_url, schema_version)
        except (KeyError, TypeError) as e:
            raise SchemaParseException(e)


class SchemaService:

    def __init__(self, ingest_client: IngestApi, ttl: Optional[int] = None):
        self.ingest_client = ingest_client
        self.ttl = ttl if ttl is not None else 600

        self.cached_latest_links_schema = ttl_cache(ttl=self.ttl)(self.latest_links_schema)
        self.cached_latest_file_descriptor_schema = ttl_cache(ttl=self.ttl)(self.latest_file_descriptor_schema)

    def latest_links_schema(self) -> SchemaResource:
        latest_schema = self.ingest_client.get_schemas(
            latest_only=True,
            high_level_entity="system",
            domain_entity="",
            concrete_entity="links"
        )[0]

        return SchemaResource.from_dict(latest_schema)

    def latest_file_descriptor_schema(self) -> SchemaResource:
        try:
            latest_schema = self.ingest_client.get_schemas(
                latest_only=True,
                high_level_entity="system",
                domain_entity="",
                concrete_entity="file_descriptor"
            )[0]

            return SchemaResource.from_dict(latest_schema)
        except IndexError as e:
            raise SchemaParseException(f'Failed to find latest file_descriptor schema')
