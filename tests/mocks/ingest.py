import json
import os
from pathlib import Path

from ingest.api.ingestapi import IngestApi


class MockIngestAPI(IngestApi):
    def __init__(self, base_path=None):
        self.base_path = None
        if base_path is not None:
            test_path = Path(base_path)
            if test_path.exists():
                self.base_path = base_path

        if self.base_path is None:
            self.base_path = os.path.dirname(__file__)

    def get_entity_by_uuid(self, entity_type, uuid):
        return self._load_entity_from_file(entity_type, uuid)

    def _load_entity_from_file(self, entity_type, entity_id):
        directory_path = Path(self.base_path + '/ingest-data/' + entity_type + '/' + entity_id + '/self.json')
        file_path = Path(self.base_path + '/ingest-data/' + entity_type + '/' + entity_id + '.json')
        if directory_path.exists() and directory_path.is_file():
            with directory_path.open('rb') as file:
                return json.load(file)
        elif file_path.exists() and file_path.is_file():
            with file_path.open('rb') as file:
                return json.load(file)
        return None

    def get_related_entities(self, relation, entity, entity_type):
        if relation in entity["_links"]:
            base_entity_uri = entity["_links"]["self"]["href"]
            relation_uri = entity["_links"][relation]["href"]
            result = self.search_related_entities(base_entity_uri, relation_uri, relation_type=entity_type)
            if '_embedded' in result and entity_type in result["_embedded"]:
                return result["_embedded"][entity_type]
        return []

    def search_related_entities(self, base_uri, relation_uri, relation_type):
        base_type_id = base_uri.replace('http://mock-ingest-api/', '')
        base_type = base_type_id.split('/')[0]
        base_id = base_type_id.split('/')[1]
        relation_name = str.strip(relation_uri.replace(base_uri, ''), '/')

        full_path = Path(self.base_path + '/ingest-data/' + base_type + '/' + base_id + '/' + relation_name)
        search_result = IngestEntitySearchResult(relation_type, relation_uri)
        if full_path.exists():
            if full_path.is_file():
                with full_path.open('r') as file:
                    for line in file.readlines():
                        if line.find('/'):
                            line_type = line.split('/')[0].strip()
                            line_id = line.split('/')[1].strip()
                            search_result.add_entity(self.get_entity_by_uuid(line_type, line_id))
            elif full_path.is_dir():
                files_in_path = (entry for entry in full_path.iterdir() if entry.is_file())
                for file_path in files_in_path:
                    with file_path.open('rb') as file:
                        search_result.add_entity(json.load(file))
        return search_result.result


class IngestEntitySearchResult:
    def __init__(self, entity_type: str, self_link: str):
        self.entity_type = entity_type
        self.result = {
            "_embedded": {
                entity_type: []
            },
            "_links": {
                "self": {
                    "href": self_link
                }
            },
            "page": {
                "size": 20,
                "totalElements": 0,
                "totalPages": 1,
                "number": 1
            }
        }

    def add_entity(self, entity):
        self.result['_embedded'][self.entity_type].append(entity)
        self.result['page']['totalElements'] = self.result['page']['totalElements'] + 1
        if self.result['page']['totalElements'] > self.result['page']['size']:
            self.result['page']['size'] = self.result['page']['size'] + 20
