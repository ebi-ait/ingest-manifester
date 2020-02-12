import json
import os
from pathlib import Path


class MockEntityFiles:
    def __init__(self, base_path=None, base_uri=None):
        self.base_path = base_path
        if self.base_path is None:
            self.base_path = os.path.dirname(__file__) + '/ingest-data/'
        self.base_uri = base_uri
        if self.base_uri is None:
            self.base_uri = 'http://mock-ingest-api/'

    def get_entity(self, entity_type, entity_id):
        self_location = Path(self.base_path + entity_type + '/' + entity_id + '/self.json')
        if self_location.exists() and self_location.is_file():
            return self.load_json_file(self_location)
        location = Path(self.base_path + entity_type + '/' + entity_id + '.json')
        if location.exists() and location.is_file():
            return self.load_json_file(location)

    def get_related_entities(self, base_entity_uri, relation_endpoint) -> list:
        base_type_id = base_entity_uri.replace(self.base_uri, '')
        base_type = base_type_id.split('/')[0]
        base_id = base_type_id.split('/')[1]
        relation_name = str.strip(relation_endpoint.replace(base_entity_uri, ''), '/')

        full_path = Path(self.base_path + base_type + '/' + base_id + '/' + relation_name)
        if full_path.exists() and full_path.is_file():
            return self.get_entities_from_file(full_path)
        if full_path.exists() and full_path.is_dir():
            return self.load_json_files_from_dir(full_path)
        return []

    def get_entities_from_file(self, location: Path) -> list:
        full_path = Path(location)
        if full_path.exists() and full_path.is_file():
            with full_path.open('r') as file:
                for line in file.readlines():
                    if line.find('/') > 0:
                        yield self.get_entity(line.split('/')[0].strip(), line.split('/')[1].strip())

    @staticmethod
    def load_json_file(location: Path):
        file_path = Path(location)
        if file_path.exists() and file_path.is_file():
            with file_path.open('rb') as file:
                return json.load(file)

    @staticmethod
    def load_json_files_from_dir(location: Path) -> list:
        if location.exists() and location.is_dir():
            files_in_path = (entry for entry in location.iterdir() if entry.is_file())
            for file_path in files_in_path:
                yield MockEntityFiles.load_json_file(file_path)
