from typing import Iterator


class MockIngestAPI:
    def __init__(self, mock_entity_retriever):
        self.mock_entities = mock_entity_retriever

    def get_entity_by_uuid(self, entity_type, uuid):
        return self.mock_entities.get_entity(entity_type, uuid)

    def get_related_entities(self, relation, entity, entity_type):
        if relation in entity["_links"]:
            base_entity_uri = entity["_links"]["self"]["href"]
            relation_uri = entity["_links"][relation]["href"]
            search_result = self.related_entity_search(base_entity_uri, relation_uri, entity_type)
            if '_embedded' in search_result and entity_type in search_result["_embedded"]:
                return search_result["_embedded"][entity_type]
        return []

    def related_entity_search(self, base_entity_uri, search_uri, related_entity_type) -> dict:
        search_result = IngestEntitySearchResult(related_entity_type, search_uri)
        search_result.add_entities(self.mock_entities.get_related_entities(base_entity_uri, search_uri))
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

    def add_entity(self, entity: dict):
        self.result['_embedded'][self.entity_type].append(entity)
        self.result['page']['totalElements'] = self.result['page']['totalElements'] + 1
        if self.result['page']['totalElements'] > self.result['page']['size']:
            self.result['page']['size'] = self.result['page']['size'] + 20

    def add_entities(self, entities: Iterator[dict]):
        for entity in entities:
            self.add_entity(entity)
