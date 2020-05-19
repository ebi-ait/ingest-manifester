from ingest.api.ingestapi import IngestApi


class TerraExporter:
    def __init__(self, ingest_client: IngestApi):
        self.ingest_client = ingest_client

    def export(self, process_uuid):
        pass
