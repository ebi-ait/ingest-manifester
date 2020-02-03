#!/usr/bin/env python
import argparse
import logging
import sys

from ingest.api.ingestapi import IngestApi

from exporter.manifest import AssayManifestGenerator


class ManifestExporter:
    def __init__(self, ingest_api: IngestApi):
        format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        logging.basicConfig(format=format)
        self.logger = logging.getLogger(__name__)
        self.ingest_api = ingest_api
        self.assay_manifest_generator = AssayManifestGenerator(ingest_api)

    def export(self, process_uuid: str, submission_uuid: str):
        assay_manifest = self.assay_manifest_generator.generate_manifest(process_uuid, submission_uuid)
        assay_manifest_url = assay_manifest['_links']['self']['href']
        self.logger.info(f"Assay manifest was created: {assay_manifest_url}")


if __name__ == '__main__':
    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=format, stream=sys.stdout, level=logging.INFO)
    p = argparse.ArgumentParser()
    p.add_argument("-i", "--submission_id", type=str, help="Ingest submission ID")
    p.add_argument("-d", "--deployment", type=str, help="Deployment to check.",
                   choices=["dev", "int", "staging", "prod"])

    ingest_api = IngestApi(url='https://api.integration.archive.data.humancellatlas.org/')
    process_uuid = '42799ef8-9f45-401a-bffd-3bcf61eed46c'
    submission_uuid = '6d821229-5591-46d6-abb6-2ac1c35eb18a'

    exporter = ManifestExporter(ingest_api)
    exporter.export_bundle(process_uuid, submission_uuid)
