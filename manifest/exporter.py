#!/usr/bin/env python
import argparse
import logging
import sys

from ingest.api.ingestapi import IngestApi

from manifest.generator import ManifestGenerator


class ManifestExporter:
    def __init__(self, ingest_api: IngestApi, manifest_generator: ManifestGenerator):
        format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        logging.basicConfig(format=format)
        self.logger = logging.getLogger(__name__)
        self.ingest_api = ingest_api
        self.manifest_generator = manifest_generator

    def export(self, process_uuid: str, submission_uuid: str):
        assay_manifest = self.manifest_generator.generate_manifest(process_uuid, submission_uuid)
        assay_manifest_resource = self.ingest_api.create_bundle_manifest(assay_manifest)
        assay_manifest_url = assay_manifest_resource['_links']['self']['href']
        self.logger.info(f"Assay manifest was created: {assay_manifest_url}")


if __name__ == '__main__':
    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=format, stream=sys.stdout, level=logging.INFO)
    p = argparse.ArgumentParser()
    p.add_argument("-s", "--submission_uuid", type=str, help="Please specify the submission UUID")
    p.add_argument("-p", "--process_uuid", type=str, help="Please specify the process UUID")
    p.add_argument("-e", "--env", type=str, help="Please specify the environment where to run the script",
                   choices=["dev", "integration", "staging", "prod"])
    args = p.parse_args()
    process_uuid = args.process_uuid
    submission_uuid = args.submission_uuid

    ingest_api = IngestApi(url=f'https://api.{args.env}.archive.data.humancellatlas.org/')
    exporter = ManifestExporter(ingest_api)
    exporter.export(process_uuid, submission_uuid)