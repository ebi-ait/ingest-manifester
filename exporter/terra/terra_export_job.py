from dataclasses import dataclass
from typing import List, Dict, Set
from ingest.api.ingestapi import IngestApi
import requests

from enum import Enum


@dataclass
class TerraExportError:
    message: str

    def to_dict(self) -> Dict:
        return {
            "message": self.message,
            "errorCode": -1,
            "details": {}
        }


@dataclass
class TerraExportEntity:
    assay_process_id: str
    errors: List[TerraExportError]

    def to_dict(self) -> Dict:
        """
        converts to a JSON as represented in ingest-core API
        """
        return {
            "status": ExportJobState.EXPORTED,
            "context": {
                "assayProcessId": self.assay_process_id
            },
            "errors": [e.to_dict() for e in self.errors]
        }


class ExportJobState(Enum):
    EXPORTING = "EXPORTING"
    EXPORTED = "EXPORTED"
    DEPRECATED = "DEPRECATED"
    FAILED = "FAILED"


@dataclass
class TerraExportJob:
    job_id: str
    num_expected_assays: int
    export_state: ExportJobState

    @staticmethod
    def from_dict(data: Dict) -> 'TerraExportJob':
        job_id = str(data["_link"]["self"]["href"]).split("/")[0]
        num_expected_assays = int(data["context"]["expectedAssays"])
        return TerraExportJob(job_id, num_expected_assays, ExportJobState(data["state"]))


class TerraExportJobService:
    def __init__(self, ingest_client: IngestApi):
        self.ingest_client = ingest_client

    def complete_assay(self, job_id: str, assay_process_id: str):
        assay_export_entity = TerraExportEntity(assay_process_id, [])
        create_export_entity_url = self.get_export_entities_url(job_id)
        requests.post(create_export_entity_url, assay_export_entity.to_dict(), json=True).raise_for_status()
        self._maybe_complete_job(job_id)

    def _maybe_complete_job(self, job_id):
        export_job = self.get_job(job_id)
        if export_job.num_expected_assays == self.get_num_entities_for_job(job_id):
            self.complete_job(job_id)

    def complete_job(self, job_id: str):
        job_url = self.get_job_url(job_id)
        self.ingest_client.patch(job_url, {"status": ExportJobState.EXPORTED.value})

    def get_job_state(self, job_id: str) -> ExportJobState:
        return self.get_job(job_id).export_state

    def get_job(self, job_id: str) -> TerraExportJob:
        job_url = self.get_job_url(job_id)
        return TerraExportJob.from_dict(self.ingest_client.get(job_url).json())

    def get_job_url(self, job_id: str) -> str:
        return self.ingest_client.get_full_url(f'/exportJobs/{job_id}')

    def get_export_entities_url(self, job_id: str):
        return self.ingest_client.get_full_url(f'/exportJobs/{job_id}/entities')

    def get_num_entities_for_job(self, job_id: str) -> int:
        entities_url = self.get_export_entities_url(job_id)
        return int(self.ingest_client.get(entities_url).json()["page"]["totalElements"])
