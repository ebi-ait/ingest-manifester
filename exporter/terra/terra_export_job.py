from dataclasses import dataclass
from typing import List, Dict
from ingest.api.ingestapi import IngestApi
import requests
import json

from enum import Enum
import polling


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
            "status": ExportJobState.EXPORTED.value,
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
    is_data_transfer_complete: bool

    @staticmethod
    def from_dict(data: Dict) -> 'TerraExportJob':
        job_id = str(data["_links"]["self"]["href"]).split("/")[0]
        num_expected_assays = int(data["context"]["totalAssayCount"])
        is_data_transfer_complete = data["context"]["isDataTransferComplete"]
        return TerraExportJob(job_id, num_expected_assays, ExportJobState(data["status"].upper()), is_data_transfer_complete)


class TerraExportJobService:
    def __init__(self, ingest_client: IngestApi):
        self.ingest_client = ingest_client

    def create_export_entity(self, job_id: str, assay_process_id: str):
        assay_export_entity = TerraExportEntity(assay_process_id, [])
        create_export_entity_url = self.get_export_entities_url(job_id)
        requests.post(create_export_entity_url, json.dumps(assay_export_entity.to_dict()),
                      headers={"Content-type": "application/json"}, json=True).raise_for_status()
        self._maybe_complete_job(job_id)

    def _maybe_complete_job(self, job_id):
        export_job = self.get_job(job_id)
        if export_job.num_expected_assays == self.get_num_complete_entities_for_job(job_id):
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

    def get_export_entities_url(self, job_id: str) -> str:
        return self.ingest_client.get_full_url(f'/exportJobs/{job_id}/entities')

    def get_num_complete_entities_for_job(self, job_id: str) -> int:
        entities_url = self.get_export_entities_url(job_id)
        find_entities_by_status_url = f'{entities_url}?status={ExportJobState.EXPORTED.value}'
        return int(self.ingest_client.get(find_entities_by_status_url).json()["page"]["totalElements"])

    def set_data_transfer_complete(self, job_id: str):
        job_url = self.get_job_url(job_id)
        context = self.ingest_client.get(job_url).json()
        context.update({"isDataTransferComplete": True})
        self.ingest_client.patch(job_url, {"context": context})

    def is_data_transfer_complete(self, job_id: str):
        return self.get_job(job_id).is_data_transfer_complete

    def wait_for_data_transfer_to_complete(self, job_id:str):
        try:
            polling.poll(
                lambda: self.is_data_transfer_complete(job_id),
                step=2,
                timeout= 60 * 60 * 6  # TODO get this from env var, should this be the same as GCP?
            )
        except polling.TimeoutException as te:
            raise