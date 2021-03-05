from sys import implementation
import googleapiclient.discovery
from typing import IO, Dict, Any, Union, Optional
from exporter.metadata import DataFile
from datetime import datetime
import time

from google.cloud import storage
from google.oauth2.service_account import Credentials
from google.api_core.exceptions import PreconditionFailed, ServiceUnavailable
from google.api_core import retry
import json
import logging
from io import StringIO, BufferedReader

from time import sleep
from dataclasses import dataclass
from googleapiclient.errors import HttpError


@dataclass
class TransferJobSpec:
    name: str
    description: str
    project_id: str
    source_bucket: str
    source_path: str
    aws_access_key_id: str
    aws_access_key_secret: str
    dest_bucket: str
    dest_path: str
    gcs_notification_topic: str

    def to_dict(self) -> Dict:
        start_date = datetime.now()
        return {
            'name': self.name,
            'description': self.description,
            'status': 'ENABLED',
            'projectId': self.project_id,
            'schedule': {
                'scheduleStartDate': {
                    'day': start_date.day,
                    'month': start_date.month,
                    'year': start_date.year
                },
                'scheduleEndDate': {
                    'day': start_date.day,
                    'month': start_date.month,
                    'year': start_date.year
                }
            },
            'transferSpec': {
                'awsS3DataSource': {
                    'bucketName': self.source_bucket,
                    'awsAccessKey': {
                        'accessKeyId': self.aws_access_key_id,
                        'secretAccessKey': self.aws_access_key_secret
                    },
                    'path': self.source_path
                },
                'gcsDataSink': {
                    'bucketName': self.dest_bucket,
                    'path': self.dest_path
                },
                'transferOptions': {
                    'overwriteObjectsAlreadyExistingInSink': False
                }
            }#,
            #'notificationConfig': {
            #    'pubsubTopic': self.gcs_notification_topic,
            #    'payloadFormat': 'JSON'
            #}
        }


class GcsXferStorage:

    def __init__(self, aws_access_key_id: str, aws_access_key_secret: str, project_id: str, gcs_dest_bucket: str, gcs_dest_prefix: str, credentials: Credentials, gcs_notification_topic: str):
        self.aws_access_key_id = aws_access_key_id
        self.aws_access_key_secret = aws_access_key_secret
        self.project_id = project_id
        self.gcs_dest_bucket = gcs_dest_bucket
        self.gcs_bucket_prefix = gcs_dest_prefix
        self.credentials = credentials
        self.gcs_notification_topic = gcs_notification_topic

        self.client = self.create_transfer_client()
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

    def transfer_upload_area(self, source_bucket: str, upload_area_key: str, project_uuid: str, export_job_id: str):
        transfer_job = self.transfer_job_for_upload_area(source_bucket, upload_area_key, project_uuid, export_job_id)

        try:
            maybe_existing_job = self.get_job(transfer_job.name)
            if maybe_existing_job is not None:
                self.assert_job_complete(transfer_job.name)
            else:
                self.client.transferJobs().create(body=transfer_job.to_dict()).execute()
                self.assert_job_complete(transfer_job.name)
        except HttpError as e:
            if e.resp.status == 409:
                self.assert_job_complete(transfer_job.name)
            else:
                raise

    def transfer_job_for_upload_area(self, source_bucket: str, upload_area_key: str, project_uuid: str, export_job_id: str) -> TransferJobSpec:
        return TransferJobSpec(name=f'transferJobs/{export_job_id}',
                               description=f'Transfer job for ingest upload-service area {upload_area_key} and export-job-id {export_job_id}',
                               project_id=self.project_id,
                               source_bucket=source_bucket,
                               source_path=f'{upload_area_key}/',
                               aws_access_key_id=self.aws_access_key_id,
                               aws_access_key_secret=self.aws_access_key_secret,
                               dest_bucket=self.gcs_dest_bucket,
                               dest_path=f'{self.gcs_bucket_prefix}/{project_uuid}/data/',
                               gcs_notification_topic=self.gcs_notification_topic)

    def get_job(self, job_name: str) -> Optional[Dict]:
        two_seconds = 2
        two_minutes_in_seconds = 60 * 2
        return self._get_job(job_name, two_seconds, 0, two_minutes_in_seconds)

    def _get_job(self, job_name: str, wait_time: int, time_waited: int, max_wait_time_secs: int) -> Optional[Dict]:
        if time_waited > max_wait_time_secs:
            raise Exception(f'Timeout trying to read transfer job {job_name}')
        else:
            try:
                return self.client.transferJobs().get(jobName=job_name,
                                                                    projectId=self.project_id).execute()
            except HttpError as e:
                if e.resp.status == 404:
                    return None
                elif e.resp.status == 429:
                    self.logger.info(f'Rate-limited for request to read transferJob {job_name}. Waiting {str(wait_time)} seconds.'
                                     f'(total time waited: {str(time_waited)}, max wait time: {str(max_wait_time_secs)} seconds)')
                    time.sleep(wait_time)
                    return self._get_job(job_name, wait_time * 2, time_waited + wait_time, max_wait_time_secs)
                else:
                    raise

    def assert_job_complete(self, job_name: str):
        six_hours_in_seconds = 60 * 60 * 6
        return self._assert_job_complete(job_name, 2, 0, six_hours_in_seconds)

    def _assert_job_complete(self, job_name: str, wait_time: int, time_waited: int, max_wait_time_secs: int):
        if time_waited > max_wait_time_secs:
            raise Exception(f'Timeout waiting for transfer job to succeed for job {job_name}')
        else:
            request = self.client.transferOperations().list(name="transferOperations",
                                                            filter=json.dumps({
                                                                "project_id": self.project_id,
                                                                "job_names": [job_name]
                                                            }))
            response: Dict = request.execute()
            try:
                if response.get("operations") and len(response["operations"]) > 0 and response["operations"][0].get("done"):
                    return
                else:
                    self.logger.info(f'Awaiting complete transfer for job {job_name}. Waiting {str(wait_time)} seconds.'
                                     f'(total time waited: {str(time_waited)}, max wait time: {str(max_wait_time_secs)} seconds)')
                    time.sleep(wait_time)

                    # Wait time is first doubled and then limited to 10 minutes
                    # This is due to the quota limit: https://cloud.google.com/storage-transfer/quotas
                    # Other areas in exporter that use quota are self.get_job and GcsStorage.assert_file_uploaded
                    # This can be tweaked
                    new_wait_time = min(wait_time * 2, 10 * 60)
                    return self._assert_job_complete(job_name, new_wait_time, time_waited + wait_time, max_wait_time_secs)
            except (KeyError, IndexError) as e:
                raise Exception(f'Failed to parse transferOperations') from e

    def create_transfer_client(self):
        return googleapiclient.discovery.build('storagetransfer', 'v1', credentials=self.credentials, cache_discovery=False)


class UploadPollingException(Exception):
    pass


Streamable = Union[BufferedReader, StringIO, IO[Any]]


class GcsStorage:
    def __init__(self, gcs_client: storage.Client, bucket_name: str, storage_prefix: str):
        self.gcs_client = gcs_client
        self.bucket_name = bucket_name
        self.storage_prefix = storage_prefix

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

    def file_exists(self, object_key: str) -> bool:
        dest_key = f'{self.storage_prefix}/{object_key}'
        staging_bucket: storage.Bucket = self.gcs_client.bucket(self.bucket_name)
        blob: storage.Blob = staging_bucket.blob(dest_key)
        if not blob.exists():
            return False
        else:
            blob.reload()
            return blob.metadata is not None and blob.metadata.get("export_completed", False)

    def write(self, object_key: str, data_stream: Streamable):
        try:
            dest_key = f'{self.storage_prefix}/{object_key}'
            staging_bucket: storage.Bucket = self.gcs_client.bucket(self.bucket_name)
            blob: storage.Blob = staging_bucket.blob(dest_key, chunk_size=1024 * 256 * 20)

            if not blob.exists():
                blob.upload_from_file(data_stream, if_generation_match=0)
                self.mark_complete(blob)
            else:
                self.assert_file_uploaded(object_key)
        except PreconditionFailed as e:
            # With if_generation_match=0, this pre-condition failure indicates that another
            # export instance has began uploading this file. We should not attempt to upload
            # and instead poll for its completion
            self.assert_file_uploaded(object_key)

    def move_file(self, source_key: str, object_key: str):
        dest_key = f'{self.storage_prefix}/{object_key}'
        staging_bucket: storage.Bucket = self.gcs_client.bucket(self.bucket_name)
        source_blob: storage.Blob = staging_bucket.blob(source_key)

        new_blob = staging_bucket.rename_blob(source_blob, dest_key)
        self.mark_complete(new_blob)
        return

    def mark_complete(self, blob: storage.Blob):
        blob.metadata = {"export_completed": True}
        patch_retryer = retry.Retry(predicate=retry.if_exception_type(ServiceUnavailable),
                                    deadline=60)

        patch_retryer(lambda: blob.patch())()

    def assert_file_uploaded(self, object_key: str):
        dest_key = f'{self.storage_prefix}/{object_key}'
        staging_bucket: storage.Bucket = self.gcs_client.bucket(self.bucket_name)
        blob = staging_bucket.blob(dest_key)

        one_hour_in_seconds = 60 * 60
        one_hundred_milliseconds = 0.1
        return self._assert_file_uploaded(blob, one_hundred_milliseconds, one_hour_in_seconds)

    def _assert_file_uploaded(self, blob: storage.Blob, sleep_time: float, max_sleep_time: float):
        if sleep_time > max_sleep_time:
            raise UploadPollingException(f'Could not verify completed upload for blob {blob.name} within maximum '
                                         f'wait time of {str(max_sleep_time)} seconds')
        else:
            sleep(sleep_time)
            blob.reload()

            export_completed = blob.metadata is not None and blob.metadata.get("export_completed")
            if export_completed:
                return
            else:
                new_sleep_time = sleep_time * 2
                self.logger.info(f'Verifying upload of blob {blob.name}. Waiting for {str(new_sleep_time)} seconds...')
                return self._assert_file_uploaded(blob, new_sleep_time, max_sleep_time)

