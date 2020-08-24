import googleapiclient.discovery
from typing import IO, Dict, Any, Union
from exporter.metadata import DataFile
from datetime import datetime
import time

from google.cloud import storage
from google.oauth2.service_account import Credentials
from google.api_core.exceptions import PreconditionFailed
import json
import logging
from io import StringIO, BufferedReader

from time import sleep
from dataclasses import dataclass


@dataclass
class TransferJobSpec:
    name: str
    description: str
    project_id: str
    source_bucket: str
    source_key: str
    dest_bucket: str
    aws_access_key_id: str
    aws_access_key_secret: str

    def to_dict(self) -> Dict:
        start_date = datetime.now()
        return {
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
                    }
                },
                'gcsDataSink': {
                    'bucketName': self.dest_bucket
                },
                'objectConditions': {
                    'includePrefixes': [self.source_key]
                }
            }
        }


class GcsXferStorage:

    def __init__(self, aws_access_key_id: str, aws_access_key_secret: str, project_id: str, gcs_dest_bucket: str, gcs_dest_prefix: str, credentials: Credentials):
        self.aws_access_key_id = aws_access_key_id
        self.aws_access_key_secret = aws_access_key_secret
        self.project_id = project_id
        self.gcs_dest_bucket = gcs_dest_bucket
        self.gcs_bucket_prefix = gcs_dest_prefix
        self.credentials = credentials

        self.client = self.create_transfer_client()

    def transfer(self,  data_file: DataFile):
        transfer_job = self.create_transfer_job(data_file)
        job_name = self.client.transferJobs().create(body=transfer_job.to_dict()).execute()["name"]
        self.assert_file_transferred(job_name)

    def assert_file_transferred(self, job_name):
        two_seconds = 2
        one_hour_in_seconds = 60 * 60
        return self._assert_file_transferred(job_name, two_seconds, 0, one_hour_in_seconds)

    def _assert_file_transferred(self, job_name: str, wait_time: int, time_waited: int, max_wait_time_secs: int):
        if time_waited > max_wait_time_secs:
            raise Exception(f'Timeout waiting for transfer job to success for job {job_name}')
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
                    time.sleep(wait_time)
                    return self._assert_file_transferred(job_name, wait_time * 2, time_waited + wait_time, max_wait_time_secs)
            except (KeyError, IndexError) as e:
                raise Exception(f'Failed to parse transferOperations') from e

    def create_transfer_client(self):
        return googleapiclient.discovery.build('storagetransfer', 'v1', credentials=self.credentials, cache_discovery=False)

    @staticmethod
    def transfer_job_name(data_file: DataFile) -> str:
        return data_file.checksums.sha256

    def create_transfer_job(self, data_file: DataFile) -> TransferJobSpec:
        return TransferJobSpec(name=self.transfer_job_name(data_file),
                               description=f'HCA data-file: {data_file.uuid}',
                               project_id=self.project_id,
                               source_bucket=data_file.source_bucket(),
                               source_key=data_file.source_key(),
                               dest_bucket=self.gcs_dest_bucket,
                               aws_access_key_id=self.aws_access_key_id,
                               aws_access_key_secret=self.aws_access_key_secret)


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
                blob.metadata = {"export_completed": True}
                blob.patch()
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
        new_blob.metadata = {"export_completed": True}
        new_blob.patch()
        return

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

