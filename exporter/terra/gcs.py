import googleapiclient.discovery
from dataclasses import dataclass
from typing import Dict
from exporter.metadata import DataFile
from datetime import datetime
import json
import time
from google.oauth2.service_account import Credentials


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
            if not response.get("operations"):
                raise Exception(f'No transfer operations for transferJob {job_name}')
            else:
                try:
                    transfer_operations = response["operations"]
                    transfer_operation = transfer_operations[0]
                    if transfer_operation.get("done"):
                        return
                    else:
                        time.sleep(wait_time)
                        return self._assert_file_transferred(job_name, wait_time * 2, time_waited + wait_time, max_wait_time_secs)
                except (KeyError, IndexError) as e:
                    raise Exception(f'Failed to parse transferOperations') from e

    def create_transfer_client(self):
        return googleapiclient.discovery.build('storagetransfer', 'v1', credentials=self.credentials)

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
