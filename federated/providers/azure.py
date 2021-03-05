#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import uuid

from batch import instance, scheduler
from providers.provider import Provider

logger = logging.getLogger('federated')


class AzureProvider(Provider):
    def __init__(self, conf):
        super(AzureProvider, self).__init__(conf)
        assert 'storage_account' in conf['Platform'], 'storage account is missing from azure conf'
        assert 'storage_connection_string' in conf['Platform'], 'storage connection string is missing from azure conf'
        self.storage_account = conf['Platform']['storage_account']
        self.connection_string = conf['Platform']['storage_connection_string']
        self.scheduler = self._get_scheduler()

        logging.getLogger('azure.storage').setLevel(logging.WARNING)

    def run_source(self):
        self._run_cmd(self.source_cmd)

    def run_target(self):
        self._run_cmd(self.target_cmd)

    def download_blobs(self, bucket_path: str, local_path: str):
        cmd = self._get_download_blobs_cmd(bucket_path, local_path)
        return self.exec_cmd(cmd)

    def upload_blobs(self, local_path: str, bucket_path: str):
        cmd = self._get_upload_blobs_cmd(local_path, bucket_path)
        return self.exec_cmd(cmd)

    # def generate_signed_url(self, container: str, blob_path: str, **kwargs):
    #     expiry: str = datetime.now().isoformat()
    #
    #     permissions = kwargs.get('permissions', 'r')
    #
    #     cmd = f'az storage blob generate-sas \
    #         --account-name {self.storage_account} \
    #         --container-name {container} \
    #         --name {blob_path} \
    #         --permissions {permissions} \
    #         --expiry {expiry} \
    #         --auth-mode key \
    #         --as-user \
    #         --full-uri'
    #
    #     return cmd

    def _get_download_blobs_cmd(self, bucket_path: str, local_path: str) -> str:
        bucket, blob_path = bucket_path.split('/', 1)
        pattern_base = blob_path.rstrip('/')
        destination = local_path.rstrip(pattern_base) or '.'
        cmd = 'mkdir -p {} && az storage blob download-batch -s {} -d {} --pattern "{}/*" {}'.format(
            local_path, bucket, destination, pattern_base, self._get_acct_args()
        )
        return cmd

    def _get_upload_blobs_cmd(self, local_path: str, bucket_path: str) -> str:
        bucket, blob_path = bucket_path.split('/', 1)
        cmd = 'az storage blob upload-batch -s {} -d {} --destination-path {} {}'.format(
            local_path, bucket, blob_path.rstrip('/'), self._get_acct_args()
        )
        return cmd

    def _get_acct_args(self) -> str:
        cmd = '--account-name {} --connection-string "{}"'.format(
            self.storage_account, self.connection_string
        )
        return cmd

    def _run_cmd(self, cmd):
        self.scheduler.image = self.image
        self.scheduler.task_definition['commandLine'] = cmd

        _, input = self.split_bucket_object(self.conf['input'])
        _, output = self.split_bucket_object(self.conf['output'])
        self.scheduler.task_definition['environmentSettings'] = [
            {
                'name': 'PRE_COMMAND_B64',
                'value': self.b64_encode('mkdir -p {} && {}'.format(
                    output,
                    self._get_download_blobs_cmd(self.conf['input'], input)
                ))
            },
            {
                'name': 'POST_COMMAND_B64',
                'value': self.b64_encode(
                    self._get_upload_blobs_cmd(output, self.conf['output']) + ' || :'
                )
            },
            {
                'name': 'INPUT',
                'value': input
            },
            {
                'name': 'OUTPUT',
                'value': output
            }
        ]
        task_id = 'fed-swarm-{}'.format(uuid.uuid4())
        self.scheduler.task_definition['id'] = task_id
        self.scheduler.task_definition['displayName'] = 'Federated Swarm Task'

        logger.info('Kicking off task %s on Azure batch...', task_id)
        info = self.scheduler.submit_job()

        self.scheduler.wait_for_tasks_to_complete([info['job_id']])

    def _get_scheduler(self):
        instance_name = instance.AzureInstance.machine_thread_mapping[self.conf.get('thread', 2)][-1]
        machine = instance.AzureInstance(self.conf, name=instance_name)
        return scheduler.AzureBatchScheduler(self.conf, machine, 200, None)
