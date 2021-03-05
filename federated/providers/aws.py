#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging

from batch import scheduler, instance
from providers.provider import Provider

logger = logging.getLogger('federated')


class AWSProvider(Provider):
    def __init__(self, conf):
        super(AWSProvider, self).__init__(conf)
        for req in ['subnets', 'security_groups']:
            assert req in conf['Platform'], 'Platform.{} has not been configured'.format(req)
        machine = instance.AWSInstance(self.conf.get('instance_type', 'r5.large'))
        self.scheduler = scheduler.AWSBatchScheduler(conf, machine, 100)

    def run_source(self):
        self._run_cmd(self.source_cmd)

    def run_target(self):
        self._run_cmd(self.target_cmd)

    def download_blobs(self, bucket_path: str, local_path: str):
        cmd = self._get_sync_cmd(bucket_path, local_path)
        return self.exec_cmd(cmd)

    def upload_blobs(self, local_path: str, bucket_path: str):
        cmd = self._get_sync_cmd(local_path, bucket_path)
        return self.exec_cmd(cmd)

    # def generate_signed_url(self, bucket: str, blob_path: str, **kwargs):
    #     expires_in = kwargs.get('expires_in', 14400)
    #     cmd = 'aws s3 presign s3://{0}/{1} --expires-in {}'.format(bucket, blob_path, expires_in)
    #
    #     return cmd

    def _get_sync_cmd(self, source, dest):
        return 'aws s3 sync {} {} --quiet'.format(source, dest)

    def _run_cmd(self, cmd):
        self.scheduler.image = self.image

        _, input = self.split_bucket_object(self.conf['input'])
        _, output = self.split_bucket_object(self.conf['output'])
        env = [
            {
                'name': 'PRE_COMMAND_B64',
                'value': self.b64_encode('mkdir -p {} {} && {}'.format(
                    output,
                    input,
                    self._get_sync_cmd(self.conf['input'], input)
                ))
            },
            {
                'name': 'POST_COMMAND_B64',
                'value': self.b64_encode(
                    self._get_sync_cmd(output, self.conf['output']) + ' || :'
                )
            },
            {
                'name': 'INPUT',
                'value': input.rstrip('/')
            },
            {
                'name': 'OUTPUT',
                'value': output.rstrip('/')
            }
        ]

        logger.info('Kicking off task on AWS batch...')
        job_id = self.scheduler.submit_job(cmd.split(' '), env)

        logger.info('Waiting for job %s to finish', job_id)
        self.scheduler.wait_jobs([job_id])
