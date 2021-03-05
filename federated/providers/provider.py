#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import abc
import base64
import subprocess
from urllib.parse import urlsplit


class Provider(metaclass=abc.ABCMeta):
    source_cmd = '/usr/src/entrypoint.sh /usr/src/source.sh'
    target_cmd = '/usr/src/entrypoint.sh /usr/src/target.sh'

    def __init__(self, conf):
        for req in ['Platform', 'image', 'input', 'output']:
            assert req in conf, '{} has not been configured'.format(req)
        self.conf = conf
        self.image = conf['image']

    @abc.abstractmethod
    def run_source(self):
        raise NotImplementedError

    @abc.abstractmethod
    def run_target(self):
        raise NotImplementedError

    @abc.abstractmethod
    def download_blobs(self, bucket_path: str, file_path: str):
        raise NotImplementedError

    @abc.abstractmethod
    def upload_blobs(self, file_path: str, bucket_path: str):
        raise NotImplementedError

    # @abc.abstractmethod
    # def generate_signed_url(self, bucket: str, blob_path: str, **kwargs):
    #     raise NotImplementedError

    def split_bucket_object(self, path: str) -> (str, str):
        output1 = urlsplit(path)
        if not output1.netloc:
            bucket = output1.path.split('/')[0]
            object_path = '/'.join(output1.path.split('/')[1:])
        else:
            bucket = output1.netloc
            object_path = output1.path

        return bucket, object_path

    @staticmethod
    def b64_encode(string: str) -> str:
        encoding = 'ascii'
        return base64.b64encode(string.encode(encoding)).decode(encoding)

    # def get_signed_url_cmd_b64(self, ):
    #     assert 'output' in self.conf, 'output is missing from conf'
    #
    #     bucket, path = self.split_bucket_object(self.conf['output'])
    #     signed_url = self.generate_signed_url(bucket, path)
    #
    #     return self.b64_encode(signed_url)

    @staticmethod
    def exec_cmd(cmd: str):
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, universal_newlines=True)
        out, err = p.communicate()
        if err is not None:
            raise ChildProcessError('There was an error executing cmd: {}'.format(cmd))
        return out.strip()
