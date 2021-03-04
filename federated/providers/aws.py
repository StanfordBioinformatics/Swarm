import logging

from providers.provider import Provider

logger = logging.getLogger('federated')


class AWSProvider(Provider):
    def __init__(self, conf):
        super(AWSProvider, self).__init__(conf)

    def run_source(self):
        pass

    def run_target(self):
        pass

    def download_blobs(self, bucket_path: str, local_path: str):
        pass

    def upload_blobs(self, local_path: str, bucket_path: str):
        pass

    # def generate_signed_url(self, bucket: str, blob_path: str, **kwargs):
    #     expires_in = kwargs.get('expires_in', 14400)
    #     cmd = 'aws s3 presign s3://{0}/{1} --expires-in {}'.format(bucket, blob_path, expires_in)
    #
    #     return cmd
