from Hummingbird import scheduler

from .provider import Provider


class GCPProvider(Provider):
    def __init__(self, conf):
        super(GCPProvider, self).__init__(conf)
        self.tool = 'dsub'
        self.instance = scheduler.GCPInstance()
        bucket, path = self.split_bucket_object(self.conf['output'])
        self.bucket = bucket
        self.path = path

    def run_source(self):
        sched = self._get_scheduler("'{}'".format(self.source_cmd))
        sched.add_argument('--output-recursive', 'OUTPUT2="{}"'.format(self.conf['input']))

        self._run_cmd(sched)

    def run_target(self):
        sched = self._get_scheduler("'{}'".format(self.target_cmd))
        self._run_cmd(sched)

    def download_blobs(self, bucket_path: str, local_path: str):
        cmd = 'gsutil -mq rsync -r {} {}'.format(bucket_path, local_path)
        return self.exec_cmd(cmd)

    def upload_blobs(self, local_path: str, bucket_path: str):
        cmd = 'gsutil -mq rsync -r {} {}'.format(local_path, bucket_path)
        return self.exec_cmd(cmd)

    # def generate_signed_url(self, bucket: str, blob_path: str, **kwargs):
    #     duration = kwargs.get('duration', '2h')
    #     cmd = 'gsutil signurl -d {} -u gs://{}{}/{}'.format(
    #         duration, bucket, blob_path, 'Height.QC.Transformed'
    #     )
    #
    #     cmd = 'ls -laR ~/.config/gcloud/'
    #
    #     return cmd

    @staticmethod
    def _run_cmd(sched: scheduler.Scheduler):
        subprocess = sched.run()
        if subprocess.wait() != 0:
            raise ChildProcessError('There was an issue running dsub task')

    def _get_scheduler(self, cmd):
        assert cmd is not None, 'cmd is required'

        sched = scheduler.Scheduler(self.tool, self.conf)

        sched.add_argument('--command', cmd)
        sched.add_argument('--image', self.conf['Platform'].get('image', self.image))

        pre_cmd = 'cd /mnt/data/input/gs/{}/'.format(self.bucket)
        sched.add_argument('--env', 'PRE_COMMAND_B64={}'.format(self.b64_encode(pre_cmd)))

        sched.add_argument('--input-recursive', 'INPUT={}'.format(self.conf['input']))
        sched.add_argument('--output-recursive', 'OUTPUT="{}"'.format(self.conf['output']))

        if 'logging' in self.conf and self.conf['logging'] is not None:
            sched.add_argument('--logging', self.conf['logging'])
        sched.add_argument('--machine-type', self.instance.name)
        sched.add_argument('--disk-size', '50')
        sched.add_argument('--wait')

        return sched
