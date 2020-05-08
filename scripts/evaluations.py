#!/bin/env python3
import requests
import re
import subprocess
import time
import json
import copy

class SwarmServer(object):
    @staticmethod
    def build():
        # Maven build
        maven_proc = subprocess.Popen(['mvn', 'package'], subprocess.PIPE)
        maven_proc.wait()
        if maven_proc.returncode != 0:
            raise RuntimeError(maven_proc.stderr)

    def __init__(self):
        # self.file_obj = open(file_name, method)
        self.out_filename = 'swarm.out'
        self.err_filename = 'swarm.err'
        self.jar_filename = 'target/swarm-1.0-SNAPSHOT.jar'

        self.started_poll_interval = 2
        pass

    def server_has_started(self):
        started_pattern = re.compile(r'.*Started Application in [\d.]+ seconds.*')
        with open(self.out_filename) as f:
            lines = f.readlines()
            for line in lines:
                if started_pattern.match(line):
                    return True
        return False

    def process_is_running(self):
        return self.server_proc.poll() is None

    def __enter__(self):
        with open(self.out_filename, 'w') as fout, open(self.err_filename, 'w') as ferr:
            self.server_proc = subprocess.Popen(
                ['java', '-jar', self.jar_filename],
                # shell=True,
                stdout=fout,
                stderr=ferr,
                bufsize=0
            )

            while self.process_is_running() and not self.server_has_started():
                print('Waiting for server to start')
                time.sleep(self.started_poll_interval)
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.server_proc.kill()


def parse_logs(server: SwarmServer):
    out_filename = server.out_filename
    db_names = ['Athena', 'BigQuery']
    times = [0]*len(db_names)
    bytes = [0]*len(db_names)
    with open(out_filename) as f:
        for line in f:
            for db_idx in range(len(db_names)):
                db_name = db_names[db_idx]
                bytes_pattern = re.compile(r".* - %s bytes scanned: ([\d.]+)" % db_name, re.IGNORECASE)
                time_pattern = re.compile(r".* - %s execution time: ([\d.]+)" % db_name, re.IGNORECASE)
                # print('checking line: %s' % line)
                time_match = time_pattern.match(line)
                if time_match:
                    val = float(time_match.group(1))
                    print('%s bytes increased by %f' % (db_name, val))
                    times[db_idx] += val
                bytes_match = bytes_pattern.match(line)
                if bytes_match:
                    val = float(bytes_match.group(1))
                    print('%s bytes increased by %f' % (db_name, val))
                    bytes[db_idx] += val

    ret = {}
    for db_idx in range(len(db_names)):
        db_name = db_names[db_idx]
        ret[db_name] = {
            'time': times[db_idx],
            'bytes': bytes[db_idx]
        }
    return ret


def allele_count():
    queries = [
        {'rsid': 'rs671', 'chr': 12, 'pos': 112241766},
        # {'rsid': 'rs12913832', 'chr': 15, 'pos': 28365618},
        # {'rsid': 'rs1333049', 'chr': 9, 'pos': 22125503},
        # {'rsid': 'rs4988235', 'chr': 2, 'pos': 136608646}
    ]
    runs = 3
    query_responses = []

    # Maven build
    SwarmServer.build()

    for query in queries:
        for test_run in range(runs):
            # Start server
            with SwarmServer() as server:
                url = 'http://localhost:8080/variants?rsid={rsid}&return_results=true'.format(rsid=query['rsid'])
                print('making request: ' + url)
                resp = requests.get(url)
                if resp.status_code != 200:
                    raise RuntimeError('Failed to make query:\n' + resp.content)
                query_responses.append({
                    'rsid': query['rsid'],
                    'chr': query['chr'],
                    'pos': query['pos'],
                    'response': resp.content.decode('utf-8')
                })

            print(json.dumps(parse_logs(server), indent=2))

    with open('allele_count.json', 'w') as fout:
        json.dump(query_responses, fout)


def annotation():
    common = {
        'variantsDatabaseType': 'athena',
        'variantsDatabaseName': 'swarm',
        'variantsTable': 'thousandorig_half2_partitioned_4000',
        'annotationDatabaseType': 'bigquery',
        'annotationDatabaseName': 'swarm',
        'annotationTable': 'hg19_Variant_9B_Table_part',
        'destinationDatabaseType': 'athena',
        'return_results': 'true',
    }
    queries = [
        {'gene': 'APC'},
        {'gene': 'TP53'}
    ]
    runs = 3
    query_responses = []

    # Maven build
    SwarmServer.build()

    for query in queries:
        for test_run in range(runs):
            # Start server
            with SwarmServer() as server:
                url = 'http://localhost:8080/annotate'
                params = copy.copy(common)
                params.update(query)
                print('making request: ' + url + ' params: %s' % query)
                resp = requests.get(url, params)
                if resp.status_code != 200:
                    raise RuntimeError('Failed to make query:\n' + resp.content)
                query_responses.append({
                    'rsid': query['rsid'],
                    'chr': query['chr'],
                    'pos': query['pos'],
                    'response': resp.content.decode('utf-8')
                })

            print(json.dumps(parse_logs(server), indent=2))

    with open('allele_count.json', 'w') as fout:
        json.dump(query_responses, fout)


# allele_count()
annotation()