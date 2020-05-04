#!/bin/env python3
import requests
import re
import subprocess
import time
import json


def parse_logs(out_filename, err_filename):
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
        {'rsid': 'rs12913832', 'chr': 15, 'pos': 28365618},
        {'rsid': 'rs1333049', 'chr': 9, 'pos': 22125503},
        {'rsid': 'rs4988235', 'chr': 2, 'pos': 136608646}
    ]
    runs = 3
    query_responses = []

    def server_has_started():
        started_pattern = re.compile(r'.*Started Application in [\d.]+ seconds.*')
        with open('swarm.out') as f:
            lines = f.readlines()
            for line in lines:
                if started_pattern.match(line):
                    return True
        return False

    # Maven build
    maven_proc = subprocess.Popen(['mvn', 'package'], subprocess.PIPE)
    maven_proc.wait()
    if maven_proc.returncode != 0:
        raise RuntimeError(maven_proc.stderr)

    for query in queries:
        for test_run in range(runs):
            # Start server
            with open('swarm.out', 'w') as fout, open('swarm.err', 'w') as ferr:
                server_proc = subprocess.Popen(
                    ['java', '-jar', 'target/swarm-1.0-SNAPSHOT.jar'],
                    # shell=True,
                    stdout=fout,
                    stderr=ferr
                )
                while not server_has_started():
                    print('Waiting for server to start')
                    time.sleep(1)
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
                server_proc.kill()
                print(json.dumps(parse_logs('swarm.out', 'swarm.err'), indent=2))

    with open('allele_count.json', 'w') as fout:
        json.dump(query_responses, fout)


allele_count()
