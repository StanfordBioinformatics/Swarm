import boto3
import json, time
import datetime
now = datetime.datetime.now

client = boto3.client('athena')
chromosomes = [ 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 'X', 'Y']
chromosomes = [str(k) for k in chromosomes]
#TODO temp
#chromosomes = ['1']

with open('column-names.txt') as f:
    col_names_str = f.read().strip()

dest_table = 'swarm.thousandorig_half2_partitioned_bucketed'
src_table = 'swarm.thousandorig_half2_partitioned'

for chrom in chromosomes:
    sql = 'insert into %s(%s) select %s from %s where reference_name = \'%s\'' % (
        dest_table, col_names_str,
        col_names_str.replace('pos', 'cast(pos as bigint)'), src_table,
        chrom)
    print('sql = ' + sql)
    response = client.start_query_execution(
        QueryString=sql,
        ResultConfiguration={
            'OutputLocation': 's3://swarm-aws-athena-results-us-west-1/'
        }
    )
    execution_id = response['QueryExecutionId']
    start = now()
    while True:
        exec_response = client.get_query_execution(QueryExecutionId=execution_id)
        execution = exec_response['QueryExecution']
        state = execution['Status']['State']
        print('state = ' + state)
        duration = now() - start
        if state in ('SUCCEEDED', 'FAILED', 'CANCELLED'):
            # not running
            if state != 'SUCCEEDED':
                raise RuntimeError(exec_response)
            print('copy of %s succeeded (elapsed = %s)' % (chrom, duration.total_seconds()))
            break
        else:
            print('copy of %s still running (elapsed = %s)' % (chrom, duration.total_seconds()))
            time.sleep(5)