import time
import boto3

session = boto3.Session(profile_name='stanford')
client = session.client('athena')
with open('partitioned_transfer_template.sql') as fin:
    sql_template = fin.read()

#references = ['1']
references = [
    '2', '3', '4','5','6','7','8','9','10',
    '11','12','13','14','15','16','17','18','19','20','21','22','X','Y']
for ref in references:
    sql = sql_template % ref
    start_ret = client.start_query_execution(
        QueryString=sql,
        ResultConfiguration={
            'OutputLocation': 's3://swarm-aws-athena-results-us-west-1/'
        }
    )
    print(start_ret)
    execution_id = start_ret['QueryExecutionId']
    def wait_for_finish(execution_id):
        # possible states:
        #   'QUEUED'|'RUNNING'|'SUCCEEDED'|'FAILED'|'CANCELLED'
        terminal_states = ['SUCCEEDED', 'FAILED', 'CANCELLED']
        start = time.time()
        while True:
            resp = client.get_query_execution(QueryExecutionId=execution_id)
            query_state = resp['QueryExecution']['Status']['State']
            if query_state in terminal_states:
                return resp
            else:
                print('Reference %s still in state %s (duration=%.2f)' % (
                    ref, query_state, (time.time() - start)
                ))
                time.sleep(5)

    execution_status_resp = wait_for_finish(execution_id)
    print(execution_status_resp['QueryExecution']['Status'])
    print('Finished reference ' + ref)