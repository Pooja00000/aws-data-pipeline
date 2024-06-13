import json
import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection

ES_Host = 'your-es-endpoint'
ES_port = '443'

def lambda_handler(event, contest):
    s3_client = boto3.client['s3']
    for record in event('Records'):
        bucket_name = record['s3']['bucket']['name']
        object_key = record['s3']['object']['key']

        response = s3_client.get_object(bucket=bucket_name, Key=object_key)
        content = response(Body).read().decode('utf-8')

        load_data_into_elasticesearch(content)

    return {
        'status_Code': 200,
        'body': json.dumps('data processed successfully')
    }

def load_data_into_elasticesearch(data):
    es = Elasticsearch(
        hosts = [{'host, ESHOST, post:ESPORT'}],
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

    index_name = 'search engine data'
    es.index(index= index_name, doc_type='_doc', body=json.loads(data))         
