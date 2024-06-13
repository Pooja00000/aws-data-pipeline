import boto3
import json
import time

def upload_file_to_s3(file_name, bucket, object_name=None):
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name or file_name)
        print(f"File {file_name} uploaded to S3 bucket {bucket}.")
    except Exception as e:
        print(f"Error uploading file: {e}")
        return False
    return True

def trigger_lambda(function_name, payload):
    lambda_client = boto3.client('lambda')
    try:
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='Event',
            Payload=json.dumps(payload)
        )
        print(f"Lambda function {function_name} triggered.")
    except Exception as e:
        print(f"Error invoking lambda function: {e}")
        return False
    return True

def start_glue_job(job_name):
    glue_client = boto3.client('glue')
    try:
        response = glue_client.start_job_run(JobName=job_name)
        job_run_id = response['JobRunId']
        print(f"Glue job {job_name} started with run ID {job_run_id}.")
        return job_run_id
    except Exception as e:
        print(f"Error starting Glue job: {e}")
        return None

def wait_for_glue_job_completion(job_name, job_run_id):
    glue_client = boto3.client('glue')
    while True:
        response = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
        status = response['JobRun']['JobRunState']
        print(f"Current status of Glue job run {job_run_id}: {status}")
        if status in ['SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT']:
            break
        time.sleep(30)  # Wait for 30 seconds before checking again

    if status == 'SUCCEEDED':
        print(f"Glue job {job_name} completed successfully.")
    else:
        print(f"Glue job {job_name} failed with status {status}.")

if _name_ == "_main_":
    # Configuration
    file_name = 'data.json'
    s3_bucket_name = 'your-s3-bucket-name'
    lambda_function_name = 'your-lambda-function-name'
    glue_job_name = 'your-glue-job-name'

    # Upload file to S3
    if upload_file_to_s3(file_name, s3_bucket_name):
        # Optionally trigger Lambda function manually (if needed for testing)
        payload = {
            'Records': [{
                's3': {
                    'bucket': {'name': s3_bucket_name},
                    'object': {'key': file_name}
                }
            }]
        }
        trigger_lambda(lambda_function_name, payload)

        # Start Glue job
        job_run_id = start_glue_job(glue_job_name)
        if job_run_id:
            # Wait for Glue job completion
            wait_for_glue_job_completion(glue_job_name, job_run_id)