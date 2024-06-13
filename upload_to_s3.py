import boto3
def upload_file_to_s3(file_name, bucket, object_name =None):
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except Exception as e:
      print("error uploading file")
      return False
    return True

__name__ == "__main__"
file_name = 'data.json'
bucket_name = 'your_s3_name'
upload_file_to_s3(file_name, bucket_name)