from cloud.basic_s3_functions import read_file_from_s3_object
from io import BytesIO
from globalresources.readredirect import read_and_redirect
import boto3
from variables import depara_config
from variables import kafka_config
s3 = boto3.client('s3')
bucket = "sftp-telecom-test"
response = s3.list_objects_v2(Bucket=bucket, Prefix=f"rbx_ContratosMotivos/")
folder_array = [obj['Key'] for obj in response['Contents']]
file_array = [i for i in folder_array if i[-1] != '/']
for key in file_array:
    file = read_file_from_s3_object(bucket, key)
    buffer = BytesIO(file)
    read_and_redirect(bucket, buffer, key, depara_config, kafka_config)
    break
