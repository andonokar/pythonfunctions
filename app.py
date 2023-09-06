import io
from globalresources.readredirect import read_and_redirect
from cloud.basic_s3_functions import read_file_from_s3_object


def handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    table_name = key.split('/')
    print(table_name[0])
    print(f'{bucket}/{key}')
    file = read_file_from_s3_object(bucket, key)
    buffer = io.BytesIO(file)
    read_and_redirect(bucket, buffer, key)
