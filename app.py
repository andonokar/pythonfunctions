import io
from globalresources.readredirect import read_and_redirect
from cloud.basic_s3_functions import read_file_from_s3_object
from variables import depara_config, auth_config, kafka_config
from authentication.cognito import cognito_aut


@cognito_aut(auth_config)
def handler(event, context):
    """main lambda function"""
    # getting the file bucket
    bucket = event['Records'][0]['s3']['bucket']['name']
    # getting the file key
    key = event['Records'][0]['s3']['object']['key']
    # printing the file path
    print(f'{bucket}/{key}')
    # reading the file in s3
    file = read_file_from_s3_object(bucket, key)
    # buffering the file for the extraction
    buffer = io.BytesIO(file)
    # main function
    read_and_redirect(bucket, buffer, key, depara_config, kafka_config)
