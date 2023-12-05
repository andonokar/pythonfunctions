from cloud.basic_s3_functions import read_file_from_s3_object
from io import BytesIO
from globalresources.readredirect import read_and_redirect
import boto3
from variables import depara_config
from variables import kafka_config
from variables import auth_config
import sys
from authentication.cognito import cognito_aut


@cognito_aut(auth_config)
def main():
    s3 = boto3.client('s3')
    if len(sys.argv) != 1:
        print("Usage: e2etest.py bucket_name")
        sys.exit(1)
    bucket = sys.argv[0]
    response = s3.list_objects_v2(Bucket=bucket, Prefix="")
    folder_array = [obj['Key'] for obj in response['Contents']]
    file_array = [i for i in folder_array if i[-1] != '/']
    for key in file_array:
        file = read_file_from_s3_object(bucket, key)
        buffer = BytesIO(file)
        read_and_redirect(bucket, buffer, key, depara_config, kafka_config)


if __name__ == "__main__":
    main()
