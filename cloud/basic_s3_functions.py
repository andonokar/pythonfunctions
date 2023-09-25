"""s3 utilities"""
import json
import yaml
import boto3
from botocore.exceptions import ClientError
from exceptions import exceptions


def list_s3_files(bucket: str, folder_path: str) -> list:
    """
    lists all files inside a bucket or folder from s3
    :param bucket: s3 bucket name
    :param folder_path: caminho da pasta até o local desejado
    :return: lista com todos os nomes de arquivos
    """
    s3 = boto3.resource('s3')
    bucket_name = s3.Bucket(bucket)
    files_found = [obj for obj in bucket_name.objects.filter(Prefix=folder_path) if
                   obj.key[-1] != '/']
    return files_found


def move_file_s3(source_bucket_name: str, destination_bucket_name: str, file_key: str, new_file_key: str):
    """
    Copies a file to another folder in S3 and deletes the old one
    :param source_bucket_name: bucket of origin
    :param destination_bucket_name: new bucket name
    :param file_key: the file key
    :param new_file_key: the new file key
    """

    try:
        s3 = boto3.client('s3')
        s3.copy_object(
            Bucket=destination_bucket_name,
            CopySource={'Bucket': source_bucket_name, 'Key': file_key},
            Key=new_file_key
        )
        s3.delete_object(Bucket=source_bucket_name, Key=file_key)
    except Exception as e:
        raise exceptions.FailedToMoveS3File(f"s3 error: {e}")


def read_json_from_s3_object(bucket: str, key: str) -> dict:
    """
    Read JSON file in s3 bucket
    :param bucket: s3 bucket name
    :param key: filename
    :return: JSON file as a python dictionary
    """
    try:
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket, Key=key)
        json_object = response['Body'].read().decode('utf-8')
    except Exception as err:
        raise exceptions.JsonReadingError(f'Error while reading json from Bucket={bucket}, Key={key}: {err}')
    return json.loads(json_object)


def read_yaml_from_s3_object(bucket: str, key: str) -> dict:
    """
    Read YAML file in s3 bucket
    :param bucket: s3 bucket name
    :param key: filename
    :return: YAML file as a python dictionary
    """
    try:
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket, Key=key)
        yaml_object = response['Body'].read()
    except Exception as err:
        raise exceptions.YamlReadingError(f'Error while reading yaml from Bucket={bucket}, Key={key}: {err}')
    return yaml.safe_load(yaml_object)


def read_file_from_s3_object(bucket: str, key: str) -> bytes:
    """
    Read file in s3 bucket
    :param bucket: s3 bucket name
    :param key: filename
    :return: file bytes
    """
    response = None
    try:
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket, Key=key)
    except Exception as err:
        raise exceptions.FileReadingError(f'Error while reading from from Bucket={bucket}, Key={key}: {err}')
    return response['Body'].read()


def save_file_to_s3_bucket2(file_path: str, bucket: str, key: str):
    """
    save binary file to s3 bucket
    :param file_path: file path
    :param bucket: s3 bucket name
    :param key: filename
    :return: dictionary with response
    """
    s3_client = boto3.resource('s3')
    try:
        response = s3_client.meta.client.upload_file(file_path, bucket, key)

    except ClientError as exc:
        raise exceptions.FailedSavingFileToS3(f'error saving in s3: file_path={file_path}, bucket={bucket}, key={key}, {exc}')
    else:
        return response
