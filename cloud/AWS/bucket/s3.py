"""s3 utilities"""
import json

import boto3
from botocore.exceptions import ClientError
from bson import ObjectId
from datetime import datetime
from util import log

@log.logs
def list_s3_files(bucket: str, folder_path: str) -> list:
    """
    lists all files inside a bucket or folder from s3
    :param bucket: s3 bucket name
    :param folder_path: caminho da pasta até o local desejado
    :return: lista com todos os nomes de arquivos
    """
    fsmg = f'{__name__}:{list_s3_files.__name__}'
    files_found = None
    try:
        s3_client = boto3.client('s3')
        objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=folder_path)
        files_found = []

        for item in objects['Contents']:
            found_file = item['Key'].split('/')[1]
            if found_file != '':
                files_found.append(found_file)
    except Exception as e:
        log.createLogger(fsmg).error(f'Erro ao obter arquivos do S3: {e}')
    return files_found


@log.logs
def move_originalfile_s3(source_bucket_name: str, destination_bucket_name: str, first_folder_name: str, file_key: str):
    """
    Copies a file to another folder in S3, creates a specific key and deletes the old one
    :param source_bucket_name: bucket of origin
    :param destination_bucket_name: new bucket name
    :param first_folder_name: the first folder name with slash in the newbucket
        (eg: "ORIGINALFILES/")(if None use "" as parameter)
    :param file_key: the file key
    """
    now = datetime.now()
    date_str = now.strftime('%Y-%m-%d')
    time_str = now.strftime('%H-%M')
    show = f"{date_str}-{time_str}"
    keynames = file_key.split("/")
    filename = keynames[-1]
    newfilename = f"{show}_{filename}"
    keynames.pop(-1)
    keynames.append(newfilename)
    newkey = f'{first_folder_name}{"/".join(keynames)}'
    move_file_s3(source_bucket_name, destination_bucket_name, file_key, newkey)


@log.logs
def move_file_s3(source_bucket_name: str, destination_bucket_name: str, file_key: str, new_file_key: str):
    """
    Copies a file to another folder in S3 and deletes the old one
    :param source_bucket_name: bucket of origin
    :param destination_bucket_name: new bucket name
    :param file_key: the file key
    :param new_file_key: the new file key
    """
    fsmg = f'{__name__}:{move_file_s3.__name__}'

    try:
        s3 = boto3.client('s3')
        s3.copy_object(
            Bucket=destination_bucket_name,
            CopySource={'Bucket': source_bucket_name, 'Key': file_key},
            Key=new_file_key
        )
        s3.delete_object(Bucket=source_bucket_name, Key=file_key)
    except Exception as e:
        log.createLogger(fsmg).error(f'Error while copying or deleting S3 file: {e}')
        raise ValueError("s3 error")

@log.logs
def read_json_from_s3_object(bucket: str, key: str) -> dict:
    """
    Read JSON file in s3 bucket
    :param bucket: s3 bucket name
    :param key: filename
    :return: JSON file as a python dictionary
    """
    fsmg = f'{__name__}:{read_json_from_s3_object.__name__}'
    json_object = None
    try:
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket, Key=key)
        json_object = response['Body'].read().decode('utf-8')
    except Exception as err:
        log.createLogger(fsmg).error(f'GENERAL///Error while readingS3 file: {err}')

    return json.loads(json_object)

@log.logs
def read_file_from_s3_object(bucket: str, key: str) -> bytes:
    """
    Read file in s3 bucket
    :param bucket: s3 bucket name
    :param key: filename
    :return: file bytes
    """
    # global s3_client
    #
    # try:
    #     if not s3_client:
    #         s3_client = boto3.client('s3')
    #
    #     response = s3_client.get_object(Bucket=bucket, Key=key)
    #
    # except ClientError as exc:
    #     raise ValueError('Wrong key, no such file in bucket') from exc
    # else:
    #     return response['Body'].read()
    fsmg = f'{__name__}:{read_file_from_s3_object.__name__}'
    response = None
    try:
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket, Key=key)
    except Exception as err:
        log.createLogger(fsmg).error(f'GENERAL///Error while readingS3 file: {err}')

    return response['Body'].read()

@log.logs
def save_file_to_s3_bucket(file_path: bytes, bucket: str, key: str):
    """
    save binary file to s3 bucket
    :param file_path: file path
    :param bucket: s3 bucket name
    :param key: filename
    :return: dictionary with response
    """
    fsmg = f'{__name__}:{save_file_to_s3_bucket.__name__}'
    s3_client = boto3.client('s3')

    try:
        response = s3_client.put_object(Body=file_path,
                                        Bucket=bucket,
                                        Key=key)

    except ClientError as exc:
        log.createLogger(fsmg).error(f'*** Added report to bucket: {bucket}, with key: {key} {str(exc)}')
        raise ValueError('Wrong key, no such file in bucket') from exc
    else:
        log.createLogger(fsmg).info(f'*** Added report to bucket: {bucket}, with key: {key}')

        #logger.info(f'*** Added report to bucket: {bucket}, with key: {key}')
        return response

@log.logs
def save_file_to_s3_bucket2(file_path: str, bucket: str, key: str):
    """
    save binary file to s3 bucket
    :param file_path: file path
    :param bucket: s3 bucket name
    :param key: filename
    :return: dictionary with response
    """
    s3_client = boto3.resource('s3')

    fsmg = f'{__name__}:{save_file_to_s3_bucket.__name__}'
    logger = log.createLogger(fsmg)
    try:
        response = s3_client.meta.client.upload_file(file_path, bucket, key)

    except ClientError as exc:
        logger.error('GENERAL///Wrong key, no such file in bucket')
        raise ValueError('Wrong key, no such file in bucket') from exc
    else:
        logger.info(f'GENERAL///*** Added report to bucket: {bucket}, with key: {key}')
        #print(f'*** Added report to bucket: {bucket}, with key: {key}')
        #logger.info(f'*** Added report to bucket: {bucket}, with key: {key}')
        return response


def get_size_s3(bucket, key):
    global s3_client

    if not s3_client:
        s3_client = boto3.client('s3')

    response = s3_client.head_object(Bucket=bucket, Key=key)
    size = response['ContentLength']


class MyEncoder(json.JSONEncoder):
    """
    Extends the encoder to recognize other objects
    """
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, datetime):
            return o.__str__()
        return json.JSONEncoder.default(self, o)
