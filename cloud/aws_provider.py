from cloud.provider import Provider
from cloud import basic_s3_functions as bsf


class AWS(Provider):
    @staticmethod
    def list_files(bucket: str, folder_path: str) -> list:
        return bsf.list_s3_files(bucket, folder_path)

    @staticmethod
    def move_file(source_bucket_name: str, destination_bucket_name: str, file_key: str, new_file_key: str) -> None:
        return bsf.move_file_s3(source_bucket_name, destination_bucket_name, file_key, new_file_key)

    @staticmethod
    def read_json_from_file(bucket: str, key: str) -> dict:
        return bsf.read_json_from_s3_object(bucket, key)

    @staticmethod
    def read_yaml_from_file(bucket: str, key: str) -> dict:
        return bsf.read_yaml_from_s3_object(bucket, key)

    @staticmethod
    def read_file(bucket: str, key: str) -> bytes:
        return bsf.read_file_from_s3_object(bucket, key)

    @staticmethod
    def save_file_to_cloud(file_path: str, bucket: str, key: str):
        return bsf.save_file_to_s3_bucket2(file_path, bucket, key)
