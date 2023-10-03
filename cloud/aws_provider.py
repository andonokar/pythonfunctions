from cloud.provider import Provider
from cloud import basic_s3_functions as bsf


class AWS(Provider):
    def list_files(self, bucket: str, folder_path: str) -> list:
        return bsf.list_s3_files(bucket, folder_path)

    def move_file(self, source_bucket_name: str, destination_bucket_name: str, file_key: str, new_file_key: str) -> None:
        return bsf.move_file_s3(source_bucket_name, destination_bucket_name, file_key, new_file_key)

    def read_json_from_file(self, bucket: str, key: str) -> dict:
        return bsf.read_json_from_s3_object(bucket, key)

    def read_yaml_from_file(self, bucket: str, key: str) -> dict:
        return bsf.read_yaml_from_s3_object(bucket, key)

    def read_file(self, bucket: str, key: str) -> bytes:
        return bsf.read_file_from_s3_object(bucket, key)

    def save_file_to_cloud(self, file_path: str, bucket: str, key: str):
        return bsf.save_file_to_s3_bucket2(file_path, bucket, key)
