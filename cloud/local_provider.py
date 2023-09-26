from cloud.provider import Provider
from cloud import basic_local_functions as blf


class Local(Provider):
    @staticmethod
    def list_files(bucket: str, folder_path: str) -> list:
        ...

    @staticmethod
    def move_file(source_bucket_name: str, destination_bucket_name: str, file_key: str, new_file_key: str) -> None:
        ...

    @staticmethod
    def read_json_from_file(bucket: str, key: str) -> dict:
        ...

    @staticmethod
    def read_yaml_from_file(bucket: str, key: str) -> dict:
        return blf.read_yaml_local(bucket, key)

    @staticmethod
    def read_file(bucket: str, key: str) -> bytes:
        ...

    @staticmethod
    def save_file_to_cloud(file_path: str, bucket: str, key: str):
        ...
