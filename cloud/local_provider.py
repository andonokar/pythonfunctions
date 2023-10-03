from cloud.provider import Provider
from cloud import basic_local_functions as blf


class Local(Provider):
    def list_files(self, bucket: str, folder_path: str) -> list:
        ...

    def move_file(self, source_bucket_name: str, destination_bucket_name: str, file_key: str, new_file_key: str) -> None:
        ...

    def read_json_from_file(self, bucket: str, key: str) -> dict:
        ...

    def read_yaml_from_file(self, bucket: str, key: str) -> dict:
        return blf.read_yaml_local(bucket, key)

    def read_file(self, bucket: str, key: str) -> bytes:
        ...

    def save_file_to_cloud(self, file_path: str, bucket: str, key: str):
        ...
