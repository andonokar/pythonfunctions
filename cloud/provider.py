from abc import ABC, abstractmethod


class Provider(ABC):
    """
    classe interface para provedores cloud
    """
    @staticmethod
    @abstractmethod
    def list_files(bucket: str, folder_path: str) -> list:
        """
        lists all files inside a bucket or folder from s3
        :param bucket: s3 bucket name
        :param folder_path: caminho da pasta até o local desejado
        :return: lista com todos os nomes de arquivos
        """
        pass

    @staticmethod
    @abstractmethod
    def move_file(source_bucket_name: str, destination_bucket_name: str, file_key: str, new_file_key: str) -> None:
        """
        Copies a file to another folder in S3 and deletes the old one
        :param source_bucket_name: bucket of origin
        :param destination_bucket_name: new bucket name
        :param file_key: the file key
        :param new_file_key: the new file key
        """
        pass

    @staticmethod
    @abstractmethod
    def read_json_from_file(bucket: str, key: str) -> dict:
        """
        Read JSON file in s3 bucket
        :param bucket: s3 bucket name
        :param key: filename
        :return: JSON file as a python dictionary
        """
        pass

    @staticmethod
    @abstractmethod
    def read_yaml_from_file(bucket: str, key: str) -> dict:
        """
        Read YAML file in s3 bucket
        :param bucket: s3 bucket name
        :param key: filename
        :return: YAML file as a python dictionary
        """
        pass

    @staticmethod
    @abstractmethod
    def read_file(bucket: str, key: str) -> bytes:
        """
        Read file in s3 bucket
        :param bucket: s3 bucket name
        :param key: filename
        :return: file bytes
        """
        pass

    @staticmethod
    @abstractmethod
    def save_file_to_cloud(file_path: str, bucket: str, key: str):
        """
        save binary file to s3 bucket
        :param file_path: file path
        :param bucket: s3 bucket name
        :param key: filename
        :return: dictionary with response
        """
        pass
