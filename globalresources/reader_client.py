from util import log
from cloud.basic_s3_functions import read_yaml_from_s3_object


class Client:

    def __init__(self, bucket, key):
        self.bucket = bucket
        self.key = key

    def _validate_depara_config(self, depara_config: dict):
        # Getting the configuration based on the bucket
        extraction_config = depara_config.get(self.bucket)
        # Checking if the configuration exists
        if not extraction_config:
            raise NotImplementedError(f'Nenhum Cliente configurado para o bucket {self.bucket}')
        return extraction_config

    def _read_client_conf(self, extraction_config: dict):
        conf_bucket = extraction_config.get('bucket')
        conf_key = extraction_config.get('key')
        if not (conf_bucket and conf_key):
            raise KeyError(
                f'O arquivo de configuracao para o {self.bucket} esta mal configurado: confira se as chaves bucket e key existem')
        # Reading the configuration
        client_yaml = read_yaml_from_s3_object(conf_bucket, conf_key)
        return client_yaml

    def _validade_folder_conf(self, client_yaml: dict):
        # Getting the folder for extraction
        folder = self.key.split("/")[0]
        file_conf = client_yaml.get(folder)
        # Checking if the configuration for the folder exists
        if not file_conf:
            raise NotImplementedError(f'Nenhuma extracao configurada para {folder}')
        return file_conf

    @staticmethod
    def _validate_escrita_conf(client_yaml: dict):
        # Getting the write configuration
        escrita_conf = client_yaml.get('escrita')
        # Checking if the write configuration exists
        if not escrita_conf:
            raise KeyError('a chave escrita encontra-se ausente, preencha de acordo com o template')
        # Checking if the write configurations are right
        bucket_avro = escrita_conf.get('bucket_avro')
        bucket_errors = escrita_conf.get('bucket_errors')
        destinationbucket = escrita_conf.get('destinationbucket')
        prefixname = escrita_conf.get('prefixname')
        if not (bucket_avro and bucket_errors and destinationbucket and prefixname):
            raise KeyError('as configuracoes de escrita estao mal configuradas, preencha de acordo com o template')
        # Completing the folder / delimiter in case its absent
        if not escrita_conf['prefixname'].endswith('/'):
            escrita_conf['prefixname'] = f'{escrita_conf["prefixname"]}/'
        return escrita_conf

    @log.logs
    def get_conf(self, depara_config: dict):
        """
        Le as configuracoes para a extracao
        :return: a tabela extraida pela classe
        """
        extraction_config = self._validate_depara_config(depara_config)
        client_yaml = self._read_client_conf(extraction_config)
        file_conf = self._validade_folder_conf(client_yaml)
        escrita_conf = self._validate_escrita_conf(client_yaml)
        return escrita_conf, file_conf
