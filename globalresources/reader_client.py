from util import log
from variables import depara_config
from cloud.basic_s3_functions import read_yaml_from_s3_object


class Client:

    def __init__(self, bucket, key):
        self.bucket = bucket
        self.key = key

    @log.logs
    def get_conf(self):
        """
        Le as configuracoes para a extracao
        :return: a tabela extraida pela classe
        """
        fmsg = f'{Client.__name__}.{self.get_conf.__name__}'
        logger = log.createLogger(fmsg)
        extraction_config = depara_config.get(self.bucket)
        if not extraction_config:
            logger.error(f'Nenhum Cliente configurado para o bucket {self.bucket}')
            raise NotImplementedError(f'Nenhum Cliente configurado para o bucket {self.bucket}')
        conf_bucket = extraction_config.get('bucket')
        conf_key = extraction_config.get('key')
        if not (conf_bucket or conf_key):
            logger.error(f'O arquivo de configuracao para o {self.bucket} esta mal configurado: confira se as chaves bucket e key existem')
            raise KeyError(f'O arquivo de configuracao para o {self.bucket} esta mal configurado: confira se as chaves bucket e key existem')
        client_yaml = read_yaml_from_s3_object(conf_bucket, conf_key)
        folder = self.key.split("/")[0]
        yaml_conf = client_yaml.get(folder)
        if not yaml_conf:
            logger.error(f'Nenhuma extracao configurada para {folder}')
            raise NotImplementedError(f'Nenhuma extracao configurada para {folder}')
        return client_yaml, yaml_conf
