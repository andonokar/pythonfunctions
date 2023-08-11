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
        file_conf = client_yaml.get(folder)
        if not file_conf:
            logger.error(f'Nenhuma extracao configurada para {folder}')
            raise NotImplementedError(f'Nenhuma extracao configurada para {folder}')
        escrita_conf = client_yaml.get('escrita')
        if not escrita_conf:
            logger.error('a chave escrita encontra-se ausente, preencha de acordo com o template')
            raise KeyError('a chave escrita encontra-se ausente, preencha de acordo com o template')
        bucket_avro = escrita_conf.get('bucket_avro')
        bucket_errors = escrita_conf.get('bucket_errors')
        destinationbucket = escrita_conf.get('destinationbucket')
        prefixname = escrita_conf.get('prefixname')
        if not(bucket_avro or bucket_errors or destinationbucket or prefixname):
            logger.error('as configuracoes de escrita estao mal configuradas, preencha de acordo com o template')
            raise KeyError('as configuracoes de escrita estao mal configuradas, preencha de acordo com o template')
        if not escrita_conf['prefixname'].endswith('/'):
            escrita_conf['prefixname'] = f'{escrita_conf["prefixname"]}/'
        return escrita_conf, file_conf
