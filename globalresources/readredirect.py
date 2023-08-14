from globalresources.criadataframe import CriaDataFrame
from avro_writer import Escrita
from globalresources.reader_client import Client
from globalresources.select_extraction_class import SelectClassExtraction
from cloud.basic_s3_functions import move_file_s3
from util.log_kafka import createloggerforkafka


def read_and_redirect(bucket, file, key):
    """
    Funcional principal a ser executada pelo lambda
    Le o cliente, redireciona para o extrator certo, extrai o arquivo, gera o avro, salva na landing zone
    e move os arquivos para o bucket de arquivos processados
    :param bucket: o bucket de onde veio o arquivo do s3
    :param file: o arquivo do s3 em bytes
    :param key: o caminho do arquivo no s3
    :return:
    """
    client = Client(bucket, key)
    escrita_conf, file_conf = client.get_conf()
    extrator = SelectClassExtraction(file_conf).get_class()
    try:
        tables = CriaDataFrame(extrator, bucket, file, key, file_conf).extrair_para_avro()
        writer = Escrita(tables, escrita_conf)
        writer.escreve()
        move_file_s3(bucket, escrita_conf["destinationbucket"], key, f'{escrita_conf["prefixname"]}{key}')
    except Exception as err:
        log_args = {
            'nome_arquivo': key,
            'S3_ini': bucket,
            'S3_fim': escrita_conf["bucket_errors"],
            'etapa': 'landing-zone'
        }
        logger = createloggerforkafka(read_and_redirect.__name__, topic=escrita_conf["topic"], **log_args)
        logger.error(str(err))
        move_file_s3(bucket, escrita_conf["bucket_errors"], key, f'{escrita_conf["prefixname"]}{key}')
        raise Exception(str(err))
    else:
        log_args = {
            'nome_arquivo': key,
            'S3_ini': bucket,
            'S3_fim': escrita_conf["destinationbucket"],
            'etapa': 'landing-zone'
        }
        logger = createloggerforkafka(read_and_redirect.__name__, topic=escrita_conf["topic"], **log_args)
        logger.info('processado ok')
        move_file_s3(bucket, escrita_conf["destinationbucket"], key, f'{escrita_conf["prefixname"]}{key}')
