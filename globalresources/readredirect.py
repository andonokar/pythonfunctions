from globalresources.criadataframe import CriaDataFrame
from fastavro_writer import Escrita
from globalresources.reader_client import Client
from globalresources.select_extraction_class import SelectClassExtraction
from cloud.basic_s3_functions import move_file_s3
from util.log_kafka import createloggerforkafka
from util import log
from io import BytesIO
from globalresources.process_dataframe import read_dataframe


def read_and_redirect(bucket: str, file: str | BytesIO, key: str) -> None:
    """
    Funcional principal a ser executada pelo lambda
    Le o cliente, redireciona para o extrator certo, extrai o arquivo, gera o avro, salva na landing zone
    e move os arquivos para o bucket de arquivos processados
    :param bucket: o bucket de onde veio o arquivo do s3
    :param file: o arquivo do s3 em bytes
    :param key: o caminho do arquivo no s3
    :return:
    """
    fmsg = f'{bucket}/{key}'
    logger = log.createLogger(fmsg)
    client = Client(bucket, key)
    logger.warning('client ok')
    escrita_conf, file_conf = client.get_conf()
    logger.warning('conf ok')
    extrator = SelectClassExtraction(file_conf).get_class()
    logger.warning('class ok')
    try:
        processdf = read_dataframe(file, key, file_conf)
        tables = CriaDataFrame(extrator, processdf).extrair_para_avro()
        logger.warning('extraction ok')
        writer = Escrita(tables, escrita_conf)
        mistakes = writer.escreve()
        logger.warning('avro ok')
    except Exception as err:
        if escrita_conf.get('topic'):
            log_args = {
                'nome_arquivo': key,
                'S3_ini': bucket,
                'S3_fim': escrita_conf["bucket_errors"],
                'etapa': 'landing-zone'
            }
            logger.warning('trying kafka')
            createloggerforkafka(str(err), 'error', topic=escrita_conf["topic"], **log_args)
            logger.warning('sucess kafka')
        move_file_s3(bucket, escrita_conf["bucket_errors"], key, f'{escrita_conf["prefixname"]}{key}')
        logger.warning('file moved with error')
        logger.critical(str(err))
        raise err
    else:
        if escrita_conf.get('topic'):
            log_args = {
                'nome_arquivo': key,
                'S3_ini': bucket,
                'S3_fim': escrita_conf["destinationbucket"],
                'etapa': 'landing-zone'
            }
            logger.warning('trying kafka')
            if len(mistakes) == 0:
                createloggerforkafka('processado ok', 'info', topic=escrita_conf["topic"], **log_args)
            else:
                createloggerforkafka(f'processado com {mistakes} erros, conferir o bucket de erros', 'warning', topic=escrita_conf["topic"], **log_args)
                logger.warning(f'{mistakes} erros gerando avro')
            logger.warning('sucess kafka')
        move_file_s3(bucket, escrita_conf["destinationbucket"], key, f'{escrita_conf["prefixname"]}{key}')
        logger.warning('file moved with sucess')
