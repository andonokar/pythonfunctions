from globalresources.criadataframe import StrategyExtractor
from side_ideas.fastavro_writer import Escrita
from globalresources.reader_client import Client
from globalresources.select_extraction_class import SelectClassExtraction
from cloud.basic_s3_functions import move_file_s3
from util.log_kafka import createloggerforkafka
from util import log
from io import BytesIO
from globalresources.dataframe_reader import read_dataframe
from globalresources.yaml_reader import YamlReader


def read_and_redirect(bucket: str, file: str | BytesIO, key: str, depara_config: dict) -> None:
    """
    Funcional principal a ser executada
    Le o cliente, redireciona para o extrator certo, extrai o arquivo, gera o avro, salva na landing zone
    e move os arquivos para o bucket de arquivos processados
    :param bucket: o bucket de onde veio o arquivo
    :param file: o arquivo em bytes
    :param key: o caminho do arquivo
    :param depara_config: dicionario onde aponta as configuracoes do cliente
    :return:
    """
    # criando um log para a funcao
    fmsg = f'{bucket}/{key}'
    logger = log.createLogger(fmsg)
    # destrinchando o yaml inicial
    provider, config = list(depara_config.items())[0]
    # instanciando o yaml reader para extrair as configuracoes do cliente
    yaml_reader = YamlReader(provider)
    client = Client(bucket, key, yaml_reader)
    print('client ok')
    escrita_conf, file_conf = client.get_conf(config)
    print('conf ok')
    extrator = SelectClassExtraction(file_conf).get_class()
    print('class ok')
    try:
        processdf = read_dataframe(file, key, file_conf)
        tables = StrategyExtractor(extrator, processdf).extrair_para_avro()
        print('extraction ok')
        writer = Escrita(tables, escrita_conf)
        mistakes = writer.escreve()
        print('avro ok')
    except Exception as err:
        if escrita_conf.get('topic'):
            log_args = {
                'nome_arquivo': key,
                'S3_ini': bucket,
                'S3_fim': escrita_conf["bucket_errors"],
                'etapa': 'landing-zone'
            }
            print('trying kafka')
            createloggerforkafka(type(err).__name__ + ': ' + str(err), 'error', topic=escrita_conf["topic"], **log_args)
            print('sucess kafka')
        move_file_s3(bucket, escrita_conf["bucket_errors"], key, f'{escrita_conf["prefixname"]}{key}')
        logger.warning('file moved with error')
        logger.critical(type(err).__name__ + ': ' + str(err))
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
