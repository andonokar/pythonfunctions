from globalresources.criadataframe import CriaDataFrame
from avro_writer import Escrita
from globalresources.reader_client import Client
from globalresources.select_extraction_class import SelectClassExtraction
from cloud.basic_s3_functions import move_file_s3


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
    tables = CriaDataFrame(extrator, bucket, file, key, file_conf).extrair_para_avro()
    writer = Escrita(tables)
    writer.escreve()
    move_file_s3(bucket, escrita_conf["destinationbucket"], key, f'{escrita_conf["prefixname"]}{key}')
