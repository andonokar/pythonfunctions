# importando as classes
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from datetime import datetime
from cloud.basic_s3_functions import save_file_to_s3_bucket2
import csv
from util import log
from variables import config


class Escrita:
    bucket = config['escrita']["bucket_avro"]
    bucketerrors = config['escrita']["bucket_errors"]

    def __init__(self, tables: dict) -> None:
        """
        :param tables: list of dict - keys = name, df, header, schema, s3key, bucket, bucketerrors
        name : nome da tabela
        df: o dataframe
        header: o cabecalho
        schema: o schema do avro
        """
        self.tables = tables

    @log.logs
    def escreve(self):
        """
        realiza a escrita no avro e, se necessario, no csv para a tabela de geracao
        """
        # abrindo o writer
        fmsg = f'{Escrita.__name__}.{self.escreve.__name__}'
        logger = log.createLogger(fmsg)
        for data in self.tables:
            if len(data['df'].index) != 0:
                avropath = f"/tmp/{data['name']}.avro"
                logger.info(f'iniciando escrita avro do arquivo {data["name"]}')
                writer = DataFileWriter(open(avropath, config['escrita']['write_format_avro']), DatumWriter(), data["schema"])

                # criando uma lista para armazenar os erros
                erros = []
                sucess = 0
                mistake = 0

                # fazendo a iteracao linha a linha para tipagem do dado
                for index, row in data['df'].iterrows():
                    try:
                        # executando a tipagem e validacoes basicas(eg: not null)
                        dic_avro = {i:
                                    None if row[i] is None else
                                    # datetime.strptime(row[i], '%Y-%m-%d').date().isoformat() if 'date' in j else
                                    # datetime.strptime(row[i], '%Y-%m-%d %H:%M:%S.%f').date().isoformat() if "timestamp-millis" in j else
                                    int(float(row[i])) if 'int' in j else
                                    float(row[i]) if 'float' in j else
                                    row[i] if 'boolean' in j else
                                    str(row[i])
                                    for i, j in data["header"].items() if i in data['df'].columns}
                        # escrevendo no avro
                        writer.append(dic_avro)
                        sucess += 1
                    # gerando a lista com os dados que nao passaram
                    except Exception as err:
                        print(sucess)
                        erro = list(row)
                        erro.append(err)
                        erros.append(erro)
                        mistake += 1
                # fechando o writer
                writer.close()
                logger.info(f'Escrita no Avro realizada com sucesso! linhas escritas:{sucess}'
                            f'  linhas recusadas:{mistake}')

                now = datetime.now()
                date_str = now.strftime('%Y-%m-%d')
                time_str = now.strftime('%H-%M')
                show = f"{date_str}-{time_str}"
                save_file_to_s3_bucket2(avropath, self.bucket, f'{data["s3key"]}{show}_{data["name"]}.avro')
                # criando o csv caso a tabela tenha algum dado errado
                if len(erros) > 0:
                    logger.info(f'iniciando escrita csv do arquivo{data["name"]}')
                    # montando o cabecalho do csv
                    csvpath = f"/tmp/{data['name']}.csv"
                    headercsv = list(data['df'].columns)
                    headercsv.append('erro')
                    erros.insert(0, headercsv)
                    with open(csvpath, config['escrita']["write_format_csv"], newline='') as csvarquivo:
                        # inserindo os erros no csv
                        writer = csv.writer(csvarquivo)
                        writer.writerows(erros)
                        logger.info(f'csv escrito com sucesso!')

                    save_file_to_s3_bucket2(csvpath, self.bucketerrors, f'{data["s3key"]}{show}_{data["name"]}.csv')
            else:
                logger.info(f'{data["name"]} sem linhas para gerar um avro')
