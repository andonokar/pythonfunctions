# importando as classes
import fastavro
from datetime import datetime
from cloud.basic_s3_functions import save_file_to_s3_bucket2
import csv
from globalresources.treated_dataframe import TreatedDataFrame
from util import log
from tests.variables_test import _avro_schema

"""
MOVED TO SIDE PROJECT BECAUSE IF ITERATE LINES GET LESS PERFORMANCE AND IF WRITE COMPLETE LIST WRITES FASTER BUT LOSES
AUDITABILITY FOR EACH REGISTER
"""


class Escrita:

    def __init__(self, tables: list[TreatedDataFrame], conf: dict) -> None:
        """
        :param tables: list of dict - keys = name, df, header, schema, s3key, bucket, bucketerrors
        name : nome da tabela
        df: o dataframe
        header: o cabecalho
        schema: o schema do avro
        """
        self.tables = tables
        self.bucket = conf['bucket_avro']
        self.bucketerrors = conf['bucket_errors']

    @staticmethod
    def _write_avro(data: TreatedDataFrame, avro_file):
        erros = []
        mistake = 0

        # fazendo a iteracao linha a linha para tipagem do dado
        for index, row in data.df.iterrows():
            try:
                # executando a tipagem e validacoes basicas(eg: not null)
                dic_avro = {i:
                            None if row[i] is None else
                            int(float(row[i])) if 'int' in j else
                            float(row[i]) if 'float' in j else
                            row[i] if 'boolean' in j else
                            str(row[i])
                            for i, j in data.header.items() if i in data.df.columns}
                # escrevendo no avro
                fastavro.schemaless_writer(avro_file, fastavro.parse_schema(_avro_schema), dic_avro)
            # executando a tipagem e validacoes basicas(eg: not null)

        # gerando a lista com os dados que nao passaram
            except Exception as err:
                print(type(err).__name__ + ': ' + str(err))
                # erro = list(row)
                # erro.append(type(err).__name__ + ': ' + str(err))
                # erros.append(erro)
                mistake += 1
                # fechando o writer
        return mistake, erros

    @log.logs
    def escreve(self):
        """
        realiza a escrita no avro e, se necessario, no csv para a tabela de geracao
        """
        # abrindo o writer
        mistakes = []
        for data in self.tables:
            if len(data.df.index) != 0:
                avropath = f"/tmp/{data.name}.avro"
                with open(avropath, 'wb') as avro_file:
                    mistake, erros = self._write_avro(data, avro_file)
                now = datetime.now()
                date_str = now.strftime('%Y-%m-%d')
                time_str = now.strftime('%H-%M')
                show = f"{date_str}-{time_str}"
                # criando o csv caso a tabela tenha algum dado errado
                if len(erros) > 0:
                    # montando o cabecalho do csv
                    csvpath = f"/tmp/{data.name}.csv"
                    headercsv = list(data.df.columns)
                    headercsv.append('erro')
                    erros.insert(0, headercsv)
                    with open(csvpath, 'w', newline='') as csvarquivo:
                        # inserindo os erros no csv
                        writer = csv.writer(csvarquivo)
                        writer.writerows(erros)
                    save_file_to_s3_bucket2(csvpath, self.bucketerrors, f'{data.s3key}{show}_{data.name}.csv')
                save_file_to_s3_bucket2(avropath, self.bucket, f'{data.s3key}{show}_{data.name}.avro')
                mistakes.append(mistake)
        return mistakes

