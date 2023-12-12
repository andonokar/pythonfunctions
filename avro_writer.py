# importando as classes
from typing import Protocol
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from datetime import datetime
import csv
from util import log
from globalresources.treated_dataframe import TreatedDataFrame


class CloudProvider(Protocol):
    def save_file_to_cloud(self, file_path: str, bucket: str, key: str) -> None:
        pass


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
        self.prefixerrors = conf.get('prefix_errors')
        self.prefixprocessed = conf.get('prefix_processed')

    @staticmethod
    def _write_avro(data: TreatedDataFrame, writer: DataFileWriter):
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
                writer.append(dic_avro)
            # gerando a lista com os dados que nao passaram
            except Exception as err:
                erro = list(row)
                erro.append(type(err).__name__ + ': ' + str(err))
                erros.append(erro)
                mistake += 1
            # fechando o writer
        writer.close()
        return mistake, erros

    @log.logs
    def escreve(self, cloud: CloudProvider):
        """
        realiza a escrita no avro e, se necessario, no csv para a tabela de geracao
        """
        # abrindo o writer
        mistakes = []
        for data in self.tables:
            if len(data.df.index) != 0:
                avropath = f"/tmp/{data.name}.avro"
                writer = DataFileWriter(open(avropath, 'wb'), DatumWriter(), data.schema)
                mistake, erros = self._write_avro(data, writer)
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
                    cloud.save_file_to_cloud(csvpath, self.bucketerrors, f'{self.prefixerrors}/{data.s3key}{show}_{data.name}.csv')
                cloud.save_file_to_cloud(avropath, self.bucket, f'{data.s3key}{show}_{data.name}.avro')
                mistakes.append(mistake)
        return mistakes
