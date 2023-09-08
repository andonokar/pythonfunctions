import pandas as pd
from util import log
from globalresources.basic_extract_functions import (
    convert_boolean, convert_decimal)
from globalresources.process_dataframe import ProcessDataFrame


class Extrator:

    def prepara_tabela(self, processdf: ProcessDataFrame) -> list[dict]:
        """
        Realiza todo o processo de extracao e tratamento/enriquecimentos basicos e fica pronta pra ter o schema
        testado para geracao do avro
        :param processdf: a classe ProcessDataFrame instanciada
        :return: retorna as tabelas a serem checadas pelo avro em uma lista de dicionarios
        """
        pass

    @staticmethod
    def check_token(config):
        if config.get('hash') != 'uMHBDosKoqTtFOsaS7kmy3XT7YMz9U7L':
            raise ConnectionRefusedError('o token encontra-se invalido, contate o suporte')


class CsvExcelExtractor(Extrator):
    @log.logs
    def prepara_tabela(self, processdf: ProcessDataFrame):
        # validando o token
        config = processdf.config
        key2 = processdf.name
        self.check_token(config)
        avrsch_config = processdf.set_avro_schema()
        header_config = {i['name']: i['logicalType'] if 'logicalType' in i.keys() else
                         i['type'] for i in config['avro_schema']['fields']}

        # renomeando colunas com suporte a regex
        df = processdf.rename_dataframe()

        # inserindo o path
        df[config.get("colunapath", 'url_path')] = key2

        # utilizando dados do avro
        floatconversion = [i for i, j in header_config.items() if 'float' in j]
        floatconversion_df = list(df.columns.intersection(floatconversion))
        int_repasse = [i for i, j in header_config.items() if 'int' in j]
        preencher = floatconversion + int_repasse
        column_dates = [i for i, j in header_config.items() if 'date' in j]
        column_dates_df = list(df.columns.intersection(column_dates))
        timestamp_dates = [i for i, j in header_config.items() if "timestamp-millis" in j]
        timestamp_dates_df = list(df.columns.intersection(timestamp_dates))
        column_boolean = [i for i, j in header_config.items() if 'boolean' in j]
        column_boolean_df = list(df.columns.intersection(column_boolean))

        # trocando o nan para null
        df = df.where((pd.notnull(df)), None)

        # tratando colunas com base no tipo
        for float_column in floatconversion_df:
            if 'float' not in str(df[float_column].dtype).lower():
                df[float_column] = df[float_column].apply(convert_decimal)

        for float_int_column in preencher:
            if float_int_column not in df.columns:
                df[float_int_column] = 0
            df[float_int_column].fillna(0, inplace=True)

        for date_column in column_dates_df:
            df[date_column] = pd.to_datetime(df[date_column], errors='raise').dt.strftime('%Y-%m-%d')

        for timestamp_column in timestamp_dates_df:
            df[timestamp_column] = pd.to_datetime(df[timestamp_column], errors='raise').dt.strftime('%Y-%m-%d %H:%M:%S.%f')
            
        for bool_column in column_boolean_df:
            if 'boolean' not in str(df[bool_column].dtype).lower():
                df[bool_column] = df[bool_column].apply(convert_boolean)

        df = df.where((pd.notnull(df)), None)
        # criando o dicionario de retorno em que o programa ira trabalhar
        tabelas = [{"name": f"{config['name']}{key2.split('.')[0]}", "df": df,
                    "schema": avrsch_config, "header": header_config,
                    "s3key": config['s3key']}]
        return tabelas
