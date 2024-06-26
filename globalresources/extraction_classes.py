import pandas as pd
from avro.schema import parse
from json import dumps
from util import log
from io import BytesIO
from globalresources.basic_extract_functions import (
    convert_boolean, convert_decimal, rename_columns, index_rename_columns, regex_rename_columns)
import sys


class Extrator:

    def prepara_tabela(self, file: BytesIO | str, key: str, config: dict) -> list[dict]:
        """
        Realiza todo o processo de extracao e tratamento/enriquecimentos basicos e fica pronta pra ter o schema
        testado para geracao do avro
        :param file: o arquivo em bytes
        :param key: o caminho do arquivo
        :param config: a configuracao da extracao
        :return: retorna as tabelas a serem checadas pelo avro em uma lista de dicionarios
        """
        pass

    @staticmethod
    def check_token(config, logger):
        if config.get('hash') != 'uMHBDosKoqTtFOsaS7kmy3XT7YMz9U7L':
            logger.critical('o token encontra-se invalido, contate o suporte')
            sys.exit(1)


class CsvExcelExtractor(Extrator):
    @log.logs
    def prepara_tabela(self, file, key, config):
        # criando o log
        fmsg = f'{CsvExcelExtractor.__name__}.{self.prepara_tabela.__name__}'
        logger = log.createLogger(fmsg)

        # validando o token
        self.check_token(config, logger)

        # tratando os nomes a ser utilizado para o path e extensao
        ext = key.split('.')[-1].lower()
        key2 = key.split("/")[-1]
        try:
            avrsch_config = parse(dumps(config.get("avro_schema")))
        except Exception as err:
            logger.error(f'Schema invalido para o avro: {err}')
            raise Exception(f'Schema invalido para o avro: {err}')
        header_config = {i['name']: i['logicalType'] if 'logicalType' in i.keys() else
                         i['type'] for i in config['avro_schema']['fields']}

        # inner method for reading csv
        def read_csv():
            csv_options = config.get('csv')
            if not csv_options:
                logger.error('chave csv ausente na configuracao yaml')
                raise KeyError('chave csv ausente na configuracao yaml')
            header = csv_options.get('header')
            encoding = csv_options.get('encoding', 'UTF-8')
            sep = csv_options.get('sep', ',')
            low_memory = csv_options.get('low_memory', False)
            if header is None:
                logger.error('a chave header deve ser especificado')
                raise KeyError('a chave header deve ser especificado')
            dataframe = pd.read_csv(file, header=header, encoding=encoding, sep=sep, low_memory=low_memory)
            return dataframe

        # inner method for reading excel
        def read_excel():
            excel_options = config.get('excel')
            if not excel_options:
                logger.error('chave excel ausente na configuracao yaml')
                raise KeyError('chave excel ausente na configuracao yaml')
            header = excel_options.get('header')
            if header is None:
                logger.error('a chave header deve ser especificado')
                raise KeyError('a chave header deve ser especificado')
            dataframe = pd.read_excel(file, header=header)
            return dataframe

        # declaring the reading method based on the extension
        depara = {
            "csv": read_csv,
            'xls': read_excel,
            'xlsb': read_excel,
            'xlsm': read_excel,
            'xlsx': read_excel,
        }

        method = depara.get(ext)

        # iniciando a extracao
        if method:
            df = method()
        else:
            logger.error(f"{key2} o arquivo nao e csv/excel")
            raise NotImplementedError(f"{key2} o arquivo nao e csv/excel")

        # renomeando colunas com suporte a regex
        column_renames = config.get('column_renames')
        if column_renames:
            rename_functions = {
                'normal': rename_columns,
                'regex': regex_rename_columns,
                'index': index_rename_columns
            }
            rename_type = config.get('rename_type', 'normal')
            rename_function = rename_functions.get(rename_type)
            if not rename_function:
                logger.error(f'o metodo {rename_type} para renomear colunas nao existe')
                raise NotImplementedError(f'o metodo {rename_type} para renomear colunas nao existe')
            df = rename_function(df, column_renames, logger)

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
        logger.info(f'extracao {key2} realizada com sucesso')
        return tabelas
