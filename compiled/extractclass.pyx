import pandas as pd
from avro.schema import parse
from json import dumps
from io import BytesIO
import logging
from functools import wraps


def createLogger(source_log: str = __name__):
    """
    Função para criar um ponto de observaçã atravé do uso de logs

    :return:
    """
    log_format = '%(levelname)-8s||%(asctime)s||%(name)-12s||%(lineno)d||%(message)s'
    logging.basicConfig(level=logging.INFO, format=log_format)
    logger = logging.getLogger(source_log)
    return logger

def logs(func):
    """
    Decorator para monitorar vi log qualquer função desejada
    :param func: não da função de entrada
    :return: retorna a função de entrada
    """

    @wraps(func)
    def inner(*args, **kwargs):
        logger = createLogger(func.__name__)
        # log_message = f'stating.... func:{func.__name__}:args:{args}:kwargs:{kwargs}'
        log_message = f'stating....'
        logger.info(log_message)
        result = func(*args, **kwargs)
        # log_message = f'finished func:{func.__name__}:args:{args}:kwargs:{kwargs}'
        log_message = f'finished.... '
        logger.info(log_message)
        return result

    return inner


def convert_boolean(val):
    """
    Trata os decimais no caso de não estarem formatados corretamente
    """
    if val:
        if val in [0, '0', 'false', 'False', 'FALSE', False]:
            return False
        if val in [1, '1', 'true', "True", "TRUE", True]:
            return True
    return None


def convert_decimal(val):
    """
    Trata os decimais no caso de não estarem como float
    """
    if ',' in str(val):
        try:
            return float(str(val).replace('.', '').replace(',', '.'))
        except:
            return 0
    else:
        try:
            return float(val)
        except:
            return 0


def rename_columns(df, column_renames, logger):
    df = df.rename(columns=column_renames)
    return df


def regex_rename_columns(df, column_renames, logger):
    try:
        columns = {i: j for i, j in zip([df.filter(regex=name).columns.tolist()[0] for name in column_renames.keys()],
                                        column_renames.values())}
    except IndexError:
        logger.error('um ou mais regex nao resgataram uma coluna para captura')
        raise NameError('um ou mais regex nao resgataram uma coluna para captura')
    df = df.rename(columns=columns)
    return df


def index_rename_columns(df, column_renames, logger):
    try:
        columns = {df.columns.tolist()[int(i)]: j for i, j in column_renames.items()}
    except IndexError:
        logger.error('o indice da coluna nao existe')
        raise IndexError('o indice da coluna nao existe')
    df = df.rename(columns=columns)
    return df

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


class CsvExcelExtractor(Extrator):
    @logs
    def prepara_tabela(self, file, key, config):

        # criando o log
        fmsg = f'{CsvExcelExtractor.__name__}.{self.prepara_tabela.__name__}'
        logger = createLogger(fmsg)

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
            encoding = csv_options('encoding', 'UTF-8')
            sep = csv_options('sep', ',')
            low_memory = csv_options('low_memory', False)
            if not header:
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
            if not header:
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
            df[timestamp_column] = pd.to_datetime(df[timestamp_column], errors='raise').dt.strftime(
                '%Y-%m-%d %H:%M:%S.%f')

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

