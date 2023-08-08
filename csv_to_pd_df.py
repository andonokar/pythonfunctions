import pandas as pd
from avro.schema import parse
from json import dumps
from util import log
from variables import config


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
    Trata os decimais no caso de não estarem formatados corretamente
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


class CsvExcelExtractor:
    @log.logs
    def prepara_tabela(self, file, key):
        """
        Realiza todo o processo de extracao e tratamento/enriquecimentos basicos e fica pronta pra ter o schema
        testado para geracao do avro
        :param file: o arquivo em bytes
        :param key: o caminho do arquivo
        :return: retorna as tabelas a serem checadas pelo avro em uma lista de dicionarios
        """
        # criando o log
        fmsg = f'{CsvExcelExtractor.__name__}.{self.prepara_tabela.__name__}'
        logger = log.createLogger(fmsg)

        # tratando os nomes a ser utilizado para o path e extensao
        folder = key.split("/")[0]
        json = config.get(folder)
        if not json:
            logger.erro("nenhum json configurado pra essa extracao sql")
            raise NotImplementedError("nenhum json configurado pra essa extracao sql")

        ext = key.split('.')[-1].lower()
        key2 = key.split("/")[-1]
        avrsch_json = parse(dumps(json["avro_schema"]))
        header_json = {i['name']: i['logicalType'] if 'logicalType' in i.keys() else
                       i['type'] for i in json['avro_schema']['fields']}

        def read_csv():
            dataframe = pd.read_csv(file, header=json['csv']['header'], encoding=json['csv']['encoding'],
                                    sep=json['csv']['sep'], low_memory=json['csv']['low_memory'])
            return dataframe

        def read_excel():
            dataframe = pd.read_excel(file)
            return dataframe

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
            logger.error(f"{key2} o arquivo nao é csv/excel")
            raise NotImplementedError(f"{key2} o arquivo nao é csv/excel")

            # renomeando colunas para ficarem iguais ao esperado pelo avro
        if json.get('column_renames'):
            df.rename(columns={
                i: j for i, j in zip([df.filter(regex=name).columns.tolist()[0] for name in json.get('column_renames').keys()],
                                     json.get('column_renames').values())}, inplace=True)
        # inserindo o path
        df[json["colunapath"]] = key2

        # utilizando dados do avro
        floatconversion = [i for i, j in header_json.items() if 'float' in j]
        floatconversion_df = list(df.columns.intersection(floatconversion))
        int_repasse = [i for i, j in header_json.items() if 'int' in j]
        preencher = floatconversion + int_repasse
        column_dates = [i for i, j in header_json.items() if 'date' in j]
        column_dates_df = list(df.columns.intersection(column_dates))
        timestamp_dates = [i for i, j in header_json.items() if "timestamp-millis" in j]
        timestamp_dates_df = list(df.columns.intersection(timestamp_dates))
        column_boolean = [i for i, j in header_json.items() if 'boolean' in j]
        column_boolean_df = list(df.columns.intersection(column_boolean))

        # trocando o nan para null
        df = df.where((pd.notnull(df)), None)

        # tratando colunas com base no tipo
        for float_column in floatconversion_df:
            # df[f"{float_column}{json['nome_original']}"] = df[float_column] ORIGINAL COLUMN DEPRECATED
            if 'float' not in str(df[float_column].dtype).lower():
                df[float_column] = df[float_column].apply(convert_decimal)

        for float_int_column in preencher:
            if float_int_column not in df.columns:
                df[float_int_column] = 0
            df[float_int_column].fillna(0, inplace=True)

        for date_column in column_dates_df:
            df[date_column] = pd.to_datetime(df[date_column], format="mixed", errors='coerce').dt.strftime('%Y-%m-%d')
            
        for timestamp_column in timestamp_dates_df:
            df[timestamp_column] = pd.to_datetime(df[timestamp_column], format="mixed", errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S.%f')
            
        for bool_column in column_boolean_df:
            if 'boolean' not in str(df[bool_column].dtype).lower():
                df[bool_column] = df[bool_column].apply(convert_boolean)

        # consertando os NaT gerados pelo to_datetime
        df = df.where((pd.notnull(df)), None)
        # criando o dicionario de retorno em que o programa ira trabalhar
        tabelas = [{"name": f"{json['name']}{key2.split('.')[0]}", "df": df,
                    "schema": avrsch_json, "header": header_json,
                    "s3key": json['s3key']}]
        logger.info(f'extracao {key2} realizada com sucesso')
        return tabelas
