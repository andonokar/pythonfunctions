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


class SQL:
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
        fmsg = f'{SQL.__name__}.{self.prepara_tabela.__name__}'
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

        # iniciando a extracao
        try:
            if "csv" in ext:
                df = pd.read_csv(file, header=json['csv']['header'], encoding=json['csv']['encoding'],
                                 sep=json['csv']['sep'], low_memory=json['csv']['low_memory'], dtype=str)
            else:
                df = pd.read_excel(file, dtype=str)
        except Exception as err:
            logger.error(f"{key2} não conseguiu ser lido: {err}")
            raise NameError(f"{key2} do arquivo não conseguiu ser lido: {err}")
            # renomeando colunas para ficarem iguais ao esperado pelo avro
        try:
            df.rename(columns=dict(zip(df.columns, header_json.keys())), inplace=True)
        except Exception as err:
            logger.error(f"{key2} falha renomear colunas: {err}")
            raise KeyError(f"{key2} falha renomear colunas: {err}")
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
        id_columns = [i for i in header_json.keys() if "id_" in i]
        id_columns_df = list(df.columns.intersection(id_columns))

        # trocando o nan para null
        df = df.where((pd.notnull(df)), None)
        # tratando colunas com base no tipo
        if len(floatconversion_df) > 0:
            for coluna in floatconversion_df:
                df[f"{coluna}{json['nome_original']}"] = df[coluna]
                df[coluna] = df[coluna].apply(convert_decimal)

        for coluna2 in preencher:
            if coluna2 not in df.columns:
                df[coluna2] = 0
            df[coluna2].fillna(0, inplace=True)

        for coluna5 in column_boolean_df:
            df[coluna5] = df[coluna5].apply(convert_boolean)

        for coluna4 in timestamp_dates_df:
            df[coluna4] = pd.to_datetime(df[coluna4], format="mixed", errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S.%f')

        for coluna3 in column_dates_df:
            df[coluna3] = pd.to_datetime(df[coluna3], format="mixed", errors='coerce').dt.strftime('%Y-%m-%d')

        for coluna6 in id_columns_df:
            df[coluna6].fillna(0, inplace=True)
            try:
                df[coluna6] = df[coluna6].astype(float).astype(int)
            except Exception as err:
                logger.warning(f"id do campo {coluna6} nao e inteiro: {err}")
        # consertando os NaT gerados pelo to_datetime
        df = df.where((pd.notnull(df)), None)
        # resetando os indices para que nao se tenha problema na iteracao linha a linha
        df.reset_index()
        # criando o dicionario de retorno em que o programa ira trabalhar
        tabelas = [{"name": f"{json['name']}{key2.split('.')[0]}", "df": df,
                    "schema": avrsch_json, "header": header_json,
                    "s3key": json['s3key']}]
        logger.info(f'extracao {key2} realizada com sucesso')
        return tabelas
