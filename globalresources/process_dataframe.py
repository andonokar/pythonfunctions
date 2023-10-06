import pandas as pd
from avro.schema import parse
from json import dumps


class ProcessDataFrame:
    """
    Classe que armazena o dataframe e suas configuracoes de processamento
    """
    def __init__(self, df: pd.DataFrame, config: dict, name: str) -> None:
        """
        :param df: o dataframe a ser lido
        :param config: o arquivo de configuracao
            (https://domrock-dev.atlassian.net/wiki/spaces/DV/pages/3345809441/Documenta+o+para+subir+um+cliente+DLR+V4)
        :param name: o nome do arquivo do dataframe
        """
        self.df = df
        self.config = config
        self.avro_schema = None
        self.name = name

    def _rename_columns(self, column_renames: dict) -> None:
        """
        renomeia as colunas de forma padrao chave-valor
        :param column_renames: dicionario com o nome das colunas a serem renomeadas
        :return: none
        """
        self.df = self.df.rename(columns=column_renames)

    def _regex_rename_columns(self, column_renames: dict) -> None:
        """
        renomeia as colunas usando regex
        :param column_renames: dicionario com o nome das colunas a serem renomeadas
        :return: none
        """
        try:
            columns = {i: j for i, j in
                       zip([self.df.filter(regex=name).columns.tolist()[0] for name in column_renames.keys()],
                           column_renames.values())}
        except IndexError:
            raise NameError('um ou mais regex nao resgataram uma coluna para captura')
        self.df = self.df.rename(columns=columns)

    def _index_rename_columns(self, column_renames: dict) -> None:
        """
        renmeia as colunas pelo indice delas
        :param column_renames: dicionario com o nome das colunas a serem renomeadas
        :return: none
        """
        try:
            columns = {self.df.columns.tolist()[int(i)]: j for i, j in column_renames.items()}
        except IndexError:
            raise IndexError('o indice da coluna nao existe')
        self.df = self.df.rename(columns=columns)

    def rename_dataframe(self) -> pd.DataFrame:
        """
        funcao executada para renomear as colunas do dataframe com base no arquivo de configuracao
        :return: o dataframe renomeado
        """
        column_renames = self.config.get('column_renames')
        if column_renames:
            rename_functions = {
                'normal': self._rename_columns,
                'regex': self._regex_rename_columns,
                'index': self._index_rename_columns
            }
            rename_type = self.config.get('rename_type', 'normal')
            rename_function = rename_functions.get(rename_type)
            if not rename_function:
                raise NotImplementedError(f'o metodo {rename_type} para renomear colunas nao existe')
            rename_function(column_renames)
        return self.df

    def set_avro_schema(self):
        """
        executa o parse do schema do avro do arquivo de configuracao
        :return: o avro schema
        """
        try:
            avrsch_config = parse(dumps(self.config.get("avro_schema")))
        except Exception as err:
            raise Exception(f'Schema invalido para o avro: {err}')
        self.avro_schema = avrsch_config
        return self.avro_schema
