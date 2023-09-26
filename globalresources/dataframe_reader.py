from io import BytesIO
import pandas as pd
from globalresources.process_dataframe import ProcessDataFrame


def _read_excel(file: str | BytesIO, config: dict) -> pd.DataFrame:
    """
    metodo para ler um excel com base no arquivo de configuracao
    :param file: o arquivo excel
    :param config: o arquivo de configuracao
    :return: dataframe
    """
    excel_options = config.get('excel')
    if not excel_options:
        raise KeyError('chave excel ausente na configuracao yaml')
    try:
        header = excel_options['header']
    except KeyError:
        raise KeyError('a chave header deve ser especificado')
    dataframe = pd.read_excel(file, header=header)
    return dataframe


def _read_csv(file: str | BytesIO, config: dict) -> pd.DataFrame:
    """
    metodo para ler um csv com base no arquivo de configuracao
    :param file: o arquivo csv
    :param config: o arquivo de configuracao
    :return: dataframe
    """
    csv_options = config.get('csv')
    if not csv_options:
        raise KeyError('chave csv ausente na configuracao yaml')
    try:
        header = csv_options['header']
    except KeyError:
        raise KeyError('a chave header deve ser especificado')
    encoding = csv_options.get('encoding', 'UTF-8')
    sep = csv_options.get('sep', ',')
    low_memory = csv_options.get('low_memory', False)
    dataframe = pd.read_csv(file, header=header, encoding=encoding, sep=sep, low_memory=low_memory)
    return dataframe


def read_dataframe(file: str | BytesIO, key: str, config: dict) -> ProcessDataFrame:
    """
    instancia a classe ProcessDataFrame com o dataframe e o arquivo de configuracao
    :param file: o arquivo a virar dataframe
    :param key: o nome do arquivo
    :param config: o arquivo de configuracao
    :return: classe ProcessDataFrame instanciada
    """
    depara = {
        "csv": _read_csv,
        'xls': _read_excel,
        'xlsb': _read_excel,
        'xlsm': _read_excel,
        'xlsx': _read_excel,
    }
    ext = key.split('.')[-1].lower()
    method = depara.get(ext)
    key2 = key.split("/")[-1]

    # iniciando a extracao
    if method:
        df = method(file, config)
    else:
        raise NotImplementedError(f"{key2} o arquivo nao e csv/excel")

    return ProcessDataFrame(df, config, key2)
