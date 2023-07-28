import pandas as pd
import pyodbc
from datetime import datetime
from cloud.AWS.bucket.s3 import save_file_to_s3_bucket2
from util import log
import csv
import os


@log.logs
def extract_csv(database: dict):
    """
    This function connects to a database, executes a query and save the results as a csv in s3 with the datetime it was
    created
    :param database: dicionario com as informacoes das tabelas utilizadas
    keys:
        nome: name for the table
        connection: an string connector for server-database
            eg: DRIVER={ODBC Driver 18 for SQL Server};SERVER=server_ip;DATABASE=database_name;UID=user;PWD=password;TrustServerCertificate=yes
        sql_query: query to be made to the database
        chunk_size: the amount of chunks to break the query for maintaning small size
        bucket: the bucket in which the files will be saved
        filename: prefix of the csv file
        index: whether or not to preserve the index(row count)
        header: whether or not to preserve the header(column names)
        sep: the separator for csv file
        encoding: the encoding for csv file
    :return:
    """
    # criando o logger
    fmsg = f'{__name__}:{extract_csv.__name__}'
    logger = log.createLogger(fmsg)
    # tentando iniciar a conexao ao banco de dados
    connection_string = database["connection"]
    logger.info('iniciando a conexao com o banco de dados')
    try:
        cnxn = pyodbc.connect(connection_string)
    except Exception as err:
        logger.error(f'erro na conexao com o banco de dados: {err}')
        logger.warning(f'base {database["nome"]} ignorada devido a falha na conexao')
        return
    # fazendo a leitura da query
    sql_query = database["sql_query"]
    chunk_size = database["chunk_size"]
    try:
        df_iterator = pd.read_sql(sql_query, cnxn, chunksize=chunk_size)
    except Exception as err:
        logger.error(f"erro na execucao da query sql para da tabela {database['nome']}: {err}")
        logger.warning(f"devido ao erro da {database['nome']}, sera ignorada")
        return
    # pegando a hora pra colocar no arquivo
    now = datetime.now()
    date_str = now.strftime('%Y-%m-%d')
    time_str = now.strftime('%H-%M')
    show = f"{date_str}-{time_str}"
    # salvando os arquivos no s3
    for i, df in enumerate(df_iterator):
        if not df.empty:
            csv_path = f'/tmp/{database["filename"]}_{i+1}.csv'
            try:
                df.to_csv(csv_path, index=database['index'], header=database['header'], sep=database["sep"],
                          encoding=database["encoding"], quoting=csv.QUOTE_NONNUMERIC)
            except Exception as err:
                logger.error(f"erro no encoding pra base {database['nome']}: {err}")
                logger.warning(f'base {database["nome"]} ignorada devido ao erro de enconding')
                return
            save_file_to_s3_bucket2(csv_path, database["bucket"], f'{database["filename"]}/{database["filename"]}_{i+1}_{show}.csv')
            os.remove(csv_path)
        else:
            logger.warning("query resultou em um dataframe vazio")
    logger.info(f"extração {database['nome']} executado com sucesso")
