# importando as classes
import fastavro
from datetime import datetime
from cloud.basic_s3_functions import save_file_to_s3_bucket2
import csv
from util import log


class Escrita:

    def __init__(self, tables: list[dict], conf: dict) -> None:
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



    @log.logs
    def escreve(self):
        """
        realiza a escrita no avro e, se necessario, no csv para a tabela de geracao
        """
        # abrindo o writer
        mistakes = []
        for data in self.tables:
            if len(data['df'].index) != 0:
                avropath = f"/tmp/{data['name']}.avro"

                # Create Avro schema and open Avro file for writing
                with open(avropath, 'wb') as avro_file:
                    avro_schema = data["schema"]
                    writer = fastavro.writer(avro_file, avro_schema)

                    # Create a list to store errors
                    errors = []
                    success = 0
                    mistake = 0

                    # Iterate through DataFrame rows for type casting and validation
                    for index, row in data['df'].iterrows():
                        try:
                            # Type casting and basic validation
                            avro_record = {i:
                                           None if row[i] is None else
                                           int(float(row[i])) if 'int' in j else
                                           float(row[i]) if 'float' in j else
                                           row[i] if 'boolean' in j else
                                           str(row[i])
                                           for i, j in data["header"].items() if i in data['df'].columns}
                            # Write the Avro record
                            writer.write(avro_record)
                            success += 1
                        except Exception as err:
                            # Record the error and the row that caused it
                            error_row = list(row)
                            error_row.append(str(err))
                            errors.append(error_row)
                            mistake += 1

                    # Close the Avro writer
                    writer.close()
                now = datetime.now()
                date_str = now.strftime('%Y-%m-%d')
                time_str = now.strftime('%H-%M')
                show = f"{date_str}-{time_str}"
                # criando o csv caso a tabela tenha algum dado errado
                if len(errors) > 0:
                    # montando o cabecalho do csv
                    csvpath = f"/tmp/{data['name']}.csv"
                    headercsv = list(data['df'].columns)
                    headercsv.append('erro')
                    errors.insert(0, headercsv)
                    with open(csvpath, 'w', newline='') as csvarquivo:
                        # inserindo os erros no csv
                        writer = csv.writer(csvarquivo)
                        writer.writerows(errors)
                    save_file_to_s3_bucket2(csvpath, self.bucketerrors, f'{data["s3key"]}{show}_{data["name"]}.csv')
                save_file_to_s3_bucket2(avropath, self.bucket, f'{data["s3key"]}{show}_{data["name"]}.avro')
                mistakes.append(mistake)
        return mistakes
