import numpy as np
import pandas as pd
from globalresources.process_dataframe import ProcessDataFrame
from datetime import datetime, timedelta
_data = {
    'int_column': np.random.randint(1, 100, size=10),
    'date_column': pd.date_range(start='2023-01-01', periods=10, freq='D'),
    'float_column': np.random.rand(10),
    'string_column': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J'],
    'timestamp_column': pd.to_datetime(['2023-09-18 10:00:00', '2023-09-18 11:00:00', '2023-09-18 12:00:00', '2023-09-18 13:00:00', '2023-09-18 14:00:00', '2023-09-18 15:00:00', '2023-09-18 16:00:00', '2023-09-18 17:00:00', '2023-09-18 18:00:00', '2023-09-18 19:00:00']),
    'bool_column': [True, False, True, False, True, False, True, False, True, False]
}

_data_problem = {
    'int_column': [1, 2, 3, 4, '5', 6, 7, 8, 9, None],
    'date_column': pd.date_range(start='2023-01-01', periods=10, freq='D'),
    'float_column': [1.1, 2.2, 3.3, '4.4', '5,5d', 6.6, 7.7, 8.8, 9.9, None],
    'string_column': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', None],
    'timestamp_column': pd.to_datetime(['2023-09-18 10:00:00', '2023-09-18 11:00:00', '2023-09-18 12:00:00', '2023-09-18 13:00:00', '2023-09-18 14:00:00', '2023-09-18 15:00:00', '2023-09-18 16:00:00', '2023-09-18 17:00:00', '2023-09-18 18:00:00', '2023-09-18 19:00:00']),
    'bool_column': [True, False, True, False, True, 'False', True, False, True, False]
}

_data_problem_2mistakes = {
    'int_column': [1, 2, 3, 4, '5', 6, 7, 8, 9, None],
    'date_column': pd.date_range(start='2023-01-01', periods=10, freq='D'),
    'float_column': [1.1, 2.2, 3.3, '4.4', '5,5d', 6.6, 7.7, 8.8, 9.9, None],
    'string_column': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', None],
    'timestamp_column': pd.to_datetime(['2023-09-18 10:00:00', None, '2023-09-18 12:00:00', '2023-09-18 13:00:00', '2023-09-18 14:00:00', '2023-09-18 15:00:00', '2023-09-18 16:00:00', '2023-09-18 17:00:00', '2023-09-18 18:00:00', '2023-09-18 19:00:00']),
    'bool_column': [True, False, True, False, True, 'False', True, False, True, False]
}

_original_schema = {
  "type": "record",
  "name": "DataFrame",
  "fields": [
    {"name": "int_column", "type": "int"},
    {"name": "date_column", "type": "string", "logicalType": "date"},
    {"name": "float_column", "type": "float"},
    {"name": "string_column", "type": "string"},
    {"name": "timestamp_column", "type": "string", "logicalType": "timestamp-millis"},
    {"name": "bool_column", "type": "boolean"}
  ]
}


_avro_schema = {
  "type": "record",
  "name": "DataFrame",
  "fields": [
    {"name": "column1", "type": "int"},
    {"name": "column2", "type": "string", "logicalType": "date"},
    {"name": "column3", "type": "float"},
    {"name": "column4", "type": "string"},
    {"name": "column5", "type": "string", "logicalType": "timestamp-millis"},
    {"name": "column6", "type": "boolean"}
  ]
}


_configs_regex = {
    "rename_type": "regex",
    "column_renames": {
        'int_column': 'column1',
        "date_column": "column2",
        "float_column": "column3",
        "string_column": "column4",
        "timestamp_column": "column5",
        "bool_column": "column6"
    },
    'avro_schema': _avro_schema
}

_configs_normal = {
    "rename_type": "normal",
    "column_renames": {
        'int_column': 'column1',
        "date_column": "column2",
        "float_column": "column3",
        "string_column": "column4",
        "timestamp_column": "column5",
        "bool_column": "column6"
    },
    'avro_schema': _avro_schema,
    'name': "teste_",
    's3key': "TESTE/"
}

_configs_index = {
    "rename_type": "index",
    "column_renames": {
        "0": 'column1',
        "1": "column2",
        "2": "column3",
        "3": "column4",
        "4": "column5",
        "5": "column6"
    },
    'avro_schema': _avro_schema
}

COLUMNS = ['column1', 'column2', 'column3', 'column4', 'column5', 'column6']
_df = pd.DataFrame(_data)
_df_problem = pd.DataFrame(_data_problem)
_df_problem_2mistakes = pd.DataFrame(_data_problem_2mistakes)
pdf_regex = ProcessDataFrame(_df, config=_configs_regex, name='test')
pdf_index = ProcessDataFrame(_df, config=_configs_index, name='test')
pdf_no_rename = ProcessDataFrame(_df, config=_original_schema, name='test')
pdf_normal_tdc = ProcessDataFrame(_df, config=_configs_normal, name='test')
pdf_normal_tec = ProcessDataFrame(_df, config=_configs_normal, name='test')
pdf_normal_avro = ProcessDataFrame(_df, config=_configs_normal, name='test')
pdf_int_problem = ProcessDataFrame(_df_problem, config=_configs_normal, name='test')
pdf_int_problem_avro = ProcessDataFrame(_df_problem, config=_configs_normal, name='test')
pdf_int_problem_avro_2mistakes = ProcessDataFrame(_df_problem_2mistakes, config=_configs_normal, name='test')

excel_wrong_conf = {
    "excel": {
    }
}

excel_conf = {
    "excel": {
        'header': 0
    }
}

csv_wrong_conf = {
    "csv": {
    }
}

csv_conf = {
    "csv": {
        'header': 0
    }
}


avro_config = {
    'bucket_avro': 'teste',
    'bucket_errors': 'teste_erro'
}


# def generate_random_data(schema, num_records):
#     dicionario = {}
#     for field in schema["fields"]:
#         field_name = field["name"]
#         field_type = field["type"]
#         if field_type == "int":
#             dicionario[field_name] = np.random.randint(1, 100, size=num_records)
#         elif field_type == "string":
#             if "logicalType" in field and field["logicalType"] == "date":
#                 start_date = datetime(2023, 1, 1)
#                 end_date = datetime(2023, 12, 31)
#                 date_range = [start_date + timedelta(days=np.random.randint((end_date - start_date).days)) for _ in range(num_records)]
#                 dicionario[field_name] = [date.strftime("%Y-%m-%d") for date in date_range]
#             elif "logicalType" in field and field["logicalType"] == "timestamp-millis":
#                 dicionario[field_name] = [datetime(2023, 9, 18) + timedelta(minutes=np.random.randint(1440)) for _ in range(num_records)]
#             else:
#                 dicionario[field_name] = [chr(65 + np.random.randint(26)) for _ in range(num_records)]
#         elif field_type == "float":
#             dicionario[field_name] = np.random.rand(num_records)
#         elif field_type == "boolean":
#             dicionario[field_name] = [bool(np.random.randint(2)) for _ in range(num_records)]
#     return dicionario
#
#
# # Generate a DataFrame with a million records
# num_record = 10000
# data = generate_random_data(_original_schema, num_record)
# df_million = pd.DataFrame(data)
# pdf_million = ProcessDataFrame(df_million, config=_configs_normal, name='test')

