from dataclasses import dataclass
import avro.schema
import pandas as pd


@dataclass
class TreatedDataFrame:
    """classe para receber os dados processados depois da extracao para passar para a escrita do avro"""
    name: str
    df: pd.DataFrame
    schema: avro.schema.RecordSchema
    header: dict
    s3key: str
