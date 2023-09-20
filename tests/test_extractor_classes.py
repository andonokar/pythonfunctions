from globalresources.extraction_classes import CsvExcelExtractor
from tests.variables_test import pdf_normal_tec, pdf_int_problem
import avro


def test_csvexcelextractor_correct_schema():
    header = {
        "column1": 'int',
        "column2": 'date',
        "column3": 'float',
        "column4": 'string',
        "column5": 'timestamp-millis',
        "column6": 'boolean'
    }
    testclass = CsvExcelExtractor()
    result = testclass.prepara_tabela(pdf_normal_tec)
    assert result[0].name == 'teste_test'
    assert isinstance(result[0].schema, avro.schema.RecordSchema)
    assert result[0].header == header
    assert result[0].s3key == 'TESTE/'
    assert all(isinstance(val, int) for val in result[0].df['column1'].to_list())
    assert all(isinstance(val, str) for val in result[0].df['column2'].to_list())
    assert all(isinstance(val, float) for val in result[0].df['column3'].to_list())
    assert all(isinstance(val, str) for val in result[0].df['column4'].to_list())
    assert all(isinstance(val, str) for val in result[0].df['column5'].to_list())
    assert all(isinstance(val, bool) for val in result[0].df['column6'].to_list())


def test_csvexcelextractor_int_problem():
    header = {
        "column1": 'int',
        "column2": 'date',
        "column3": 'float',
        "column4": 'string',
        "column5": 'timestamp-millis',
        "column6": 'boolean'
    }
    testclass = CsvExcelExtractor()
    result = testclass.prepara_tabela(pdf_int_problem)
    assert result[0].name == 'teste_test'
    assert isinstance(result[0].schema, avro.schema.RecordSchema)
    assert result[0].header == header
    assert result[0].s3key == 'TESTE/'
    assert all(isinstance(val, int) for val in result[0].df['column1'].to_list())
    assert all(isinstance(val, str) for val in result[0].df['column2'].to_list())
    assert all(isinstance(val, float) for val in result[0].df['column3'].to_list())
    assert all(isinstance(val, str | None) for val in result[0].df['column4'].to_list())
    assert all(isinstance(val, str) for val in result[0].df['column5'].to_list())
    assert all(isinstance(val, bool) for val in result[0].df['column6'].to_list())
