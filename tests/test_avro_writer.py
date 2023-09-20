from avro_writer import Escrita
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from globalresources.extraction_classes import CsvExcelExtractor
from tests.variables_test import pdf_normal_avro, avro_config, pdf_int_problem_avro, pdf_int_problem_avro_2mistakes
from io import BytesIO


def test_avro_writer():
    testclass = CsvExcelExtractor()
    result = testclass.prepara_tabela(pdf_normal_avro)
    test_class = Escrita(result, avro_config)
    buffer = BytesIO()
    writer = DataFileWriter(buffer, DatumWriter(), test_class.tables[0].schema)
    mistake, erros = test_class._write_avro(test_class.tables[0], writer)
    assert mistake == 0


def test_avro_writer_with_schema_issues():
    testclass = CsvExcelExtractor()
    result = testclass.prepara_tabela(pdf_int_problem_avro)
    test_class = Escrita(result, avro_config)
    buffer2 = BytesIO()
    writer = DataFileWriter(buffer2, DatumWriter(), test_class.tables[0].schema)
    mistake, erros = test_class._write_avro(test_class.tables[0], writer)
    assert mistake == 1


def test_avro_writer_with_2schema_issues():
    testclass = CsvExcelExtractor()
    result = testclass.prepara_tabela(pdf_int_problem_avro_2mistakes)
    test_class = Escrita(result, avro_config)
    buffer3 = BytesIO()
    writer = DataFileWriter(buffer3, DatumWriter(), test_class.tables[0].schema)
    mistake, erros = test_class._write_avro(test_class.tables[0], writer)
    assert mistake == 2
