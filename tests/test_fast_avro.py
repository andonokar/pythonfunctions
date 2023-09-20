from side_ideas.fastavro_writer import Escrita
from globalresources.extraction_classes import CsvExcelExtractor
from tests.variables_test import pdf_normal_avro, avro_config, pdf_int_problem_avro, pdf_int_problem_avro_2mistakes
from io import BytesIO


def test_fastavro_writer():
    testclass = CsvExcelExtractor()
    result = testclass.prepara_tabela(pdf_normal_avro)
    test_class = Escrita(result, avro_config)
    buffer = BytesIO()
    mistake, erros = test_class._write_avro(test_class.tables[0], buffer)
    assert mistake == 0


def test_fastavro_writer_with_schema_issues():
    testclass = CsvExcelExtractor()
    result = testclass.prepara_tabela(pdf_int_problem_avro)
    test_class = Escrita(result, avro_config)
    buffer2 = BytesIO()
    mistake, erros = test_class._write_avro(test_class.tables[0], buffer2)
    assert mistake == 1


def test_fastavro_writer_with_2schema_issues():
    testclass = CsvExcelExtractor()
    result = testclass.prepara_tabela(pdf_int_problem_avro_2mistakes)
    test_class = Escrita(result, avro_config)
    buffer3 = BytesIO()
    mistake, erros = test_class._write_avro(test_class.tables[0], buffer3)
    assert mistake == 2


# def test_fastavro_writer_lot_of_data():
#     testclass = CsvExcelExtractor()
#     result = testclass.prepara_tabela(pdf_million)
#     test_class = Escrita(result, avro_config)
#     buffer4 = open('test_fastavro.avro', 'wb')
#     mistake, erros = test_class._write_avro(test_class.tables[0], buffer4)
#     assert mistake == 0
#     buffer4.close()
