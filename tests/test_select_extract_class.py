import pytest
from globalresources.select_extraction_class import SelectClassExtraction
import yaml


def test_select_extract_class():
    with open("tests/dummy_files/test_client_class.yaml") as file:
        response = yaml.safe_load(file)
    test_class = SelectClassExtraction()
    result = test_class.get_class(response['dummy_files'])
    assert type(result).__name__ == 'CsvExcelExtractor'


def test_select_extract_class_wrong_extractor():
    with pytest.raises(NotImplementedError):
        with open("tests/dummy_files/test_client_class.yaml") as file:
            response = yaml.safe_load(file)
        test_class = SelectClassExtraction()
        response['dummy_files']['class'] = 'pao'
        result = test_class.get_class(response['dummy_files'])
        assert type(result).__name__ == 'CsvExcelExtractor'


def test_select_extract_class_empty():
    with open("tests/dummy_files/test_client_class.yaml") as file:
        response = yaml.safe_load(file)
    test_class = SelectClassExtraction()
    response['dummy_files'].pop('class')
    result = test_class.get_class(response['dummy_files'])
    assert type(result).__name__ == 'CsvExcelExtractor'
