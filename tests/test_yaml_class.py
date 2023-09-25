from globalresources.yaml_reader import YamlReader
import pytest
from exceptions import exceptions


def test_yaml_reader():
    reader = YamlReader('local')
    result = reader.read_yaml_file('tests', 'dummy_files/test_yaml_class.yaml')
    assert result == {'local': {'tests': {'bucket': 'tests', 'key': 'dummy_files/test_client_class.yaml'}}}


def test_yaml_reader_wrong_option():
    with pytest.raises(NotImplementedError):
        reader = YamlReader('wrong')
        result = reader.read_yaml_file('tests', 'dummy_files/test_yaml_class.yaml')
        assert result == {'local': {'tests': {'bucket': 'tests', 'key': 'dummy_files/test_client_class.yaml'}}}


def test_yaml_file_not_found():
    with pytest.raises(exceptions.YamlReadingError):
        reader = YamlReader('local')
        result = reader.read_yaml_file('tests', 'dummy_files/test_yaml_clas.yaml')
        assert result == {'local': {'tests': {'bucket': 'tests', 'key': 'dummy_files/test_client_class.yaml'}}}
