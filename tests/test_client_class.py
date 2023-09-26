from globalresources.reader_client import Client
from cloud.cria_provider import CriaProvider
import pytest
import yaml

from exceptions import exceptions


def test_valitade_depara_config():
    client = Client('tests', 'dummy_files/addresses.csv')
    result = client._validate_depara_config(
        {'tests': {'bucket': 'tests', 'key': 'dummy_files/test_client_class.yaml'}})
    assert result == {'bucket': 'tests', 'key': 'dummy_files/test_client_class.yaml'}


def test_valitade_depara_config_error():
    with pytest.raises(NotImplementedError):
        client = Client('test', 'dummy_files/addresses.csv')
        result = client._validate_depara_config(
            {'tests': {'bucket': 'tests', 'key': 'dummy_files/test_client_class.yaml'}})
        assert result == {'bucket': 'tests', 'key': 'dummy_files/test_client_class.yaml'}


def test_read_client_conf_wrong_keys():
    with pytest.raises(KeyError):
        reader = CriaProvider().create_provider('local')
        client = Client('tests', 'dummy_files/addresses.csv')
        client._read_client_conf({'buckets': 'tests', 'key': 'dummy_files/test_client_class.yaml'}, reader)
    with pytest.raises(KeyError):
        reader = CriaProvider().create_provider('local')
        client = Client('tests', 'dummy_files/addresses.csv')
        client._read_client_conf({'bucket': 'tests', 'keys': 'dummy_files/test_client_class.yaml'}, reader)


def test_read_client_conf_wrong_file():
    with pytest.raises(exceptions.YamlReadingError):
        reader = CriaProvider().create_provider('local')
        client = Client('tests', 'dummy_files/addresses.csv')
        client._read_client_conf({'bucket': 'tests', 'key': 'dummy_files/test_client_classwrong.yaml'}, reader)


def test_read_client_conf():
    reader = CriaProvider().create_provider('local')
    client = Client('tests', 'dummy_files/addresses.csv')
    result = client._read_client_conf({'bucket': 'tests', 'key': 'dummy_files/test_client_class.yaml'}, reader)
    with open('tests/dummy_files/test_client_class.yaml', 'r') as file:
        check = yaml.safe_load(file)
    assert result == check


def test_validade_folder_conf():
    client = Client('tests', 'dummy_files/addresses.csv')
    with open('tests/dummy_files/test_client_class.yaml', 'r') as file:
        dicionario = yaml.safe_load(file)
    result = client._validate_folder_conf(dicionario)
    assert result == dicionario['dummy_files']


def test_validade_folder_conf_wrong():
    with pytest.raises(NotImplementedError):
        client = Client('tests', 'dummy_file/addresses.csv')
        with open('tests/dummy_files/test_client_class.yaml', 'r') as file:
            dicionario = yaml.safe_load(file)
        result = client._validate_folder_conf(dicionario)
        assert result == dicionario['dummy_files']


def test_def_validate_escrita_conf():
    client = Client('tests', 'dummy_files/addresses.csv')
    with open('tests/dummy_files/test_client_class.yaml', 'r') as file:
        dicionario = yaml.safe_load(file)
    result = client._validate_escrita_conf(dicionario)
    assert result == dicionario['escrita']


def test_get_conf():
    client = Client('tests', 'dummy_files/addresses.csv')
    reader = CriaProvider().create_provider('local')
    result = client.get_conf({'tests': {'bucket': 'tests', 'key': 'dummy_files/test_client_class.yaml'}}, reader)
    with open('tests/dummy_files/test_client_class.yaml', 'r') as file:
        dicionario = yaml.safe_load(file)
    assert result == (dicionario['escrita'], dicionario['dummy_files'])
