from globalresources.yaml_reader import YamlReader


def test_yaml_reader():
    reader = YamlReader('local')
    result = reader.read_yaml_file('tests', 'dummy_files/test_yaml_class.yaml')
    assert result == {'local': {'tests': {'bucket': 'tests', 'key': 'dummy_files/test_client_class.yaml'}}}
