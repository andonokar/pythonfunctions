from globalresources.readredirect import read_and_redirect
from io import BytesIO
from cloud.cria_provider import CriaProvider


def test_read_and_redirect():
    with open('tests/dummy_files/addresses.csv', 'rb') as file:
        content = file.read()
    reader = CriaProvider().create_provider('local')
    buffer = BytesIO(content)
    depara_config = reader.read_yaml_from_file("tests", "dummy_files/test_yaml_class.yaml")
    read_and_redirect('tests', buffer, 'dummy_files/addresses.csv', depara_config)
