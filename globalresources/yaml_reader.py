from cloud.basic_s3_functions import read_yaml_from_s3_object
from cloud.basic_local_functions import read_yaml_local


class YamlReader:
    """classe para ler yamls dado um cloud provider"""
    _options = {
        'aws': read_yaml_from_s3_object,
        'local': read_yaml_local
    }

    def __init__(self, provider: str):
        if provider not in self._options.keys():
            raise NotImplementedError(f'the {provider} provider is not accepted')
        self._reader = self._options.get(provider)

    def read_yaml_file(self, bucket: str, key: str):
        return self._reader(bucket, key)
