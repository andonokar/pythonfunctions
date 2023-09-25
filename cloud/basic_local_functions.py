import yaml
from exceptions import exceptions


def read_yaml_local(bucket: str, key: str) -> dict:
    """
    Read YAML file in s3 bucket
    :param bucket: s3 bucket name
    :param key: filename
    :return: YAML file as a python dictionary
    """
    try:
        with open(f'{bucket}/{key}') as yaml_file:
            yaml_object = yaml.safe_load(yaml_file)
    except Exception as err:
        raise exceptions.YamlReadingError(f'Error while reading Bucket={bucket}, Key={key}: {err}')

    return yaml_object
