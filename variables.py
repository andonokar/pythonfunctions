from cloud.cria_provider import CriaProvider
reader = CriaProvider().create_provider('aws')
depara_config = reader.read_yaml_from_file("test-conf-domrock", "depara_conf.yaml")
kafka_config = {}
auth_config = {}
try:
    kafka_options = depara_config['kafka']
    auth_options = depara_config['authentication']
except KeyError as err:
    raise KeyError(f'as configuracoes de kafka ou authentication nao existem: {err}')
kafka = kafka_options.get('active', False)
auth = auth_options.get('active', False)
if kafka:
    kafka_config = reader.read_yaml_from_file(kafka_options.get('bucket'), kafka_options.get('key'))
if auth:
    auth_config = reader.read_yaml_from_file(auth_options.get('bucket'), auth_options.get('key'))
