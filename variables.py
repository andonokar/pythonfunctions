from cloud.cria_provider import CriaProvider
reader = CriaProvider().create_provider('aws')
depara_config = reader.read_yaml_from_file("csn-configuration-layer-prd-9154-8417-5192", "depara_conf.yaml")
kafka_config = {}
auth_config = {}
kafka_options = depara_config.get('kafka')
auth_options = depara_config.get('authentication')

if kafka_options:
    kafka = kafka_options.get('active', False)
    if kafka:
        kafka_config = reader.read_yaml_from_file(kafka_options.get('bucket'), kafka_options.get('key'))


if auth_options:
    auth = auth_options.get('active', False)
    if auth:
        auth_config = reader.read_yaml_from_file(auth_options.get('bucket'), auth_options.get('key'))
