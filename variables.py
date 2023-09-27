from cloud.cria_provider import CriaProvider
reader = CriaProvider().create_provider('aws')
depara_config = reader.read_yaml_from_file("test-conf-domrock", "depara_conf.yaml")
kafka_config = reader.read_yaml_from_file("test-conf-domrock", "kafka_config.yaml")
