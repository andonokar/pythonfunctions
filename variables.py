from globalresources.yaml_reader import YamlReader
reader = YamlReader('aws')
depara_config = reader.read_yaml_file("test-conf-domrock", "depara_conf.yaml")
kafka_config = reader.read_yaml_file("test-conf-domrock", "kafka_config.yaml")
