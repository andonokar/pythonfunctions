from cloud.basic_s3_functions import read_yaml_from_s3_object
depara_config = read_yaml_from_s3_object("test-conf-domrock", "depara_conf.yaml")
kafka_config = read_yaml_from_s3_object("test-conf-domrock", "kafka_config.yaml")
