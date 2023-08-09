from cloud.basic_s3_functions import read_yaml_from_s3_object
depara_config = read_yaml_from_s3_object("bucket-configuration-name", "default-configuration-keys")
kafka_config = read_yaml_from_s3_object("bucket-configuration-name", "default-configuration-keys")
