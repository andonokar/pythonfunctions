from cloud.basic_s3_functions import read_json_from_s3_object
config = read_json_from_s3_object("bucket-configuration-name", "default-configuration-keys")
kafka_config = read_json_from_s3_object("bucket-configuration-name", "default-configuration-keys")
