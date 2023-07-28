from cloud.AWS.bucket.s3 import read_json_from_s3_object
config = read_json_from_s3_object("bucket-configuration-name", "default-configuration-keys")
