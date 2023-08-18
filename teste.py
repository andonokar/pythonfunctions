import yaml
from avro.schema import parse
import json
with open('templates/kafka_config.yaml', 'r') as r:
    config = yaml.safe_load(r)

print(type(parse(json.dumps(config['avro_log_schema']))))
