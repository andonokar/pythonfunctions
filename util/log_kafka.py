import logging
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
)
import json
from datetime import datetime
from variables import kafka_config


def createloggerforkafka(source_log: str = __name__, topic: str = 'logs', **kwargs):
    """
    Função para criar um ponto de observação através do uso de logs e enviar ao kafka
    :return:
    """
    producer = Producer(kafka_config.get('producer_config'))

    avro_schema_str = json.dumps(kafka_config.get('avro_log_schema'))

    schema_registry_conf = kafka_config.get('schema_registry')
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    avro_serializer = AvroSerializer(schema_registry_client, avro_schema_str)
    log_format = '%(levelname)-8s||%(asctime)s||%(name)-12s||%(lineno)d||%(message)s'
    logging.basicConfig(level=logging.INFO, format=log_format)
    logger = logging.getLogger(source_log)

    def send_log_to_kafka(record):

        message_payload = {
            "data_hora": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "mensagem": record.getMessage(),
            "log_mensagem": record.levelname
        }
        message_payload = {**message_payload, **kwargs}
        producer.produce(topic=topic, value=avro_serializer(message_payload, SerializationContext(topic, MessageField.VALUE)))
        producer.flush()

    # Remove the default logging handler
    logger.handlers = []

    # Add a new Kafka logging handler
    kafka_handler = logging.StreamHandler()
    kafka_handler.emit = send_log_to_kafka
    logger.addHandler(kafka_handler)

    return logger
