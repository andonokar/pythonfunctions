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

kafka_config = {
    'client.id': 'client-id',
    'bootstrap.servers': "my-cluster-kafka-bootstrap:9092",
    'enable.idempotence': 'true',
    'acks': 'all',
    'linger.ms': 100,
}
producer = Producer(kafka_config)

avro_schema_str = """
{
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "data_hora", "type": "string"},
        {"name": "nome_arquivo", "type": "string"},
        {"name": "mensagem", "type": "string"},
        {"name": "log_mensagem", "type": "string"},
        {"name": "S3_ini", "type": "string"},
        {"name": "S3_fim", "type": "string"},
        {"name": "cliente", "type": "string"},
        {"name": "etapa", "type": "string"}
    ]
}
"""
schema_registry_conf = {'url': 'http://schema-registry:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
avro_serializer = AvroSerializer(schema_registry_client, avro_schema_str)


def createloggerforkafka(source_log: str = __name__, topic: str = 'logs', **kwargs):
    """
    Função para criar um ponto de observação através do uso de logs

    :return:
    """
    information = kwargs
    log_format = '%(levelname)-8s||%(asctime)s||%(name)-12s||%(lineno)d||%(message)s'
    logging.basicConfig(level=logging.INFO, format=log_format)
    logger = logging.getLogger(source_log)

    def send_log_to_kafka(record):
        message_payload = {
            "data_hora": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "mensagem": record.getMessage(),
            "log_mensagem": record.levelname
        }
        producer.produce(topic=topic, value=avro_serializer(message_payload, SerializationContext(topic, MessageField.VALUE)))
        producer.flush()

    # Remove the default logging handler
    logger.handlers = []

    # Add a new Kafka logging handler
    kafka_handler = logging.StreamHandler()
    kafka_handler.emit = send_log_to_kafka
    logger.addHandler(kafka_handler)

    return logger
