"""Продюсер кафка."""
from datetime import datetime
import uuid
from typing import Optional, Iterable

from confluent_kafka import Producer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer

from core.config import logger
from kafka_client.schemas import json_schema_str


class MessageProducer:
    """Продюсер кафка."""
    def __init__(
            self,
            bootstrap_servers: str,
            schema_registry_url: str,
            conf_extra: dict
    ):
        """
        Инициализация продюсера с поддержкой Schema Registry
        """
        self.bootstrap_servers = bootstrap_servers

        # Инициализация Schema Registry Client
        schema_registry_conf = {'url': schema_registry_url}
        self.schema_registry_client = SchemaRegistryClient(
            schema_registry_conf
        )

        # Создание JSON сериализатора с Schema Registry
        self.json_serializer = JSONSerializer(
            json_schema_str,
            self.schema_registry_client
        )

        # Конфигурация продюсера
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'python-producer',
            'acks': 'all',
            'retries': 3,
            'enable.idempotence': True,
            'compression.type': 'gzip',
            **conf_extra
        }

        self.producer = Producer(conf)

    def delivery_callback(self, err, msg):
        """Callback для отслеживания доставки сообщений"""
        if err:
            logger.error('Ошибка доставки сообщения: %s', err)
        else:
            logger.info(
                'Сообщение доставлено: topic=%s partition=%s offset=%s',
                msg.topic(), msg.partition(), msg.offset()
            )

    def _build_value(self, key: str, message: str) -> dict:
        """Создание структурированного сообщения."""
        return {
            'id': str(uuid.uuid4()),
            'timestamp': datetime.now().isoformat(),
            'key': key,
            'msg': message
        }

    def send_message(self, topic: str, key: str, message: str) -> bool:
        """
        Отправка одиночного сообщения в Kafka с использованием Schema Registry
        """
        try:
            message_value = self._build_value(key, message)

            logger.error('Отправка сообщения в Kafka: %s', message_value)

            # Сериализация значения с использованием Schema Registry
            serialized_value = self.json_serializer(
                message_value,
                SerializationContext(topic, MessageField.VALUE)
            )

            # Сериализация ключа (просто строку в bytes)
            serialized_key = key.encode('utf-8') if key else None

            # Отправка сообщения
            self.producer.produce(
                topic=topic,
                key=serialized_key,
                value=serialized_value,
                callback=self.delivery_callback
            )

            # Обработка событий
            self.producer.poll(0)

            # Принудительная отправка всех сообщений
            self.producer.flush(timeout=5)

            return True

        except Exception as e:
            logger.error('Ошибка при отправке сообщения в Kafka: %s', e)
            return False

    def send_batch(self, topic: str, items: Iterable[dict]) -> bool:
        """
        Отправка пакета сообщений в Kafka с использованием Schema Registry
        """
        try:
            for item in items:
                key = item.get("key")
                msg = item.get("msg")
                value = self._build_value(key=key, message=msg)

                serialized_value = self.json_serializer(
                    value,
                    SerializationContext(topic, MessageField.VALUE)
                )
                serialized_key = key.encode('utf-8') if key else None

                self.producer.produce(
                    topic=topic,
                    key=serialized_key,
                    value=serialized_value,
                    callback=self.delivery_callback
                )
                self.producer.poll(0)

            self.producer.flush(timeout=10)
            return True
        except Exception as e:
            logger.error("Ошибка пакетной отправки в Kafka: %s", e)
            return False

    def close(self):
        """Закрытие продюсера."""
        try:
            self.producer.flush(timeout=10)
        except Exception as e:
            logger.error('Ошибка при закрытии продюсера: %s', e)


# Глобальный экземпляр продюсера
producer_instance: Optional[MessageProducer] = None


def init_producer(
    bootstrap_servers: str,
    schema_registry_url: str,
    conf_extra: dict
) -> MessageProducer:
    """Инициализация глобального продюсера."""
    global producer_instance
    if producer_instance is None:
        producer_instance = MessageProducer(
            bootstrap_servers,
            schema_registry_url,
            conf_extra
        )
    return producer_instance


def get_producer() -> Optional[MessageProducer]:
    """Получение глобального продюсера"""
    return producer_instance
