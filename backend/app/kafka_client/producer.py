import json
import logging
from datetime import datetime
import uuid
from typing import Optional

from confluent_kafka import Producer
from confluent_kafka.serialization import (
    SerializationContext, 
    MessageField
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer

from kafka_client.schemas import json_schema_str

logger = logging.getLogger(__name__)


class MessageProducer:
    def __init__(self, bootstrap_servers: str, topic: str, schema_registry_url: str):
        """
        Инициализация продюсера с поддержкой Schema Registry
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        
        # Инициализация Schema Registry Client
        schema_registry_conf = {'url': schema_registry_url}
        self.schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        
        # Создание JSON сериализатора с Schema Registry
        self.json_serializer = JSONSerializer(json_schema_str, self.schema_registry_client)
        
        # Конфигурация продюсера
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'python-producer',
            'acks': 'all',
            'retries': 3,
            'enable.idempotence': True,
            'compression.type': 'gzip'
        }
        
        self.producer = Producer(conf)
    
    def delivery_callback(self, err, msg):
        """Callback для отслеживания доставки сообщений"""
        if err:
            logger.error(f'Ошибка доставки сообщения: {err}')
            print(f"Ошибка доставки: {err}")
        else:
            logger.info(f'Сообщение доставлено в {msg.topic()} [{msg.partition()}]')
            print(f"Сообщение доставлено: topic={msg.topic()}, partition={msg.partition()}, offset={msg.offset()}")
    
    def send_message(self, key: str, message: str) -> bool:
        """
        Отправка сообщения в Kafka с использованием Schema Registry
        """
        try:
            # Создание структурированного сообщения
            message_value = {
                'id': str(uuid.uuid4()),
                'timestamp': datetime.now().isoformat(),
                'key': key,
                'msg': message
            }
            
            print(f"Отправка сообщения в Kafka: {message_value}")
            
            # Сериализация значения с использованием Schema Registry
            serialized_value = self.json_serializer(
                message_value, 
                SerializationContext(self.topic, MessageField.VALUE)
            )
            
            # Сериализация ключа (просто строку в bytes)
            serialized_key = key.encode('utf-8') if key else None
            
            # Отправка сообщения
            self.producer.produce(
                topic=self.topic,
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
            logger.error(f"Ошибка при отправке сообщения в Kafka: {e}")
            print(f"Ошибка при отправке: {e}")
            return False
    
    def close(self):
        """Закрытие продюсера"""
        try:
            self.producer.flush(timeout=10)
        except Exception as e:
            logger.error(f"Ошибка при закрытии продюсера: {e}")

# Глобальный экземпляр продюсера
producer_instance: Optional[MessageProducer] = None

def init_producer(bootstrap_servers: str, topic: str, schema_registry_url: str) -> MessageProducer:
    """Инициализация глобального продюсера"""
    global producer_instance
    if producer_instance is None:
        producer_instance = MessageProducer(bootstrap_servers, topic, schema_registry_url)
    return producer_instance

def get_producer() -> Optional[MessageProducer]:
    """Получение глобального продюсера"""
    return producer_instance
