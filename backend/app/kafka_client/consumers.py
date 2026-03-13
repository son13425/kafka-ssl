from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer, JSONDeserializer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField
)
import threading

from core.config import logger


class SingleMessageConsumer:
    """Консьюмер единичных сообщений."""
    def __init__(self, bootstrap_servers, topic, group_id, schema_registry_url, schema_str=None):
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,  # УНИКАЛЬНЫЙ group.id
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,  # Отключаем авто-коммит
            'fetch.min.bytes': 1,  # Минимум 1 байт для быстрого ответа
            'fetch.wait.max.ms': 100  # Ждем максимум 100 мс
        }
        self.consumer = Consumer(self.config)
        self.topic = topic
        schema_registry_conf = {'url': schema_registry_url}
        self.schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        # Инициализация JSONDeserializer с обязательным параметром schema_str
        if schema_str:
            self.json_deserializer = JSONDeserializer(
                schema_str=schema_str,
                schema_registry_client=self.schema_registry_client
            )
        else:
            # Получить схему из Schema Registry
            try:
                subject = f"{topic}-value"
                schema_response = self.schema_registry_client.get_latest_version(subject)
                schema_str = schema_response.schema.schema_str
                self.json_deserializer = JSONDeserializer(
                    schema_str=schema_str,
                    schema_registry_client=self.schema_registry_client
                )
                logger.info(f"Схема получена из Schema Registry: {subject}")
            except Exception as e:
                logger.error(
                    "Не удалось получить схему из Schema Registry: %s",
                    e
                )
                raise
        self.consumer.subscribe([self.topic])
        logger.info(f"SingleMessageConsumer запущен. Группа: {
            self.config['group.id']
        }")

    def process_message(self, msg):
        try:
            # Проверяем ключ сообщения
            if msg.key() and msg.key().decode('utf-8') == 'one':
                serialization_context = SerializationContext(self.topic, MessageField.VALUE)
                value = self.json_deserializer(msg.value(), serialization_context)
                logger.info(f"Обработка одиночного сообщения: key={msg.key()}, value={value}")
            else:
                logger.debug(f"Пропуск сообщения с ключом: {msg.key()}")
            # КОММИТ ОФФСЕТА ПОСЛЕ ОБРАБОТКИ
            self.consumer.commit(message=msg, asynchronous=False)
            logger.info(f"Закоммичен оффсет: partition={msg.partition()}, offset={msg.offset()}")

        except Exception as e:
            logger.error("Ошибка обработки сообщения: %s", e)

    def run(self):
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error("Ошибка Kafka: %s", msg.error())
                        continue

                self.process_message(msg)

        except KeyboardInterrupt:
            logger.info("Остановка консьюмера...")
        finally:
            self.consumer.close()


class BatchMessageConsumer:
    """Пакетный консьюмер."""
    def __init__(self, bootstrap_servers, topic, group_id, schema_registry_url, schema_str=None):
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,  # ДРУГОЙ УНИКАЛЬНЫЙ group.id
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'fetch.min.bytes': 10240,  # Ждем минимум ~10КБ данных
            'fetch.wait.max.ms': 5000  # Ждем до 5 секунд для накопления
        }
        self.consumer = Consumer(self.config)
        self.topic = topic
        schema_registry_conf = {'url': schema_registry_url}
        self.schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        # Инициализация JSONDeserializer с обязательным параметром schema_str
        if schema_str:
            self.json_deserializer = JSONDeserializer(
                schema_str=schema_str,
                schema_registry_client=self.schema_registry_client
            )
        else:
            # Получить схему из Schema Registry
            try:
                subject = f"{topic}-value"
                schema_response = self.schema_registry_client.get_latest_version(subject)
                schema_str = schema_response.schema.schema_str
                self.json_deserializer = JSONDeserializer(
                    schema_str=schema_str,
                    schema_registry_client=self.schema_registry_client
                )
                logger.info(f"Схема получена из Schema Registry: {subject}")
            except Exception as e:
                logger.error(
                    "Не удалось получить схему из Schema Registry: %s", e
                )
                raise
        self.consumer.subscribe([self.topic])
        self.batch_size = 10
        logger.info(f"BatchMessageConsumer запущен. Группа: {
            self.config['group.id']
        }")

    def process_batch(self, messages):
        if not messages:
            return

        try:
            for msg in messages:
                if msg.key() and msg.key().decode('utf-8') == 'many':
                    serialization_context = SerializationContext(self.topic, MessageField.VALUE)
                    value = self.json_deserializer(msg.value(), serialization_context)
                    logger.info(f"Обработка сообщения из пачки: key={msg.key()}, value={value}")

            # КОММИТ ВСЕЙ ПАЧКИ ОДИН РАЗ
            self.consumer.commit(asynchronous=False)
            logger.info(f"Закоммичена пачка из {len(messages)} сообщений")

        except Exception as e:
            logger.error("Ошибка обработки пачки: %s", e)

    def run(self):
        try:
            while True:
                messages = []

                # Собираем минимум batch_size сообщений
                while len(messages) < self.batch_size:
                    msg = self.consumer.poll(timeout=1.0)
                    if msg is None:
                        if len(messages) > 0:
                            break  # Обрабатываем то, что накопили
                        continue

                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        else:
                            logger.error(f"Ошибка Kafka: {msg.error()}")
                            continue

                    messages.append(msg)

                if messages:
                    self.process_batch(messages)

        except KeyboardInterrupt:
            logger.info("Остановка консьюмера...")
        finally:
            self.consumer.close()


def run_consumers(bootstrap_servers: str, topic: str, single_group_id: str, batch_group_id: str, schema_registry_url: str, schema_str: str = None):
    """
    Запускает оба консьюмера в отдельных потоках
    Возвращает словарь с потоками и консьюмерами для управления
    """
    single_consumer = SingleMessageConsumer(bootstrap_servers, topic, single_group_id, schema_registry_url, schema_str)
    batch_consumer = BatchMessageConsumer(bootstrap_servers, topic, batch_group_id, schema_registry_url, schema_str)

    thread1 = threading.Thread(target=single_consumer.run, daemon=True, name="single-consumer")
    thread2 = threading.Thread(target=batch_consumer.run, daemon=True, name="batch-consumer")

    thread1.start()
    thread2.start()

    logger.info(f"Консьюмеры запущены. Topic: {topic}, Bootstrap: {bootstrap_servers}")

    return {
        'threads': [thread1, thread2],
        'consumers': [single_consumer, batch_consumer]
    }
