from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField
)
import threading

from core.config import logger


class SingleMessageConsumer:
    """Консьюмер единичных сообщений."""
    def __init__(
            self,
            bootstrap_servers,
            topic,
            group_id,
            schema_registry_url,
            security_conf,
            schema_str=None
    ):
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,  # УНИКАЛЬНЫЙ group.id
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,  # Отключаем авто-коммит
            'fetch.min.bytes': 1,  # Минимум 1 байт для быстрого ответа
            'fetch.wait.max.ms': 100,  # Ждем максимум 100 мс
            **security_conf
        }
        self.consumer = Consumer(self.config)
        self.topic = topic
        schema_registry_conf = {'url': schema_registry_url}
        self.schema_registry_client = SchemaRegistryClient(
            schema_registry_conf
        )
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
                logger.info('Схема получена из Schema Registry: %s', subject)
            except Exception as e:
                logger.error(
                    'Не удалось получить схему из Schema Registry: %s',
                    e
                )
                raise
        self.consumer.subscribe([self.topic])
        logger.info(
            'SingleMessageConsumer запущен. Группа: %s',
            self.config['group.id']
        )

    def process_message(self, msg):
        try:
            serialization_context = SerializationContext(
                self.topic,
                MessageField.VALUE
            )
            value = self.json_deserializer(msg.value(), serialization_context)
            logger.info(
                'Обработка одиночного сообщения: key=%s, value=%s',
                msg.key(),
                value
            )
            # КОММИТ ОФФСЕТА ПОСЛЕ ОБРАБОТКИ
            self.consumer.commit(message=msg, asynchronous=False)
            logger.info(
                'Закоммичен оффсет: partition=%s, offset=%s',
                msg.partition(),
                msg.offset()
            )
        except Exception as e:
            logger.error('Ошибка обработки сообщения: %s', e)

    def run(self):
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error('Ошибка Kafka: %s', msg.error())
                    continue
                self.process_message(msg)

        except KeyboardInterrupt:
            logger.info("Остановка консьюмера...")
        finally:
            self.consumer.close()


class BatchMessageConsumer:
    """Пакетный консьюмер."""
    def __init__(
        self,
        bootstrap_servers,
        topic,
        group_id,
        schema_registry_url,
        schema_str=None
    ):
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
        self.schema_registry_client = SchemaRegistryClient(
            schema_registry_conf
        )
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
                logger.info('Схема получена из Schema Registry: %s', subject)
            except Exception as e:
                logger.error(
                    'Не удалось получить схему из Schema Registry: %s', e
                )
                raise
        self.consumer.subscribe([self.topic])
        self.batch_size = 10
        logger.info(
            'BatchMessageConsumer запущен. Группа: %s',
            self.config['group.id']
        )

    def process_batch(self, messages):
        if not messages:
            return
        try:
            for msg in messages:
                if msg.key() and msg.key().decode('utf-8') == 'many':
                    serialization_context = SerializationContext(
                        self.topic,
                        MessageField.VALUE
                    )
                    value = self.json_deserializer(
                        msg.value(),
                        serialization_context
                    )
                    logger.info(
                        'Обработка сообщения из пачки: key=%s, value=%s',
                        msg.key(),
                        value
                    )

            # КОММИТ ВСЕЙ ПАЧКИ ОДИН РАЗ
            self.consumer.commit(asynchronous=False)
            logger.info('Закоммичена пачка из %s сообщений', len(messages))

        except Exception as e:
            logger.error('Ошибка обработки пачки: %s', e)

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
                            logger.error('Ошибка Kafka: %s', msg.error())
                            continue
                    messages.append(msg)
                if messages:
                    self.process_batch(messages)
        except KeyboardInterrupt:
            logger.info('Остановка консьюмера...')
        finally:
            self.consumer.close()


def run_consumers(
    bootstrap_servers: str,
    topic: str,
    single_group_id: str,
    schema_registry_url: str,
    security_conf: dict,
    schema_str: str = None
):
    """
    Запускает консьюмер в отдельном потоке
    Возвращает словарь с потоком и консьюмером для управления
    """
    single_consumer = SingleMessageConsumer(
        bootstrap_servers,
        topic,
        single_group_id,
        schema_registry_url,
        security_conf,
        schema_str
    )
    thread = threading.Thread(
        target=single_consumer.run,
        daemon=True,
        name="single-consumer"
    )
    thread.start()

    logger.info(
        'Консьюмер запущен. Topic: %s, Bootstrap: %s',
        topic,
        bootstrap_servers
    )

    return {
        'threads': [thread],
        'consumers': [single_consumer]
    }
