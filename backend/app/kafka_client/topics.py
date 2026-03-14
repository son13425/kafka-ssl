from confluent_kafka.admin import AdminClient, NewTopic

from core.config import logger


def build_admin_conf(settings) -> dict:
    """Cоздать конфиг администратора для топика."""
    return {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "security.protocol": settings.kafka_security_protocol,
        "sasl.mechanisms": settings.kafka_sasl_mechanism,
        "sasl.username": settings.kafka_admin_username,
        "sasl.password": settings.kafka_admin_password,
        "ssl.ca.location": settings.kafka_ssl_ca_location,
        "client.id": "topic-admin-client"
    }


def ensure_topics(settings):
    """Проверить наличие topic-1/topic-2 и создать при отсутствии."""
    admin_conf = build_admin_conf(settings)
    admin = AdminClient(admin_conf)
    md = admin.list_topics(timeout=15)
    existing = set(md.topics.keys())
    required = [
        settings.kafka_topic_single,
        settings.kafka_topic_batch
    ]
    to_create = []
    for topic in required:
        if topic not in existing:
            to_create.append(
                NewTopic(
                    topic=topic,
                    num_partitions=settings.kafka_topic_partitions,
                    replication_factor=settings.kafka_topic_replication_factor
                )
            )
    if not to_create:
        logger.info("Kafka topics already exist: %s", required)
        return
    futures = admin.create_topics(to_create, request_timeout=20)
    for topic, fut in futures.items():
        try:
            fut.result()
            logger.info("Created topic: %s", topic)
        except Exception as e:
            # Идемпотентность на случай гонки старта
            if "already exists" in str(e).lower():
                logger.info("Topic already exists: %s", topic)
            else:
                logger.error("Failed to create topic %s: %s", topic, e)
                raise
