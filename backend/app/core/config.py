"""Настройки приложения."""
import logging
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


BASE_DIR = Path(__file__).resolve().parent.parent.parent
ENV_PATH = BASE_DIR / '.env.backend'


class Settings(BaseSettings):
    """Настройки приложения."""
    name: str
    host: str
    port: int
    log_level: str
    kafka_bootstrap_servers: str
    kafka_schemaregistry_url: str
    kafka_topic_single: str
    kafka_topic_batch: str
    kafka_topic_partitions: int = 3
    kafka_topic_replication_factor: int = 3
    kafka_security_protocol: str
    kafka_sasl_mechanism: str
    kafka_producer_username: str
    kafka_producer_password: str
    kafka_consumer_username: str
    kafka_consumer_password: str
    kafka_admin_username: str
    kafka_admin_password: str
    kafka_ssl_ca_location: str
    kafka_ssl_endpoint_identification_algorithm: str = ""
    single_consumer_group_id: str
    batch_consumer_group_id: str

    model_config = SettingsConfigDict(
        env_file=ENV_PATH,
        env_file_encoding='utf-8'
    )


settings = Settings()


logging.basicConfig(
    level=settings.log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


logger = logging.getLogger(__name__)
