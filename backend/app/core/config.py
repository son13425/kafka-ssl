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
    kafka_bootstrap_servers: str
    kafka_schemaregistry_url: str
    kafka_topic: str
    single_consumer_group_id: str
    batch_consumer_group_id: str
    log_level: str

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
