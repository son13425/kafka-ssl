import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse

from api.routers import main_router
from core.config import settings, logger
from kafka_client.producer import init_producer, get_producer
from kafka_client.consumers import run_consumers
from kafka_client.schemas import json_schema_str
from kafka_client.topics import ensure_topics


app = FastAPI(
    title=settings.name,
    default_response_class=ORJSONResponse,
    docs_url='/api/openapi',
    openapi_url='/api/openapi.json'
)


app.include_router(main_router, prefix='/api')


app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['GET', 'POST', 'OPTIONS'],
    allow_headers=[
        'Content-Type',
        'Access-Control-Allow-Headers',
        'Access-Control-Allow-Origin'
    ],
)


app.state.kafka_consumers = None


@app.on_event('startup')
async def startup_event():
    """Запуск сервиса Кафка."""
    logger.info('Запуск Kafka')
    producer_security_conf = {
        "security.protocol": settings.kafka_security_protocol,
        "sasl.mechanisms": settings.kafka_sasl_mechanism,
        "sasl.username": settings.kafka_producer_username,
        "sasl.password": settings.kafka_producer_password,
        "ssl.ca.location": settings.kafka_ssl_ca_location
    }
    consumer_security_conf = {
        "security.protocol": settings.kafka_security_protocol,
        "sasl.mechanisms": settings.kafka_sasl_mechanism,
        "sasl.username": settings.kafka_consumer_username,
        "sasl.password": settings.kafka_consumer_password,
        "ssl.ca.location": settings.kafka_ssl_ca_location
    }
    # Инициализация продюсера с передачей URL Schema Registry
    try:
        ensure_topics(settings)
        logger.info('Проверка/создание topic-1/topic-2 выполнена')
    except Exception as e:
        logger.error('Ошибка проверки/создания топиков: %s', e)
    try:
        init_producer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            schema_registry_url=settings.kafka_schemaregistry_url,
            conf_extra=producer_security_conf
        )
        logger.info('Kafka продюсер инициализирован')
    except Exception as e:
        logger.error('Ошибка инициализации продюсера: %s', e)
    # Запуск консьюмеров
    try:
        consumers_info = run_consumers(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            topic=settings.kafka_topic_single,
            single_group_id=settings.single_consumer_group_id,
            schema_registry_url=settings.kafka_schemaregistry_url,
            security_conf=consumer_security_conf,
            schema_str=json_schema_str
        )
        # Сохраняем информацию о консьюмерах для возможного graceful shutdown
        app.state.kafka_consumers = consumers_info
        logger.info('Kafka консьюмер успешно запущен')
    except Exception as e:
        logger.error('Ошибка запуска консьюмеров: %s', e)


@app.on_event('shutdown')
async def shutdown_event():
    """Остановка сервиса Кафка."""
    logger.info('Завершение приложения Kafka')
    # Закрываем продюсер
    producer = get_producer()
    if producer:
        try:
            producer.close()
            logger.info('Kafka продюсер закрыт')
        except Exception as e:
            logger.error('Ошибка закрытия продюсера: %s', e)
    # Фиксируем остановку консьюмеров
    if app.state.kafka_consumers:
        logger.info('Консьюмер завершает работу')


if __name__ == '__main__':
    uvicorn.run(
        'main:app',
        host=settings.host,
        port=settings.port,
        reload=True
    )
