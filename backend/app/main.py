import uvicorn
import logging
import threading

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse
from api.routers import main_router
from core.config import settings
from kafka_client.producer import init_producer, get_producer
from kafka_client.consumers import run_consumers
from kafka_client.schemas import json_schema_str


logging.basicConfig(
    level=settings.log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


logger = logging.getLogger(__name__)


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
    logger.info('Запуск Kafka')
    try:
        # Инициализация продюсера с передачей URL Schema Registry
        init_producer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            topic=settings.kafka_topic,
            schema_registry_url=settings.kafka_schemaregistry_url
        )
        logger.info('Kafka продюсер инициализирован')
    except Exception as e:
        logger.error(f'Ошибка инициализации продюсера: {e}')
    
    # Запуск консьюмеров
    try:
        consumers_info = run_consumers(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            topic=settings.kafka_topic,
            single_group_id=settings.single_consumer_group_id,
            batch_group_id=settings.batch_consumer_group_id,
            schema_registry_url=settings.kafka_schemaregistry_url,
            schema_str=json_schema_str
        )
        
        # Сохраняем информацию о консьюмерах для возможного graceful shutdown
        app.state.kafka_consumers = consumers_info
        logger.info('Kafka консьюмеры успешно запущены')
    except Exception as e:
        logger.error(f'Ошибка запуска консьюмеров: {e}')
    
    
@app.on_event('shutdown')
async def shutdown_event():
    logger.info('Завершение приложения Kafka')
    # Закрываем продюсер
    producer = get_producer()
    if producer:
        try:
            producer.close()
            logger.info('Kafka продюсер закрыт')
        except Exception as e:
            logger.error(f'Ошибка закрытия продюсера: {e}')
    # Фиксируем остановку консьюмеров
    if app.state.kafka_consumers:
        logger.info('Консьюмеры завершают работу')


if __name__ == '__main__':
    uvicorn.run(
        'main:app',
        host=settings.host,
        port=settings.port,
        reload=True
    )