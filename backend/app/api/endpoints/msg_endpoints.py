from http import HTTPStatus
from fastapi import APIRouter, HTTPException

from core.config import logger, settings
from schemas.msg_schema import (OutgoingMessageSchema,
                                IncomingMessageSchema)
from kafka_client.producer import get_producer


router = APIRouter()


@router.post(
    '/message',
    response_model=OutgoingMessageSchema,
    response_model_exclude_none=True
)
async def receiving_message(
    data_msg: IncomingMessageSchema
):
    """Получает одиночное сообщение и отправляет его в Kafka (в topic-1)."""
    logger.info(
        'Получено сообщение от клиента: key=%s, msg=%s',
        data_msg.key,
        data_msg.msg
    )
    producer = get_producer()
    if not producer:
        logger.info('Kafka-продюсер не инициализирован')
        raise HTTPException(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail='Kafka-продюсер не инициализирован'
        )
    success = producer.send_message(
        topic=settings.kafka_topic_single,
        key=data_msg.key,
        message=data_msg.msg
    )
    if not success:
        logger.info('Ошибка при отправке сообщения в Kafka')
        raise HTTPException(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail='Ошибка при отправке сообщения в Kafka'
        )
    return {
        "msg": f"Сообщение получено и отправлено в {
            settings.kafka_topic_single
        }"
    }


@router.post(
    '/messages/batch',
    response_model=OutgoingMessageSchema,
    response_model_exclude_none=True
)
async def receiving_messages_batch(
    data_msgs: list[IncomingMessageSchema]
):
    """Получает пакет сообщенй и отправляет его в Kafka (в topic-2)."""
    logger.info('Получен пакет сообщений от клиента')
    producer = get_producer()
    if not producer:
        raise HTTPException(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail='Kafka-продюсер не инициализирован'
        )
    payload = [{"key": m.key, "msg": m.msg} for m in data_msgs]
    success = producer.send_batch(
        topic=settings.kafka_topic_batch,
        items=payload
    )
    if not success:
        raise HTTPException(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail='Ошибка пакетной отправки в Kafka'
        )
    return {
        "msg": f"Пакет из {len(payload)} сообщений отправлен в {
            settings.kafka_topic_batch
        }"
    }
