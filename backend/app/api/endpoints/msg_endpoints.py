from fastapi import APIRouter, HTTPException

from core.config import logger
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
    """Получает сообщение и отправляет его в Kafka."""
    logger.info(
        f"Получено сообщение от клиента: key={data_msg.key}, "
        f"msg={data_msg.msg}"
    )
    producer = get_producer()
    if not producer:
        logger.info("Kafka-продюсер не инициализирован")
        raise HTTPException(
            status_code=500,
            detail="Kafka-продюсер не инициализирован"
        )
    success = producer.send_message(
        key=data_msg.key,
        message=data_msg.msg
    )
    if not success:
        logger.info("Ошибка при отправке сообщения в Kafka")
        raise HTTPException(
            status_code=500,
            detail="Ошибка при отправке сообщения в Kafka"
        )
    return {"msg": "Сообщение получено и отправлено в Kafka"}
