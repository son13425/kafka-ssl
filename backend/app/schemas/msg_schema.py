from pydantic import BaseModel


class OutgoingMessageSchema(BaseModel):
    """Схема исходящего сообщения"""
    msg: str

    class Config:
        from_attributes = True


class IncomingMessageSchema(BaseModel):
    """Схема входящего сообщения для ПР1"""
    key: str
    msg: str

    class Config:
        from_attributes = True
