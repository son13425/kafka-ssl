from pydantic import BaseModel


class OutgoingMessageSchema(BaseModel):
    msg: str

    class Config:
        from_attributes = True


class IncomingMessageSchema(BaseModel):
    key: str
    msg: str

    class Config:
        from_attributes = True