from fastapi import APIRouter

from api.endpoints import msg_router

main_router = APIRouter()

main_router.include_router(msg_router, tags=['Сообщения'])