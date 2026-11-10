
import logging
import secrets

from fastapi import (
    APIRouter,
    WebSocket,
    WebSocketDisconnect
)

from core.config import settings

from services.connection_manager import manager
from services.earthquake_service import earthquake_service


logger = logging.getLogger("api")


router = APIRouter(
    tags=["Internal"]
)


@router.websocket("/ws/earthquakes")
async def earthquakes_websocket(websocket: WebSocket):

    await manager.connect(websocket)

    try:

        while True:

            # Mantener la conexión abierta
            await websocket.receive_text()

    except WebSocketDisconnect:

        manager.disconnect(websocket)