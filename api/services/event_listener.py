import asyncio
import json
import logging
import asyncpg

from core.config import settings
from services.connection_manager import manager

logger = logging.getLogger(__name__)


async def notification_listener():

    while True:

        connection = None

        try:

            connection = await asyncpg.connect(
                settings.DATABASE_URL
            )

            async def callback(conn, pid, channel, payload):

                try:

                    event = json.loads(payload)

                    logger.info(
                        "Evento recibido de PostgreSQL: %s",
                        event.get("event_id")
                    )

                    # Enviar el evento recibido a los WebSockets
                    await manager.broadcast({
                        "type": "earthquakes_update",
                        "items": [event]
                    })

                    logger.info(
                        "Evento enviado por WebSocket: %s",
                        event.get("event_id")
                    )

                    

                except Exception:

                    logger.exception(
                        "Error enviando evento WebSocket"
                    )

            await connection.add_listener(
                "new_earthquake",
                callback
            )

            logger.info("Escuchando notificaciones PostgreSQL")

            while True:

                await asyncio.sleep(3600)

        except asyncio.CancelledError:

            raise

        except Exception:

            logger.exception(
                "Error en listener PostgreSQL. Reconectando..."
            )

            await asyncio.sleep(5)

        finally:

            if connection and not connection.is_closed():

                await connection.close()