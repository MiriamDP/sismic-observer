
import asyncio
import logging

from fastapi import WebSocket


logger = logging.getLogger("api")


class ConnectionManager:
    """Gestiona las conexiones WebSocket activas."""

    def __init__(self):

        self.active_connections: list[WebSocket] = []

    # --------------------------------------------------
    # CONECTAR CLIENTE
    # --------------------------------------------------

    async def connect(self, websocket: WebSocket):
        await websocket.accept()

        self.active_connections.append(websocket)

        logger.info(
            "Nueva conexión WebSocket. Conexiones activas: %s",
            len(self.active_connections)
        )

    # --------------------------------------------------
    # DESCONECTAR CLIENTE
    # --------------------------------------------------

    def disconnect(self, websocket: WebSocket):

        if websocket in self.active_connections:

            self.active_connections.remove(websocket)

            logger.info(
                "Conexión WebSocket cerrada. Conexiones activas: %s",
                len(self.active_connections)
            )

    # --------------------------------------------------
    # ENVIAR MENSAJE A UN CLIENTE
    # --------------------------------------------------

    async def send_message(self, websocket: WebSocket, message: dict):
        """Envía un mensaje a un cliente concreto."""

        try:

            await websocket.send_json(message)
            return True

        except Exception:

            logger.warning(
                "Error enviando mensaje WebSocket a un cliente",
                exc_info=True
            )

            return False

    # --------------------------------------------------
    # DIFUNDIR MENSAJE
    # --------------------------------------------------

    async def broadcast(self, message: dict):
        """Envía un mensaje concurrentemente a todos los clientes."""

        connections = self.active_connections.copy()

        if not connections:
            return

        results = await asyncio.gather(
            *[
                self.send_message(connection, message)
                for connection in connections
            ]
        )

        # Eliminar conexiones que han fallado.
        for connection, success in zip(connections, results):

            if not success:
                self.disconnect(connection)

        logger.debug(
            "Broadcast completado. Clientes=%s, enviados=%s",
            len(connections),
            sum(results)
        )


# --------------------------------------------------
# INSTANCIA GLOBAL
# --------------------------------------------------

manager = ConnectionManager()
