
import logging
import httpx

from core.config import settings


logger = logging.getLogger("api.replay_client")


class ReplayProducerClient:
    """Cliente HTTP para comunicarse con producer-replay."""

    def __init__(self):
        self.client = None
        self.url = settings.REPLAY_PRODUCER_URL

    async def connect(self):
        """Inicializa el cliente HTTP."""

        if self.client is not None:
            return

        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(
                connect=10,
                read=300,
                write=30,
                pool=10
            ),
            headers={
                "User-Agent": "SismicObserver/1.0"
            }
        )

        logger.info("Cliente HTTP producer-replay iniciado")

    async def close(self):
        """Cierra el cliente HTTP."""

        if self.client is None:
            return

        await self.client.aclose()
        self.client = None

        logger.info("Cliente HTTP producer-replay cerrado")

    async def publish_extraction(self, extraction):
        """
        Envía una extracción completa al productor replay.

        Conserva el GeoJSON original y los metadatos
        de la petición realizada a USGS.
        """

        if self.client is None:
            raise RuntimeError(
                "El cliente producer-replay no está inicializado"
            )

        if not isinstance(extraction.data, dict):
            raise ValueError(
                "La extracción debe contener un objeto GeoJSON"
            )

        if not isinstance(extraction.data.get("features"), list):
            raise ValueError(
                "La extracción no contiene una lista de features"
            )

        payload = {
            "data": extraction.data,
            "original_body": extraction.original_body,
            "request_url": extraction.request_url,
            "requested_at": extraction.requested_at.isoformat(),
            "completed_at": extraction.completed_at.isoformat(),
            "http_status": extraction.http_status
        }

        try:

            response = await self.client.post(
                f"{self.url}/extract",
                json=payload
            )

            response.raise_for_status()

            result = response.json()

            logger.info(
                "Extracción procesada por producer-replay: %s",
                result
            )

            return result

        except httpx.HTTPStatusError:

            logger.exception(
                "producer-replay respondió con un error HTTP"
            )

            raise

        except httpx.RequestError:

            logger.exception(
                "Error de comunicación con producer-replay"
            )

            raise


replay_producer_client = ReplayProducerClient()
