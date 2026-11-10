'''Modulo de peticiones a usgs'''
import asyncio
import logging
import random
import httpx

from core.config import settings


logger = logging.getLogger("api.usgs")


class USGSService:
    """Gestiona las consultas al servicio FDSN de USGS."""

    MAX_RETRIES = 4
    INITIAL_BACKOFF = 1
    MAX_BACKOFF = 15

    RETRYABLE_STATUS_CODES = {429, 500, 502, 503,504}

    def __init__(self):

        self.url = settings.USGS_URL
        self.limit = settings.USGS_LIMIT
        self.client = None

    async def connect(self):
        """Inicializa el cliente HTTP reutilizable."""

        if self.client is not None:
            return

        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=10, read=60, write=10, pool=10),
            headers={
                "User-Agent": "SismicObserver/1.0"
            }
        )

        logger.info("Cliente HTTP de USGS iniciado")

    async def close(self):
        """Cierra el cliente HTTP."""

        if self.client is None:
            return

        await self.client.aclose()
        self.client = None

        logger.info("Cliente HTTP de USGS cerrado")


    def _get_retry_delay(self, attempt, response=None):
        """Calcula el tiempo de espera antes del siguiente intento"""

        if response is not None:

            retry_after = response.headers.get("Retry-After")

            if retry_after:

                try:
                    return min(max(float(retry_after), 0), self.MAX_BACKOFF)

                except ValueError:
                    pass

        backoff = min(
            self.INITIAL_BACKOFF * (2 ** attempt),
            self.MAX_BACKOFF
        )

        jitter = random.uniform(0, backoff * 0.25)

        return backoff + jitter

    async def _request(self, params):
        """Realiza una petición HTTP con reintentos."""

        if self.client is None:
            raise RuntimeError("USGSService no está inicializado")

        for attempt in range(self.MAX_RETRIES):

            try:

                response = await self.client.get(self.url, params=params)

                response.raise_for_status()

                return response

            except httpx.HTTPStatusError as exc:

                status_code = exc.response.status_code

                if status_code not in self.RETRYABLE_STATUS_CODES:
                    raise

                if attempt == self.MAX_RETRIES - 1:
                    raise

                delay = self._get_retry_delay(attempt, exc.response)

                logger.warning(
                    "USGS respondió HTTP %s. "
                    "Reintento %s/%s en %.2f segundos",
                    status_code,
                    attempt + 1,
                    self.MAX_RETRIES - 1,
                    delay
                )

            except httpx.RequestError:

                if attempt == self.MAX_RETRIES - 1:
                    raise

                delay = self._get_retry_delay(attempt)

                logger.warning(
                    "Error de conexión con USGS. "
                    "Reintento %s/%s en %.2f segundos",
                    attempt + 1,
                    self.MAX_RETRIES - 1,
                    delay
                )

            await asyncio.sleep(delay)

        raise RuntimeError(
            "Se agotaron los intentos de conexión con USGS"
        )

    # --------------------------------------------------
    # CONSULTAR EVENTOS
    # --------------------------------------------------

    async def fetch_events(self, start, end, min_magnitude=None):
        """Obtiene eventos sísmicos de USGS en formato GeoJSON."""

        params = {
            "format": "geojson",
            "starttime": start.isoformat(),
            "endtime": end.isoformat(),
            "orderby": "time-asc",
            "limit": self.limit
        }

        if min_magnitude is not None:
            params["minmagnitude"] = min_magnitude

        try:

            response = await self._request(params)

            data = response.json()

            if not isinstance(data, dict):
                raise ValueError(
                    "La respuesta de USGS no es un objeto JSON válido"
                )

            features = data.get("features")

            if not isinstance(features, list):
                raise ValueError(
                    "La respuesta de USGS no contiene una lista de eventos"
                )

            logger.info(
                "Consulta USGS completada: %s - %s | eventos=%s",
                start,
                end,
                len(features)
            )

            return features

        except ValueError:

            logger.exception(
                "Respuesta inválida de USGS: %s - %s",
                start,
                end
            )

            raise

        except Exception:

            logger.exception(
                "Error consultando USGS: %s - %s",
                start,
                end
            )

            raise


usgs_service = USGSService()
