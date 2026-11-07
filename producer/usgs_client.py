'''Modulo de conexion a API externa para obtención de datos'''
import httpx
import logging
import asyncio

from config import settings

logger = logging.getLogger("producer.usgs")


async def fetch_earthquakes(client):
    """
    Descarga el feed de las últimas 24 horas.

    Gestiona errores HTTP, límites de peticiones
    y reintentos con espera progresiva.
    """

    for attempt in range(settings.MAX_RETRIES):

        try:
            response = await client.get(settings.USGS_URL)

            response.raise_for_status() # comprobamos si hay algun error => Excepcion

            data = response.json()

            # validacion de la respuesta
            if not isinstance(data, dict):
                raise ValueError("Respuesta JSON no válida")

            if not isinstance(data.get("features"), list):
                raise ValueError("La respuesta no contiene features válidas")

            return data, response.status_code, response.text

        except httpx.HTTPStatusError as exc:

            status = exc.response.status_code

            # vamos a distinguir entre errores que se pueden recuperar y errores que no
            # solo reintentaremos erroes 429 o de servidor 5xx
            if status != 429 and status < 500:
                raise

            logger.warning("Error HTTP %s. Intento %s", status, attempt + 1)

        except (httpx.TimeoutException, httpx.TransportError, ValueError) as exc:
            # Errores de timeout, de la capa de transporte (de red, conexiones...) por las validaciones de json

            logger.warning("Error consultando USGS: %s. Intento %s", exc, attempt + 1)

        if attempt < settings.MAX_RETRIES - 1:

            wait_time = min(2 ** attempt, 60)

            logger.info("Nuevo intento en %s segundos", wait_time)

            await asyncio.sleep(wait_time)

    # Lanzamos excepcion si se agotan los intentos
    raise RuntimeError(
        "No se pudo obtener información de USGS "
        "después de varios intentos"
    )
