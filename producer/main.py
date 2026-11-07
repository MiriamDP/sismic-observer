
import asyncio
import logging

from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx

from config import settings
from database import create_pool
from kafka_client import KafkaClient
from extraction import process_extraction
from fastapi import FastAPI
from api import create_app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

logger = logging.getLogger("producer")

class ProducerState:
    '''Clase que almacena el estado interno del productor'''

    def __init__(self):
        self.running = False
        self.last_attempt = None
        self.last_success = None
        self.last_error = None
        self.last_result = None
        self.consecutive_errors = 0


state = ProducerState()

async def producer_loop(pool, kafka, http_client):
    '''Ciclo de extracción de datos'''

    logger.info("Iniciando extracción periódica de USGS")

    state.running = True

    while True:

        cycle_start = asyncio.get_running_loop().time() # medir cuanto dura el ciclo

        state.last_attempt = datetime.now(timezone.utc)

        try:

            result = await process_extraction(
                pool,
                kafka,
                http_client
            )

            state.last_result = result

            if result["failed"] > 0:

                state.last_error = (
                    f"Extracción parcial: "
                    f"{result['failed']} eventos no publicados"
                )

                state.consecutive_errors += 1

                logger.warning(state.last_error)

            else:

                state.last_success = datetime.now(timezone.utc)
                state.last_error = None
                state.consecutive_errors = 0

            logger.info("Resultado del ciclo: %s", result)

        except asyncio.CancelledError:
            raise

        except Exception as exc:

            state.last_error = str(exc)
            state.consecutive_errors += 1

            logger.exception("Error durante la extracción")

        # Duracion ciclo de extraccion
        elapsed = (asyncio.get_running_loop().time() - cycle_start)

        # como se dan retrasos con la peticion a usgs asi no esperamos 
        # 60s completos desde que acaba el ciclo, sino el tiempo que queda para el siguiente
        remaining = max(0,settings.POLL_INTERVAL - elapsed)

        await asyncio.sleep(remaining)


@asynccontextmanager
async def lifespan(app: FastAPI):

    logger.info("Iniciando productor")

    pool = None
    kafka = None
    http_client = None
    producer_task = None

    try:

        # PostgreSQL
        pool = await create_pool()

        await pool.fetchval("SELECT 1")

        logger.info("PostgreSQL conectado")

        # Kafka
        kafka = KafkaClient()

        await kafka.start()

        logger.info("Kafka conectado")

        # Cliente HTTP
        http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(settings.HTTP_TIMEOUT),
            headers={
                "User-Agent": "SismicObserver/1.0"
            }
        )

        # para que la api pueda acceder a ellos
        app.state.pool = pool
        app.state.kafka = kafka

        # Iniciar tarea periódica
        producer_task = asyncio.create_task(
            producer_loop(pool,kafka,http_client)
        )

        yield

    finally:

        logger.info("Cerrando productor")

        if producer_task:

            producer_task.cancel()

            try:
                await producer_task
            except asyncio.CancelledError:
                pass

        if http_client:
            await http_client.aclose()

        if kafka:
            await kafka.stop()

        if pool:
            await pool.close()

        state.running = False

        logger.info("Productor detenido")

app = create_app(lifespan,state)