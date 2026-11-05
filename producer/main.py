import asyncio
import json
import logging
import os

import httpx
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI

logging.basicConfig(level=logging.INFO) #registra mensaje de nivel info y superiores
logger = logging.getLogger("producer")
# para comprobar que el producer funciona correctamente

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "earthquakes")

# url de la api de terremotos
USGS_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"


# crea la aplicación del producer y sus variables de entorno
app = FastAPI(title="Earthquake Producer")

producer = None # instancia del cliente producer de kafka
task = None #tarea asincrona que jecuta el proceso de publicacion
running = False # ultimo ciclo de consulta y publicacion termino sin lanzar excepcion
sent_ids = set() #guarda identificadores de terremotos que ya se han enviado durante la ejecucion


async def publish_earthquakes():
    """Se conecta a kafka si todavia no hay conexion, consulta la api de USGS, 
    recorre los terremotos recibidos, filtra los ya publicados y envia cada evento a kafka
    Espera 1 minuto antes de volver a consultar la api"""
    global producer, running

    while True:
        try:
            if producer is None:
                producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
                await producer.start()
                logger.info("Conectado a Kafka")

            # consulta la api de usgs
            async with httpx.AsyncClient(timeout=20) as client: #crea cliente http asincrono que se cierra tras 20s
                response = await client.get(USGS_URL)
                response.raise_for_status()
                data = response.json()

            for feature in data.get("features", []):
                event_id = feature.get("id")

                if not event_id or event_id in sent_ids: #ignora los que ya estan en la lista o vienen sin id
                    continue

                message = json.dumps(feature).encode("utf-8")

                await producer.send_and_wait( #publica el terremoto
                    KAFKA_TOPIC,
                    value=message,
                    key=event_id.encode("utf-8"),
                )

                sent_ids.add(event_id)
                logger.info(
                    "Terremoto publicado: %s",
                    event_id,
                )

            running = True

        except asyncio.CancelledError:
            raise

        except Exception:
            running = False
            logger.exception("Error consultando USGS o publicando en Kafka")

            if producer is not None:
                try:
                    await producer.stop()
                except Exception:
                    pass
                producer = None

            # TODO: establecer politica de reintentos incrementales

        await asyncio.sleep(60) # para el ciclo infinito durante 60s antes de empezar otra vez


@app.on_event("startup")
async def startup():
    global task
    task = asyncio.create_task(publish_earthquakes())


@app.on_event("shutdown")
async def shutdown():
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    if producer is not None:
        await producer.stop()


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "kafka_configured": bool(KAFKA_BOOTSTRAP),
        "topic": KAFKA_TOPIC,
    }
