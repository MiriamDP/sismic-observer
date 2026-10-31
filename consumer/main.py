
import asyncio
import json
import logging
import os

import asyncpg
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("consumer")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "earthquakes")
KAFKA_GROUP = os.getenv("KAFKA_GROUP", "earthquake-processor")
DATABASE_URL = os.getenv("DATABASE_URL")

app = FastAPI(title="Earthquake Consumer")

consumer = None
task = None
db_pool = None


def validate_event(event):
    if not isinstance(event, dict):
        return False

    properties = event.get("properties")
    geometry = event.get("geometry")

    if not isinstance(properties, dict):
        return False

    if not isinstance(geometry, dict):
        return False

    if properties.get("mag") is None:
        return False

    if not properties.get("place"):
        return False

    if properties.get("time") is None:
        return False

    coordinates = geometry.get("coordinates")

    if not isinstance(coordinates, list) or len(coordinates) < 2:
        return False

    return True


async def init_database():
    global db_pool

    db_pool = await asyncpg.create_pool(DATABASE_URL)

    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS earthquakes (
                id TEXT PRIMARY KEY,
                magnitude DOUBLE PRECISION NOT NULL,
                place TEXT NOT NULL,
                event_time TIMESTAMPTZ NOT NULL,
                longitude DOUBLE PRECISION NOT NULL,
                latitude DOUBLE PRECISION NOT NULL,
                depth DOUBLE PRECISION,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

    logger.info("Base de datos inicializada correctamente")


async def save_earthquake(event):
    properties = event["properties"]
    coordinates = event["geometry"]["coordinates"]

    earthquake_id = event.get("id")

    if not earthquake_id:
        raise ValueError("El terremoto no tiene identificador")

    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO earthquakes (
                id, magnitude, place, event_time,
                longitude, latitude, depth
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (id) DO NOTHING
        """,
            earthquake_id,
            properties["mag"],
            properties["place"],
            __import__("datetime").datetime.fromtimestamp(
                properties["time"] / 1000,
                tz=__import__("datetime").timezone.utc
            ),
            coordinates[0],
            coordinates[1],
            coordinates[2] if len(coordinates) > 2 else None
        )


async def consume_earthquakes():
    global consumer

    while True:
        try:
            consumer = AIOKafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=KAFKA_GROUP,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            )

            await consumer.start()

            logger.info(
                "Consumidor conectado: topic=%s, group=%s",
                KAFKA_TOPIC,
                KAFKA_GROUP,
            )

            async for message in consumer:
                try:
                    event = json.loads(message.value.decode("utf-8"))

                    if not validate_event(event):
                        logger.warning(
                            "Evento inválido descartado: offset=%s",
                            message.offset,
                        )
                        continue

                    properties = event["properties"]

                    await save_earthquake(event)

                    logger.info(
                        "Terremoto guardado | id=%s | magnitud=%s | lugar=%s",
                        event.get("id"),
                        properties["mag"],
                        properties["place"],
                    )

                except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
                    logger.exception("Mensaje que no contiene JSON válido")

                except Exception:
                    logger.exception("Error procesando o guardando terremoto")

        except asyncio.CancelledError:
            raise

        except Exception:
            logger.exception("Error consumiendo de Kafka")
            await asyncio.sleep(5)

        finally:
            if consumer is not None:
                try:
                    await consumer.stop()
                except Exception:
                    pass
                consumer = None


@app.on_event("startup")
async def startup():
    global task

    await init_database()
    task = asyncio.create_task(consume_earthquakes())


@app.on_event("shutdown")
async def shutdown():
    global db_pool

    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    if consumer is not None:
        await consumer.stop()

    if db_pool is not None:
        await db_pool.close()


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "kafka_configured": bool(KAFKA_BOOTSTRAP),
        "database_connected": db_pool is not None,
        "topic": KAFKA_TOPIC,
    }
