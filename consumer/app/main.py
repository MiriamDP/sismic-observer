
import asyncio
import json
import logging
from contextlib import asynccontextmanager

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import TopicPartition, OffsetAndMetadata
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn

from app.config import settings
from app.validator import validate_message, ValidationError
from app.transformer import transform_message
from app.database import Database

from app.geography import Geography

from app.quality import assess_quality, log_quality_issues


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

database = Database()


async def consume_messages(app,geography):
    '''Lee los mensajes desde kafka'''

    consumer = AIOKafkaConsumer(
        settings.KAFKA_TOPIC,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP,
        group_id=settings.KAFKA_GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest"
    )

    try:

        await consumer.start()

        app.state.consumer_running = True

        logger.info("Conectado a Kafka")
        logger.info("Escuchando topic: %s", settings.KAFKA_TOPIC)

        async for message in consumer:

            try:

                # 1. Decodificar JSON
                data = json.loads(message.value.decode("utf-8"))

                # 2. Validar
                validate_message(data)

                # 3. Excluir otros tipos de eventos
                event_type = data["event"]["properties"].get("type")

                if event_type != "earthquake":

                    reason = f"excluded_event_type: {event_type}"

                    logger.info("Evento excluido: %s | Motivo: %s", data["event"]["id"], reason)

                    await database.save_quarantine(data,reason)

                    await consumer.commit({
                        TopicPartition(message.topic,message.partition): OffsetAndMetadata(
                            message.offset + 1, "")
                    })

                    continue

                # 4. Transformar
                event = transform_message(data)
                event["country_code"] = geography.find_country(event["longitude"], event["latitude"])

                # 5. Guardar en PostgreSQL
                result = await database.save_event(event)

                logger.info("Evento %s: %s", event["event_id"], result)

                # 6. Notificar a la API
                mode = data.get("mode")
                if mode == "live":

                    await database.notify_new_event(event)

                # 7. Confirmar offset
                await consumer.commit({
                    TopicPartition(message.topic, message.partition): OffsetAndMetadata(
                        message.offset + 1, ""
                    )
                })

            except (ValidationError, json.JSONDecodeError) as exc:

                logger.warning("Mensaje rechazado: %s", exc)

                try:

                    raw = message.value.decode("utf-8", errors="replace")

                    try:
                        original = json.loads(raw)
                    except json.JSONDecodeError:
                        original = {"raw": raw}

                    await database.save_quarantine(original, str(exc))

                    await consumer.commit({
                        TopicPartition(message.topic, message.partition): OffsetAndMetadata(
                            message.offset + 1, "")
                    })

                except Exception:

                    logger.exception("No se pudo guardar en cuarentena")
                    raise

            except Exception:

                logger.exception(
                    "Error procesando mensaje. "
                    "No se confirma el offset."
                )
                raise

    finally:

        app.state.consumer_running = False

        await consumer.stop()

        logger.info("Consumidor detenido")


@asynccontextmanager
async def lifespan(app: FastAPI):

    app.state.consumer_running = False
    app.state.consumer_error = None

    await database.connect()

    # Cargar catálogo geográfico
    geography = Geography(settings.GEOJSON_PATH)

    countries = geography.load()

    # Persistir catálogo en PostgreSQL
    await database.save_countries(countries)

    # Enriquecer los eventos que ya estaban almacenados
    await database.backfill_country_codes(geography)

    task = asyncio.create_task(consume_messages(app, geography))

    app.state.consumer_task = task

    try:
        yield

    finally:

        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("Error al detener consumidor")

        await database.close()


app = FastAPI(
    title="Earthquake Consumer",
    lifespan=lifespan
)


@app.get("/health")
async def health():
    '''Endpoint para la comprobación del estado de salud del consumidor'''

    task = app.state.consumer_task

    if not app.state.consumer_running or task.done():

        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "consumer_running": False
            }
        )

    return {
        "status": "healthy",
        "consumer_running": True,
        "kafka_topic": settings.KAFKA_TOPIC,
        "consumer_group": settings.KAFKA_GROUP
    }


if __name__ == "__main__":

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=settings.HEALTH_PORT
    )
