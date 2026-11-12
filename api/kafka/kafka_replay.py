import json
import logging

from datetime import datetime, timezone
from typing import Awaitable, Callable
from aiokafka import AIOKafkaProducer
from core.config import settings



logger = logging.getLogger("api.replay")


class KafkaReplayPublisher:
    """Gestiona la publicación de eventos históricos en Kafka."""

    def __init__(self, bootstrap_servers, topic):

        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None

    async def start(self):
        """Inicializa el productor Kafka."""

        if self.producer is not None:
            return

        producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda value: json.dumps(
                value,
                ensure_ascii=False,
                separators=(",", ":")
            ).encode("utf-8"),
            acks="all",
            enable_idempotence=True
        )

        try:

            await producer.start()

            self.producer = producer

            logger.info(
                "Publicador replay conectado a Kafka: %s",
                self.topic
            )

        except BaseException:

            try:
                await producer.stop()
            except Exception:
                logger.exception(
                    "Error limpiando el productor Kafka"
                )

            raise

    async def stop(self):
        """Cierra el productor Kafka."""

        if self.producer is None:
            return

        producer = self.producer
        self.producer = None

        await producer.stop()

        logger.info("Publicador replay detenido")


    async def publish(self, feature):
        """Publica un evento histórico en Kafka."""

        if self.producer is None:
            raise RuntimeError(
                "El publicador Kafka no está iniciado"
            )

        if not isinstance(feature, dict):
            raise ValueError(
                "El evento debe ser un diccionario"
            )

        event_id = feature.get("id")

        if not isinstance(event_id, str) or not event_id.strip():
            raise ValueError(
                "El evento no tiene un identificador válido"
            )

        message = {
            "source": "USGS",
            "ingested_at": datetime.now(
                timezone.utc
            ).isoformat(),
            "mode": "replay",
            "event": feature
        }

        try:

            await self.producer.send_and_wait(
                self.topic,
                message,
                key=event_id.encode("utf-8")
            )

            return True

        except Exception:

            logger.exception(
                "Error publicando evento replay en Kafka: %s",
                event_id
            )

            raise


    async def publish_many(
        self,
        features,
        on_progress: Callable[[int], Awaitable[None]] | None = None,
        batch_size=100
    ):
        """Publica eventos y notifica periódicamente del progreso."""

        if batch_size<=0:
            raise ValueError("batch_size debe ser mayor que cero")

        published = 0

        try:

            for feature in features:

                await self.publish(feature)
                published += 1

                if published % batch_size == 0:

                    if on_progress:
                        await on_progress(batch_size)

                    logger.info(
                        "Eventos replay publicados: %s",
                        published
                    )

            # Registrar los eventos restantes del último bloque.
            remaining = published % batch_size

            if remaining > 0 and on_progress:
                await on_progress(remaining)

            logger.info(
                "Publicación replay finalizada: %s eventos",
                published
            )

            return published

        except Exception as exc:

            logger.exception(
                "Error publicando eventos replay. "
                "Confirmados: %s",
                published
            )

            raise KafkaPublishError(
                str(exc),
                published
            ) from exc


replay_publisher = KafkaReplayPublisher(
    bootstrap_servers=settings.KAFKA_BOOTSTRAP,
    topic=settings.KAFKA_TOPIC
)

class KafkaPublishError(Exception):
    """Error de publicación que conserva el número de mensajes confirmados."""

    def __init__(self, message, published):
        super().__init__(message)
        self.published = published