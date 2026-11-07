'''Modulo de conexión a kafka'''
import json
import logging

from aiokafka import AIOKafkaProducer

from config import settings

logger = logging.getLogger("producer.kafka")


class KafkaClient:
    '''Clase que encapsula la conexión a Kafka. Establece la conexion, 
    configura el productor, gestiona la publicacion y esperas de confirmacion de kafka'''

    def __init__(self):
        self.producer = None

    async def start(self):
        """Establece la conexión con Kafka y crea el productor"""

        if self.producer is None:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP,
                acks="all",
                enable_idempotence=True,
                compression_type="gzip",
                request_timeout_ms=30000
            )

            await self.producer.start()

            logger.info("Conectado a Kafka")

    async def publish(self, event_id, message):
        """
        Publica un evento utilizando su identificador
        como clave y espera la confirmación de Kafka.
        """

        if self.producer is None:
            raise RuntimeError("Kafka no está conectado")

        # kafka require recibir el mensaje en bytes
        payload = json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

        # publicación del mensaje con espera de confirmación
        metadata = await self.producer.send_and_wait(
            settings.KAFKA_TOPIC,
            key=event_id.encode("utf-8"),
            value=payload
        )

        logger.info(
            "Evento %s publicado. Partición=%s Offset=%s",
            event_id,
            metadata.partition,
            metadata.offset
        )

        return metadata

    async def stop(self):
        """Cierra correctamente la conexión."""

        if self.producer is not None:
            await self.producer.stop()
            self.producer = None
            logger.info("Conexión Kafka cerrada")
