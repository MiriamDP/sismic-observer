
import json
import logging
import asyncpg
from config import settings

logger = logging.getLogger("consumer.database")


class Database:
    '''Clase que encapsula todo lo relacionado con la base de datos (conexion, manipulacion...)'''

    def __init__(self):

        self.database_url = settings.DATABASE_URL
        self.pool = None
        if not self.database_url:
            raise RuntimeError("DATABASE_URL no está configurada")

    async def connect(self):
        '''Crea el pull de conexiones a la base de datos'''
        self.pool = await asyncpg.create_pool(
            dsn=self.database_url,
            min_size=1,
            max_size=5
        )

        logger.info("Conexión con PostgreSQL establecida")

    async def close(self):
        '''Cierra el pool de conexiones a la base de datos'''

        if self.pool:
            await self.pool.close()

    async def save_quarantine(self, message, reason):
        '''Guarda los datos excluidos del procesamiento y su motivo en la tabla quarantine'''

        event = message.get("event")

        event_id = (event.get("id") if isinstance(event, dict) else None)

        mode = message.get("mode")

        async with self.pool.acquire() as connection:

            await connection.execute(
                """
                INSERT INTO consumer_quarantine (
                    event_id,
                    mode,
                    reason,
                    original_message
                )
                VALUES ($1, $2, $3, $4::jsonb)
                """,
                event_id, mode, reason, json.dumps(message)
            )
            
        logger.warning("Mensaje enviado a cuarentena: %s | %s", event_id, reason)

    async def save_event(self, event):
        '''Guarda un evento valido'''

        async with self.pool.acquire() as connection:

            async with connection.transaction():
                # Para asegurar la consistencia de datos ambas operaciones se hacen en una misma transaccion

                inserted = await self.save_event_version(connection, event)

                if not inserted:

                    logger.info("Versión duplicada: %s", event["event_id"])

                    return "duplicate"

                # 2. Actualizar el estado actual únicamente
                # si la versión recibida es más reciente.
                updated = await self.update_current_event(connection, event)

                return ("saved_current" if updated else "saved_history_only")

    async def save_event_version(self, connection, event):
        """Guarda una versión del evento en el histórico."""

        return await connection.fetchval(
            """
            INSERT INTO earthquake_versions (
                event_id,
                mode,
                updated_ms,
                original_message,
                payload_hash
            )
            VALUES ($1, $2, $3, $4::jsonb, $5)
            ON CONFLICT (event_id, mode, updated_ms)
            DO NOTHING
            RETURNING TRUE
            """,
            event["event_id"],
            event["mode"],
            event["updated_ms"],
            json.dumps(event["original_message"]),
            event["payload_hash"]
        )

    async def update_current_event(self, connection, event):
        """Actualiza el evento actual si la versión es más reciente, en caso contrario la guarda."""

        return await connection.fetchval(
            """
            INSERT INTO earthquakes (
                event_id, mode, source, magnitude,
                magnitude_type, event_type, place,
                longitude, latitude, depth_km,
                event_time, updated, updated_ms, ingested_at,
                country_code
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7,
                $8, $9, $10, $11, $12, $13, $14, $15
            )
            ON CONFLICT (event_id, mode)
            DO UPDATE SET
                source = EXCLUDED.source,
                magnitude = EXCLUDED.magnitude,
                magnitude_type = EXCLUDED.magnitude_type,
                event_type = EXCLUDED.event_type,
                place = EXCLUDED.place,
                longitude = EXCLUDED.longitude,
                latitude = EXCLUDED.latitude,
                depth_km = EXCLUDED.depth_km,
                event_time = EXCLUDED.event_time,
                updated = EXCLUDED.updated,
                updated_ms = EXCLUDED.updated_ms,
                ingested_at = EXCLUDED.ingested_at,
                country_code = EXCLUDED.country_code
            WHERE EXCLUDED.updated_ms > earthquakes.updated_ms
            RETURNING TRUE
            """,
            event["event_id"],
            event["mode"],
            event["source"],
            event["magnitude"],
            event["magnitude_type"],
            event["event_type"],
            event["place"],
            event["longitude"],
            event["latitude"],
            event["depth_km"],
            event["event_time"],
            event["updated"],
            event["updated_ms"],
            event["ingested_at"],
            event["country_code"]
        )

