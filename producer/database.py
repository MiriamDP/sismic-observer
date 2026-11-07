'''Modulo de la capa de acceso a la base de datos'''
import asyncpg
import hashlib
import logging
import json

from datetime import datetime, timezone

from config import settings

# Logger para identificar los mensaje de log del modulo
logger = logging.getLogger("producer.database")


async def create_pool():
    """Crea el pool de conexiones PostgreSQL."""

    if not settings.DATABASE_URL:
        raise ValueError("DATABASE_URL no está configurada")

    return await asyncpg.create_pool(
        dsn=settings.DATABASE_URL,
        min_size=1,
        max_size=5,
        command_timeout=30
    )


async def save_raw_extraction(pool, response_body):
    """
    Conserva la respuesta original de USGS.
    Si ya existe exactamente la misma respuesta,
    actualiza last_seen sin duplicarla.
    """

    # Calculamos el sha de la respuesta tras codificarla en bytes 
    # y despues lo convertimos a hexadecimal de 64 caracteres
    digest = hashlib.sha256(response_body.encode("utf-8")).hexdigest()

    size_bytes = len(response_body.encode("utf-8"))

    # conexion con la base de datos
    async with pool.acquire() as connection:

        row = await connection.fetchrow(
            # Inserta en la tabla raw_extraction, si el sha esta repetido actualiza el campo last_seen
            # Es una consulta UPSERT
            """
            INSERT INTO raw_extractions (
                sha256,
                response_body,
                size_bytes
            )
            VALUES ($1, $2, $3)

            ON CONFLICT (sha256)
            DO UPDATE SET last_seen = NOW()

            RETURNING id
            """,
            digest,
            response_body,
            size_bytes
        )

    return row["id"]


async def register_ingestion(pool, raw_extraction_id, request_url,
    requested_at, completed_at, http_status, status,
    records_received=0, error_message=None):
    """Registra una consulta realizada a USGS."""

    async with pool.acquire() as connection:

        await connection.execute(
            """
            INSERT INTO ingestion_runs (
                raw_extraction_id,
                source,
                request_url,
                requested_at,
                completed_at,
                http_status,
                status,
                records_received,
                error_message
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            """,
            raw_extraction_id, settings.SOURCE,
            request_url, requested_at, completed_at,
            http_status, status,
            records_received, error_message
        )


async def get_last_published(pool, event_id):
    """Consulta la última versión publicada de un evento. 
    Devuelve None si todavia no se ha publicado el evento"""

    async with pool.acquire() as connection:

        return await connection.fetchrow(
            """
            SELECT
                last_published_updated_ms,
                last_payload_hash
            FROM producer_event_state
            WHERE event_id = $1
              AND mode = $2
            """,
            event_id, settings.MODE
        )


async def mark_published(pool, event_id, updated_ms, payload_hash):
    """
    Actualiza el estado solamente si la versión publicada
    es más reciente que la versión registrada.
    """

    async with pool.acquire() as connection:

        await connection.execute(
            # Consulta UPSERT
            """
            INSERT INTO producer_event_state (
                event_id,
                mode,
                last_published_updated_ms,
                last_payload_hash,
                last_published_at
            )
            VALUES ($1,$2,$3,$4,$5)

            ON CONFLICT (event_id, mode)
            DO UPDATE SET
                last_published_updated_ms =
                    EXCLUDED.last_published_updated_ms,
                last_payload_hash =
                    EXCLUDED.last_payload_hash,
                last_published_at =
                    EXCLUDED.last_published_at

            WHERE producer_event_state.last_published_updated_ms
                  < EXCLUDED.last_published_updated_ms
            """,
            event_id, settings.MODE,
            updated_ms, payload_hash,
            datetime.now(timezone.utc)
        )
