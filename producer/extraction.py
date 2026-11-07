'''Modulo con la lógica de negocio del productor'''
import asyncio
import hashlib
import json
import logging
from database import (
    save_raw_extraction,
    register_ingestion,
    get_last_published,
    mark_published,
    save_exclusion
)

from datetime import datetime, timezone

from config import settings

from usgs_client import fetch_earthquakes

logger = logging.getLogger("producer.extraction")


def calculate_hash(data):
    """Calcula una huella SHA-256 del contenido de un evento"""

    serialized = json.dumps(data, sort_keys=True, ensure_ascii=False, separators=(",", ":"))

    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def build_message(feature, ingested_at):
    """Construye el mensaje que se enviará a Kafka."""

    return {
        "source": settings.SOURCE,
        "ingested_at": ingested_at.isoformat(),
        "mode": settings.MODE,
        "event": feature
    }


async def process_extraction(pool, kafka, http_client):
    """
    Ejecuta una extracción completa:

    1. Consulta USGS.
    2. Guarda la respuesta original.
    3. Registra la extracción.
    4. Detecta eventos nuevos y revisiones.
    5. Publica las novedades en Kafka.
    6. Actualiza el estado persistente.
    """

    requested_at = datetime.now(timezone.utc)

    raw_extraction_id = None
    http_status = None

    try:

        # 1. Obtener respuesta de USGS
        data, http_status, original_body = await fetch_earthquakes(http_client)

        completed_at = datetime.now(timezone.utc)

        # 2. Conservar la respuesta original
        original_body = json.dumps(data, ensure_ascii=False, separators=(",", ":"))

        raw_extraction_id = await save_raw_extraction(pool,original_body) # guardamos los datos originales

        features = data.get("features", [])

        # 3. Registrar extracción
        await register_ingestion(
            pool=pool,
            raw_extraction_id=raw_extraction_id,
            request_url=settings.USGS_URL,
            requested_at=requested_at,
            completed_at=completed_at,
            http_status=http_status,
            status="success",
            records_received=len(features)
        )

        logger.info("Extracción completada. Registros recibidos: %s", len(features))

        published = 0
        ignored = 0
        excluded = 0
        failed = 0

        # 4. Procesar cada evento que llega en la peticion
        for feature in features:

            event_id = feature.get("id")
            properties = feature.get("properties") or {}

            # Sin identificador no podemos gestionar versiones
            if not event_id:
                await save_exclusion(
                    pool, None, "Identificador ausente",
                    raw_extraction_id, feature
                )
                excluded += 1
                continue

            # Excluir eventos que no sean terremotos
            if properties.get("type") != "earthquake":
                await save_exclusion(
                    pool,
                    event_id,
                    f"Tipo de evento no permitido: {properties.get('type')}",
                    raw_extraction_id,
                    feature
                )
                excluded += 1
                continue

            updated_ms = properties.get("updated")

            # Excluir si faltan campos necesarios o no son validos, updated es necesario para controlar revisiones
            if (not isinstance(updated_ms, int) or isinstance(updated_ms, bool) or updated_ms < 0):
                await save_exclusion(
                    pool, event_id, "Campo updated ausente o inválido",
                    raw_extraction_id, feature
                )
                excluded += 1
                continue

            # 5. Consultar estado persistente
            previous = await get_last_published(pool,event_id)

            payload_hash = calculate_hash(feature)

            # 6. Detectar duplicados y versiones antiguas
            if previous:
                previous_updated = previous["last_published_updated_ms"]
                previous_hash = previous["last_payload_hash"]

                # si la verison del evento es mas antigua que la que hay en base de datos
                if updated_ms < previous_updated:
                    ignored += 1
                    continue

                # si ambas fechas del evento son iguales
                if updated_ms == previous_updated:

                    # si ambos eventos son los mismos
                    if payload_hash == previous_hash:
                        ignored += 1
                        continue

                    # si el contenido de los eventos es diferente
                    logger.warning(
                        "Anomalía: evento %s con igual updated "
                        "pero contenido diferente",
                        event_id
                    )

                    ignored += 1
                    continue

            try:

                ingested_at = datetime.now(timezone.utc)
                # 7. Construir mensaje
                message = build_message(feature, ingested_at)

                # 8. Publicar y esperar confirmación Kafka
                await kafka.publish(event_id,message)

                # 9. Actualizar estado después de la confirmación
                await mark_published(pool, event_id, updated_ms, payload_hash)

                published += 1

            except Exception:

                failed += 1

                logger.exception(
                    "Error publicando el evento %s",
                    event_id
                )

        logger.info(
            "Resumen extracción | Publicados=%s | Ignorados=%s | "
            "Excluidos=%s | Fallidos=%s",
            published,
            ignored,
            excluded,
            failed
        )

        return {
            "status": "success",
            "received": len(features),
            "published": published,
            "ignored": ignored,
            "excluded": excluded,
            "failed": failed
        }

    except asyncio.CancelledError:
        raise

    except Exception as exc:

        completed_at = datetime.now(timezone.utc)

        await register_ingestion(
            pool=pool,
            raw_extraction_id=raw_extraction_id,
            request_url=settings.USGS_URL,
            requested_at=requested_at,
            completed_at=completed_at,
            http_status=http_status,
            status="error",
            error_message=str(exc)
        )

        logger.exception("Error durante la extracción")

        raise
