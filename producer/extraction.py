
'''Modulo con la lógica de negocio del productor'''

import asyncio
import hashlib
import json
import logging

from datetime import datetime, timezone

from database import (
    save_raw_extraction,
    register_ingestion,
    get_last_published,
    mark_published,
    save_exclusion
)

from config import settings
from usgs_client import fetch_earthquakes


logger = logging.getLogger("producer.extraction")


def calculate_hash(data):
    """Calcula una huella SHA-256 del contenido de un evento."""

    serialized = json.dumps(
        data,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":")
    )

    return hashlib.sha256(
        serialized.encode("utf-8")
    ).hexdigest()


def build_message(feature, ingested_at):
    """Construye el mensaje que se enviará a Kafka."""

    return {
        "source": settings.SOURCE,
        "ingested_at": ingested_at.isoformat(),
        "mode": settings.MODE,
        "event": feature
    }


async def process_received_extraction(
    pool,
    kafka,
    data,
    original_body,
    request_url,
    requested_at,
    completed_at,
    http_status
):
    """
    Procesa una extracción recibida.

    No realiza ninguna petición HTTP.

    Se utiliza tanto para:
    - Extracciones obtenidas directamente de USGS (live).
    - Extracciones recibidas desde la API principal (replay).
    """

    raw_extraction_id = None

    try:

        # 1. Validar estructura del GeoJSON.

        if not isinstance(data, dict):
            raise ValueError("Respuesta JSON no válida")

        if not isinstance(data.get("features"), list):
            raise ValueError(
                "La respuesta no contiene features válidas"
            )

        features = data["features"]

        # 2. Conservar la respuesta original.

        raw_extraction_id = await save_raw_extraction(
            pool,
            original_body
        )

        # 3. Registrar extracción.

        await register_ingestion(
            pool=pool,
            raw_extraction_id=raw_extraction_id,
            request_url=request_url,
            requested_at=requested_at,
            completed_at=completed_at,
            http_status=http_status,
            status="success",
            records_received=len(features)
        )

        logger.info(
            "Extracción recibida. Registros: %s | Modo: %s",
            len(features),
            settings.MODE
        )

        published = 0
        ignored = 0
        excluded = 0
        failed = 0

        # 4. Procesar eventos.

        for feature in features:

            if not isinstance(feature, dict):

                await save_exclusion(
                    pool,
                    None,
                    "Evento con estructura inválida",
                    raw_extraction_id,
                    feature
                )

                excluded += 1
                continue

            event_id = feature.get("id")

            if not event_id:

                await save_exclusion(
                    pool,
                    None,
                    "Identificador ausente",
                    raw_extraction_id,
                    feature
                )

                excluded += 1
                continue

            properties = feature.get("properties") or {}

            if not isinstance(properties, dict):

                await save_exclusion(
                    pool,
                    event_id,
                    "Propiedades del evento inválidas",
                    raw_extraction_id,
                    feature
                )

                excluded += 1
                continue

            # 5. Excluir eventos que no sean terremotos.

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

            # 6. Validar updated.

            if (
                not isinstance(updated_ms, int)
                or isinstance(updated_ms, bool)
                or updated_ms < 0
            ):

                await save_exclusion(
                    pool,
                    event_id,
                    "Campo updated ausente o inválido",
                    raw_extraction_id,
                    feature
                )

                excluded += 1
                continue

            try:

                # 7. Consultar estado persistente.

                previous = await get_last_published(
                    pool,
                    event_id
                )

                payload_hash = calculate_hash(feature)

                # 8. Detectar duplicados y versiones antiguas.

                if previous:

                    previous_updated = previous[
                        "last_published_updated_ms"
                    ]

                    previous_hash = previous[
                        "last_payload_hash"
                    ]

                    if updated_ms < previous_updated:

                        ignored += 1
                        continue

                    if updated_ms == previous_updated:

                        if payload_hash == previous_hash:

                            ignored += 1
                            continue

                        logger.warning(
                            "Anomalía: evento %s con igual updated "
                            "pero contenido diferente",
                            event_id
                        )

                        ignored += 1
                        continue

                # 9. Construir mensaje.

                ingested_at = datetime.now(timezone.utc)

                message = build_message(
                    feature,
                    ingested_at
                )

                # 10. Publicar y esperar confirmación Kafka.

                await kafka.publish(
                    event_id,
                    message
                )

                # 11. Actualizar estado después de confirmar Kafka.

                await mark_published(
                    pool,
                    event_id,
                    updated_ms,
                    payload_hash
                )

                published += 1

            except Exception:

                failed += 1

                logger.exception(
                    "Error publicando el evento %s",
                    event_id
                )

        # 12. Resultado final.

        result_status = (
            "partial" if failed > 0 else "success"
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
            "status": result_status,
            "received": len(features),
            "published": published,
            "ignored": ignored,
            "excluded": excluded,
            "failed": failed
        }

    except asyncio.CancelledError:

        raise

    except Exception as exc:
        try:
                await register_ingestion(
                    pool=pool,
                    raw_extraction_id=raw_extraction_id,
                    request_url=request_url,
                    requested_at=requested_at,
                    completed_at=datetime.now(timezone.utc),
                    http_status=http_status,
                    status="error",
                    error_message=str(exc)
                )

        except Exception:
            logger.exception(
                "No se pudo registrar el error de la extracción"
            )

        logger.exception(
            "Error durante el procesamiento de la extracción"
        )

        raise


async def process_extraction(pool, kafka, http_client):
    """
    Ejecuta una extracción live.

    Consulta USGS y delega el procesamiento
    a process_received_extraction().
    """

    requested_at = datetime.now(timezone.utc)

    # 1. Obtener datos de USGS.

    data, http_status, original_body = await fetch_earthquakes(
        http_client
    )

    completed_at = datetime.now(timezone.utc)

    # 2. Delegar procesamiento común.

    return await process_received_extraction(
        pool=pool,
        kafka=kafka,
        data=data,
        original_body=original_body,
        request_url=settings.USGS_URL,
        requested_at=requested_at,
        completed_at=completed_at,
        http_status=http_status
    )
