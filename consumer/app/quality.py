'''Modulo para analizar la calidad de los datos almancenados por el consumidor'''
import logging
from datetime import timezone

logger = logging.getLogger(__name__)


def assess_quality(event: dict) -> list[str]:
    """
    Detecta anomalías sin rechazar automáticamente el evento.

    Devuelve una lista de incidencias.
    Una lista vacía significa que no se han detectado
    las anomalías contempladas.
    """

    issues = []

    # 1. COMPLETITUD

    if event.get("magnitude") is None:
        issues.append("missing_magnitude")

    if not event.get("magnitude_type"):
        issues.append("missing_magnitude_type")

    if not event.get("place"):
        issues.append("missing_place")

    if event.get("depth_km") is None:
        issues.append("missing_depth")

    # 2. VALIDEZ Y ANOMALÍAS

    magnitude = event.get("magnitude")

    if magnitude is not None and magnitude < 0:
        issues.append("negative_magnitude_review")

    depth = event.get("depth_km")

    if depth is not None and depth < 0:
        issues.append("negative_depth_review")

    # 3. CONSISTENCIA TEMPORAL

    event_time = event.get("event_time")
    updated = event.get("updated")
    ingested_at = event.get("ingested_at")

    if event_time and updated:

        if updated < event_time:
            issues.append("updated_before_event_time")

    if ingested_at and event_time:

        delay = (
            ingested_at - event_time
        ).total_seconds()

        if delay < 0:
            issues.append("ingestion_before_event")

        event["ingestion_delay_seconds"] = delay

    # 4. CONSISTENCIA GEOGRÁFICA

    if event.get("country_code") is None:
        issues.append("country_not_assigned")

    # 5. IDENTIFICACIÓN DEL MODO

    if event.get("mode") == "replay":
        event["is_replay"] = True
    else:
        event["is_replay"] = False

    return issues


def log_quality_issues(event_id: str, issues: list[str]):

    if not issues:
        return

    logger.warning("Calidad del evento %s: %s", event_id, ", ".join(issues))
