'''Modulo para la validación de los campos del mensaje'''
import math
from datetime import datetime, timezone


class ValidationError(Exception):
    """Error de validación de un evento sísmico."""


def validate_message(data: dict) -> None:

    if not isinstance(data, dict):
        raise ValidationError("El mensaje no es un objeto JSON")

    event = data.get("event")

    if not isinstance(event, dict):
        raise ValidationError("Falta el objeto event")

    if event.get("type") != "Feature":
        raise ValidationError("El evento no es una Feature GeoJSON")

    event_id = event.get("id")

    if not isinstance(event_id, str) or not event_id.strip():
        raise ValidationError("Identificador del evento ausente")

    properties = event.get("properties")

    if not isinstance(properties, dict):
        raise ValidationError("Falta properties")

    geometry = event.get("geometry")

    if not isinstance(geometry, dict):
        raise ValidationError("Falta geometry")

    if geometry.get("type") != "Point":
        raise ValidationError("La geometría no es Point")

    coordinates = geometry.get("coordinates")

    if not isinstance(coordinates, list) or len(coordinates) != 3:
        raise ValidationError("Coordenadas inválidas")

    try:
        longitude = float(coordinates[0])
        latitude = float(coordinates[1])

    except (TypeError, ValueError, OverflowError):
        raise ValidationError("Longitud o latitud no numéricas")

    if not math.isfinite(longitude) or not math.isfinite(latitude):
        raise ValidationError("Coordenadas no finitas")

    if not -180 <= longitude <= 180:
        raise ValidationError("Longitud fuera de rango")

    if not -90 <= latitude <= 90:
        raise ValidationError("Latitud fuera de rango")

    # La profundidad puede ser nula.
    depth = coordinates[2]

    if depth is not None:

        try:
            depth = float(depth)
        except (TypeError, ValueError, OverflowError):
            raise ValidationError("Profundidad no numérica")

        if not math.isfinite(depth):
            raise ValidationError("Profundidad no finita")

    # La magnitud puede ser nula.
    magnitude = properties.get("mag")

    if magnitude is not None:

        try:
            magnitude = float(magnitude)
        except (TypeError, ValueError, OverflowError):
            raise ValidationError("Magnitud no numérica")

        if not math.isfinite(magnitude):
            raise ValidationError("Magnitud no finita")

    # Validación de timestamps.
    for field in ("time", "updated"):

        value = properties.get(field)

        if isinstance(value, bool) or not isinstance(value, int):
            raise ValidationError(
                f"Timestamp {field} ausente o inválido"
            )

        try:
            datetime.fromtimestamp(
                value / 1000,
                tz=timezone.utc
            )
        except (ValueError, OverflowError, OSError):
            raise ValidationError(
                f"Timestamp {field} fuera de rango"
            )

    mode = data.get("mode")

    if mode not in ("live", "replay"):
        raise ValidationError("Modo de procesamiento inválido")

    source = data.get("source")

    if not isinstance(source, str) or not source.strip():
        raise ValidationError("Fuente ausente")

    ingested_at = data.get("ingested_at")

    if not isinstance(ingested_at, str):
        raise ValidationError("Fecha de ingesta ausente")

    try:
        parsed_ingestion = datetime.fromisoformat(
            ingested_at.replace("Z", "+00:00")
        )

        if parsed_ingestion.tzinfo is None:
            raise ValidationError(
                "La fecha de ingesta no contiene zona horaria"
            )

    except ValueError:
        raise ValidationError(
            "Fecha de ingesta inválida"
        )
