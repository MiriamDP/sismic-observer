'''Modulo para la transformación de datos del mensaje'''
import hashlib
import json

from datetime import datetime, timezone


def milliseconds_to_utc(value: int) -> datetime:

    return datetime.fromtimestamp(
        value / 1000,
        tz=timezone.utc
    )


def transform_message(data: dict) -> dict:

    event = data["event"]
    properties = event["properties"]

    coordinates = event["geometry"]["coordinates"]

    longitude = float(coordinates[0])
    latitude = float(coordinates[1])

    # La profundidad puede faltar.
    depth = coordinates[2]

    if depth is not None:
        depth = float(depth)

    # La magnitud también puede faltar.
    magnitude = properties.get("mag")

    if magnitude is not None:
        magnitude = float(magnitude)

    payload_hash = hashlib.sha256(
        json.dumps(
            data,
            sort_keys=True,
            separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()

    ingested_at = datetime.fromisoformat(
        data["ingested_at"].replace("Z", "+00:00")
    )

    return {
        "event_id": event["id"],
        "mode": data["mode"],
        "source": data["source"],
        "payload_hash": payload_hash,

        "magnitude": magnitude,
        "magnitude_type": properties.get("magType"),
        "event_type": properties.get("type"),
        "place": properties.get("place"),

        "longitude": longitude,
        "latitude": latitude,
        "depth_km": depth,

        "event_time": milliseconds_to_utc(
            properties["time"]
        ),

        "updated": milliseconds_to_utc(
            properties["updated"]
        ),

        "updated_ms": properties["updated"],

        "ingested_at": ingested_at,

        "original_message": data
    }
