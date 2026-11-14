'''Modulo con las variables de configuración y de entorno'''
import os
from datetime import date

class Settings:

    INTERNAL_API_TOKEN = os.getenv("INTERNAL_API_TOKEN")

    DATABASE_URL = os.getenv("DATABASE_URL")

    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP","kafka:19092")

    KAFKA_TOPIC = os.getenv("KAFKA_REPLAY_TOPIC","earthquakes-replay")

    USGS_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

    USGS_LIMIT = 20000

    HISTORICAL_IMPORT_ENABLED = (os.getenv("HISTORICAL_IMPORT_ENABLED", "false").lower() == "true")

    HISTORICAL_IMPORT_START_DATE = date.fromisoformat(os.getenv("HISTORICAL_IMPORT_START_DATE", "2025-01-01"))

    HISTORICAL_IMPORT_MIN_MAGNITUDE = os.getenv("HISTORICAL_IMPORT_MIN_MAGNITUDE","")
    REPLAY_PRODUCER_URL = os.getenv(
    "REPLAY_PRODUCER_URL",
    "producer-replay:8083"
    )

settings = Settings()