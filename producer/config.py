
import os


class Settings:
    '''Clase que agrupa todas las configuraciones de la aplicacion. 
    Permite centralizar las variables de configuración y entorno'''

    # Kafka
    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:19092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "earthquakes-live")

    # PostgreSQL
    DATABASE_URL = os.getenv("DATABASE_URL")

    # USGS
    USGS_URL = os.getenv(
        "USGS_URL",
        "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
    )

    POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))

    # Metadatos
    SOURCE = os.getenv("SOURCE", "USGS")
    MODE = os.getenv("MODE", "live")

    # HTTP
    HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "30"))
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))

    # Retención de originales
    RAW_RETENTION_DAYS = int(os.getenv("RAW_RETENTION_DAYS", "30"))


settings = Settings()
