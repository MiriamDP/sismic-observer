
import os


class Settings:
    '''Clase que agrupa todas las configuraciones de la aplicacion. 
        Permite centralizar las variables de configuración y entorno'''
    
    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:19092")

    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "earthquakes")

    KAFKA_GROUP = os.getenv("KAFKA_GROUP", "earthquake-processor")

    DATABASE_URL = os.getenv("DATABASE_URL")

    HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8081"))

    GEOJSON_PATH = os.getenv("GEOJSON_PATH","/app/data/paises.geojson")


settings = Settings()
