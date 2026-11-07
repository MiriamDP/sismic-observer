'''Modulo FastAPI para el estado del productor'''
import logging

from datetime import datetime, timezone

from fastapi import FastAPI, Request, Response, status

from config import settings

logger = logging.getLogger("producer.api")

def create_app(lifespan_handler,state):
    '''Construye y devuelve una aplicación FastAPI'''

    app = FastAPI(
        title="Sismic Observer - Producer",
        description="Extracción automática de terremotos USGS",
        version="1.0.0",
        lifespan=lifespan_handler
    )

    @app.get("/health")
    async def health(request: Request, response: Response):
        '''Endpoints de salud del servicio'''

        now = datetime.now(timezone.utc)

        age_seconds = None

        if state.last_success:
            # comrpueba cuanto tiempo ha pasado desde la ultima extraccion exitosa
            age_seconds = (now - state.last_success).total_seconds() 

        pool = request.app.state.pool
        kafka = request.app.state.kafka

        # Comprobamos que postgres funciona
        postgres_ok = False

        if pool is not None:
            try:
                postgres_ok = await pool.fetchval("SELECT 1") == 1
            except Exception:
                postgres_ok = False

        # comprobamos la disponibilidad de kafka
        kafka_ok = (kafka is not None and kafka.producer is not None)

        # comprobamos si los datos son recientes en este caso si la ultima fue hace menos de 180s se da por buena
        # son 180s porque las peticiones http tienen cierto retraso entre la peticion y la respuesta
        fresh_data = (age_seconds is not None
            and age_seconds <= max(settings.POLL_INTERVAL * 3, 180)
        )

        # comprobamos el estado de salud
        healthy = (state.running
            and postgres_ok
            and kafka_ok
            and fresh_data
            and state.consecutive_errors == 0
        )

        if not healthy:
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

        return {
            "status": "healthy" if healthy else "unhealthy",
            "service": "producer",
            "postgres": postgres_ok,
            "kafka": kafka_ok,
            "running": state.running,
            "last_attempt": (
                state.last_attempt.isoformat()
                if state.last_attempt else None
            ),
            "last_success": (
                state.last_success.isoformat()
                if state.last_success else None
            ),
            "last_success_age_seconds": age_seconds,
            "consecutive_errors": state.consecutive_errors,
            "last_error": state.last_error,
            "last_result": state.last_result
        }

    @app.get("/")
    async def root():
        '''Endpoint con información sobre el servicio'''

        return {
            "service": "Sismic Observer Producer",
            "version": "1.0.0",
            "source": settings.SOURCE,
            "mode": settings.MODE,
            "topic": settings.KAFKA_TOPIC,
            "poll_interval": settings.POLL_INTERVAL
        }

    return app