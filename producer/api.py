'''Modulo FastAPI para el estado y recepción de extracciones del productor'''

import asyncio
import logging

from datetime import datetime, timezone

from fastapi import (
    FastAPI,
    Request,
    Response,
    status,
    HTTPException
)

from pydantic import BaseModel, Field, ConfigDict, model_validator

from config import settings
from extraction import process_received_extraction


logger = logging.getLogger("producer.api")


class ExtractionRequest(BaseModel):
    """Modelo de recepción de una extracción externa."""

    model_config = ConfigDict(extra="forbid")

    data: dict
    original_body: str
    request_url: str = Field(min_length=1)
    requested_at: datetime
    completed_at: datetime
    http_status: int = Field(ge=100, le=599)

    @model_validator(mode="after")
    def validate_geojson(self):

        if not isinstance(self.data.get("features"), list):
            raise ValueError(
                "La extracción debe contener una lista features válida"
            )

        if self.completed_at < self.requested_at:
            raise ValueError(
                "completed_at no puede ser anterior a requested_at"
            )

        return self


def create_app(lifespan_handler, state):
    '''Construye y devuelve una aplicación FastAPI'''

    app = FastAPI(
        title="Sismic Observer - Producer",
        description="Extracción y publicación de terremotos",
        version="1.0.0",
        lifespan=lifespan_handler
    )

    # Evita que dos extracciones replay se procesen simultáneamente.
    app.state.extraction_lock = asyncio.Lock()

    # --------------------------------------------------
    # RECEPCIÓN DE EXTRACCIONES HISTÓRICAS
    # --------------------------------------------------

    @app.post("/extract", status_code=200)
    async def receive_extraction(
        payload: ExtractionRequest,
        request: Request
    ):
        """
        Recibe una extracción histórica desde la API principal
        y publica sus eventos en Kafka.
        """

        if settings.MODE != "replay":

            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Este endpoint solo está disponible en modo replay"
            )

        pool = getattr(request.app.state, "pool", None)
        kafka = getattr(request.app.state, "kafka", None)

        if pool is None or kafka is None:

            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="El productor no está preparado"
            )

        logger.info(
            "Extracción histórica recibida. Registros: %s",
            len(payload.data["features"])
        )

        async with request.app.state.extraction_lock:

            state.last_attempt = datetime.now(timezone.utc)

            try:

                result = await process_received_extraction(
                    pool=pool,
                    kafka=kafka,
                    data=payload.data,
                    original_body=payload.original_body,
                    request_url=payload.request_url,
                    requested_at=payload.requested_at,
                    completed_at=payload.completed_at,
                    http_status=payload.http_status
                )

                state.last_result = result

                if result["status"] == "partial" or result.get("failed", 0) > 0:

                    state.last_error = (
                        "Publicación parcial de extracción histórica"
                    )

                    state.consecutive_errors += 1

                    logger.warning(
                        "Extracción histórica parcialmente publicada: %s",
                        result
                    )

                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail={
                            "message": "Publicación parcial de la extracción",
                            **result
                        }
                    )

                if result["status"] != "success":

                    raise RuntimeError(
                        f"Estado de extracción inesperado: {result['status']}"
                    )

                state.last_success = datetime.now(timezone.utc)
                state.last_error = None
                state.consecutive_errors = 0

                logger.info(
                    "Extracción histórica publicada correctamente: %s",
                    result
                )

                return result

            except HTTPException:
                raise

            except Exception as exc:

                state.last_error = str(exc)
                state.consecutive_errors += 1

                logger.exception(
                    "Error procesando extracción histórica"
                )

                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Error procesando la extracción histórica"
                )

    # --------------------------------------------------
    # HEALTHCHECK
    # --------------------------------------------------

    @app.get("/health")
    async def health(request: Request, response: Response):
        '''Endpoint de salud del servicio'''

        now = datetime.now(timezone.utc)

        age_seconds = None

        if state.last_success:

            age_seconds = (
                now - state.last_success
            ).total_seconds()

        pool = getattr(request.app.state, "pool", None)
        kafka = getattr(request.app.state, "kafka", None)

        # Comprobar PostgreSQL.

        postgres_ok = False

        if pool is not None:

            try:

                postgres_ok = (
                    await pool.fetchval("SELECT 1") == 1
                )

            except Exception:

                postgres_ok = False

        # Comprobar Kafka.

        kafka_ok = (
            kafka is not None
            and kafka.producer is not None
        )

        # Salud específica según el modo.

        if settings.MODE == "live":

            fresh_data = (
                age_seconds is not None
                and age_seconds <= max(
                    settings.POLL_INTERVAL * 3,
                    180
                )
            )

            healthy = (
                state.running
                and postgres_ok
                and kafka_ok
                and fresh_data
                and state.consecutive_errors == 0
            )

        elif settings.MODE == "replay":

            # Replay no necesita extracciones periódicas.
            # Solo necesita estar disponible para recibir peticiones.

            healthy = (
                state.running
                and postgres_ok
                and kafka_ok
                and state.consecutive_errors == 0
            )

        else:

            healthy = False

        if not healthy:

            response.status_code = (
                status.HTTP_503_SERVICE_UNAVAILABLE
            )

        return {
            "status": "healthy" if healthy else "unhealthy",
            "service": "producer",
            "mode": settings.MODE,
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

    # --------------------------------------------------
    # INFORMACIÓN DEL SERVICIO
    # --------------------------------------------------

    @app.get("/")
    async def root():
        '''Endpoint con información sobre el servicio'''

        return {
            "service": "Sismic Observer Producer",
            "version": "1.0.0",
            "source": settings.SOURCE,
            "mode": settings.MODE,
            "topic": settings.KAFKA_TOPIC,
            "poll_interval": (
                settings.POLL_INTERVAL
                if settings.MODE == "live"
                else None
            )
        }

    return app