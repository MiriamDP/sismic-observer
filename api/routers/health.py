
import logging

from fastapi import APIRouter, HTTPException

from core.database import database


logger = logging.getLogger("api.router.health")


router = APIRouter(tags=["Health"])


@router.get("/health")
async def health():
    """Comprueba la disponibilidad de la API y PostgreSQL."""

    try:

        if database.pool is None:
            raise RuntimeError(
                "Database pool no está inicializado"
            )

        async with database.pool.acquire() as connection:

            await connection.execute("SELECT 1")

        return {
            "status": "ok",
            "database": "connected"
        }

    except Exception:

        logger.exception(
            "Error comprobando el estado de la API"
        )

        raise HTTPException(
            status_code=503,
            detail="Database unavailable"
        )
