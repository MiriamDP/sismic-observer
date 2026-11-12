
import logging
import asyncpg

from fastapi import APIRouter, HTTPException

from models.import_models import ImportRequest
from services.import_service import import_service


logger = logging.getLogger("api")


router = APIRouter(
    prefix="/api/earthquakes/import-history",
    tags=["Historical Imports"]
)

@router.post("", status_code=202)
async def start_import(request: ImportRequest):
    """Inicia una importación histórica."""

    try:

        job_id, task = await import_service.start_import(
            request.start_date,
            request.end_date,
            request.min_magnitude
        )

        return {
            "message": "Importación histórica iniciada",
            "job_id": job_id,
            "status": "running"
        }

    except ValueError as exc:

        raise HTTPException(
            status_code=400,
            detail=str(exc)
        )

    except asyncpg.UniqueViolationError:

        raise HTTPException(
            status_code=409,
            detail="Ya existe una importación histórica en ejecución"
        )

    except Exception:

        logger.exception(
            "Error iniciando importación histórica"
        )

        raise HTTPException(
            status_code=500,
            detail="Error starting historical import"
        )


@router.get("/status")
async def import_status():
    """Consulta el estado de la última importación."""

    try:

        job = await import_service.get_import_status()

        if not job:

            return {
                "message": "Todavía no hay importaciones",
                "job": None
            }

        return {
            "job": job
        }

    except Exception:

        logger.exception(
            "Error consultando el estado de importación"
        )

        raise HTTPException(
            status_code=500,
            detail="Error retrieving import status"
        )


@router.get("/jobs")
async def get_all_import_jobs():
    """Devuelve todas las importaciones históricas."""

    try:
        return await import_service.get_all_import_jobs()

    except Exception:
        logger.exception(
            "Error obteniendo las importaciones históricas"
        )

        raise HTTPException(
            status_code=500,
            detail="Error obteniendo las importaciones históricas"
        )