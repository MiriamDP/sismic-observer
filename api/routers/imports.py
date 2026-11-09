
import logging

from fastapi import APIRouter, HTTPException

from models.import_models import ImportRequest
from services.import_service import import_service


logger = logging.getLogger("api")


router = APIRouter(
    prefix="/earthquakes/import-history",
    tags=["Historical Imports"]
)

@router.post("", status_code=202)
async def start_import(request: ImportRequest):
    """Inicia una importación histórica."""

    try:

        job_id = await import_service.start_import(
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
