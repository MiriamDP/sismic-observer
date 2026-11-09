
import logging

from datetime import datetime

from fastapi import APIRouter, HTTPException, Query

from models.earthquake_models import EarthquakeListResponse
from services.earthquake_service import earthquake_service


logger = logging.getLogger("api.router.earthquake")


router = APIRouter(
    prefix="/api/earthquakes",
    tags=["Earthquakes"]
)



@router.get("",response_model=EarthquakeListResponse)
async def get_earthquakes(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    mode: str = Query(
        default="all",
        pattern="^(live|replay|all)$"
    ),
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    min_magnitude: float | None = None,
    max_magnitude: float | None = None,
    country_code: str | None = None
):
    """Consulta terremotos con filtros y paginación."""


    if (
        start_date is not None
        and end_date is not None
        and start_date >= end_date
    ):
        raise HTTPException(
            status_code=400,
            detail="La fecha inicial debe ser anterior a la final"
        )

    if (
        min_magnitude is not None
        and max_magnitude is not None
        and min_magnitude > max_magnitude
    ):
        raise HTTPException(
            status_code=400,
            detail="La magnitud mínima no puede superar la máxima"
        )


    try:

        return await earthquake_service.get_earthquakes(
            page=page,
            page_size=page_size,
            mode=mode,
            start_date=start_date,
            end_date=end_date,
            min_magnitude=min_magnitude,
            max_magnitude=max_magnitude,
            country_code=country_code
        )

    except Exception:

        logger.exception(
            "Error consultando terremotos"
        )

        raise HTTPException(
            status_code=500,
            detail="Error retrieving earthquakes"
        )
