
import logging

from datetime import datetime
from datetime import date

from fastapi import APIRouter, HTTPException, Query

from models.earthquake_models import EarthquakeListResponse, EarthquakeResponse
from services.earthquake_service import earthquake_service

from models.earthquake_models import (
    EarthquakeAnalysisFilters,
    EarthquakeMetricsResponse
)


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
    min_depth: float | None = None,
    max_depth: float | None = None,
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
    
    if (
        min_depth is not None
        and max_depth is not None
        and min_depth > max_depth
    ):
        raise HTTPException(
            status_code=400,
            detail="La profundidad mínima no puede superar la máxima"
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
            min_depth=min_depth,
            max_depth=max_depth,
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

@router.get("/latest",response_model=list[EarthquakeResponse])
async def get_latest(
    limit: int = 3,
):
    return await earthquake_service.get_latest_earthquakes(limit=limit)



@router.get(
    "/metrics",
    response_model=EarthquakeMetricsResponse
)
async def get_earthquake_metrics(

    mode: str = Query(
        default="all",
        pattern="^(live|replay|all)$"
    ),

    start_date: date = Query(...),
    end_date: date = Query(...),

    min_magnitude: float | None = None,
    max_magnitude: float | None = None,

    min_depth: float | None = None,
    max_depth: float | None = None,

    country_code: str | None = None

):

    if start_date > end_date:
        raise HTTPException(
            status_code=400,
            detail="La fecha inicial debe ser anterior o igual a la final"
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

    if (
        min_depth is not None
        and max_depth is not None
        and min_depth > max_depth
    ):
        raise HTTPException(
            status_code=400,
            detail="La profundidad mínima no puede superar la máxima"
        )

    filters = EarthquakeAnalysisFilters(
        mode=mode,
        start_date=start_date,
        end_date=end_date,
        min_magnitude=min_magnitude,
        max_magnitude=max_magnitude,
        min_depth=min_depth,
        max_depth=max_depth,
        country_code=country_code
    )

    try:

        return await earthquake_service.get_earthquake_metrics(
            filters
        )

    except Exception:

        logger.exception(
            "Error calculando estadísticas de terremotos"
        )

        raise HTTPException(
            status_code=500,
            detail="Error calculating earthquake metrics"
        )

@router.get("/{earthquake_id}")
async def get_earthquake(earthquake_id):
    earthquake = await earthquake_service.get_earthquake(earthquake_id)

    if earthquake is None:
        raise HTTPException(
            status_code=404,
            detail="Terremoto no encontrado"
        )

    return earthquake