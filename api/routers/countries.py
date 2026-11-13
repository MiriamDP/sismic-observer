from fastapi import APIRouter, HTTPException, status

from services.country_service import country_service
from models.country import CountryResponse


router = APIRouter(
    prefix="/api/countries",
    tags=["Countries"]
)


@router.get(
    "",
    response_model=list[CountryResponse],
    status_code=status.HTTP_200_OK
)
async def get_countries():

    try:
        return await country_service.get_all_countries()

    except Exception:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error al obtener los países"
        )