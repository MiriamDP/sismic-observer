
from math import ceil

from repositories.earthquake_repository import earthquake_repository


class EarthquakeService:
    """Gestiona la lógica de consulta de terremotos."""

    def __init__(self, repository):
        self.repository = repository


    async def get_earthquakes(self, page=1, page_size=20, mode="live", start_date=None, end_date=None, 
        min_magnitude=None, max_magnitude=None, country_code=None):
        """Consulta todos los terremotos y construye la respuesta paginada."""

        result = await self.repository.get_earthquakes(
            page=page,
            page_size=page_size,
            mode=mode,
            start_date=start_date,
            end_date=end_date,
            min_magnitude=min_magnitude,
            max_magnitude=max_magnitude,
            country_code=country_code
        )

        total = result["total_items"]

        return {
            "items": result["items"],
            "pagination": self._build_pagination(
                page,
                page_size,
                total
            ),
            "filters": {
                "mode": mode,
                "start_date": start_date,
                "end_date": end_date,
                "min_magnitude": min_magnitude,
                "max_magnitude": max_magnitude,
                "country_code": country_code
            }
        }

    async def get_earthquake(self,id):
        result = await earthquake_repository.get_earthquake(id)
        return result if result else None


    @staticmethod
    def _build_pagination(page, page_size, total):
        """Construye los metadatos de paginación."""

        total_pages = ceil(total / page_size)

        return {
            "page": page,
            "page_size": page_size,
            "total_items": total,
            "total_pages": total_pages,
            "has_next": page < total_pages,
            "has_previous": page > 1
        }

    async def get_latest_earthquakes(self, limit=20):
        """Obtiene los últimos terremotos."""

        return await self.repository.get_latest_earthquakes(limit=limit)


earthquake_service = EarthquakeService(
    repository=earthquake_repository
)
