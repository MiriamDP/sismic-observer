import logging

from repositories.country_repository import country_repository

logger = logging.getLogger("api.country.service")


class CountryService:
    """Gestiona la lógica de negocio del catálogo de países."""

    def __init__(self, repository=country_repository):
        self.repository = repository

    async def get_all_countries(self):

        try:
            return await self.repository.get_all_countries()

        except Exception:
            logger.exception(
                "Error al obtener el catálogo de países"
            )
            raise


country_service = CountryService()