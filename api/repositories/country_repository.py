
import logging

from core.database import database

logger = logging.getLogger("api.country.repository")


class CountryRepository:
    """Gestiona las consultas del catálogo de países."""

    def __init__(self, db=database):
        self.db = db

    async def get_all_countries(self):
        """Obtiene todos los países."""

        async with self.db.pool.acquire() as connection:

            countries = await connection.fetch("""
                SELECT
                    adm0_a3,
                    name_es,
                    admin,
                    continent,
                    subregion
                FROM countries
                ORDER BY name_es ASC
            """)

        return [dict(country) for country in countries]


country_repository = CountryRepository()
