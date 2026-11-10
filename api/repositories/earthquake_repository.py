
import logging

from core.database import database


logger = logging.getLogger("api")


class EarthquakeRepository:
    """Gestiona las consultas y operaciones de persistencia de terremotos."""

    def __init__(self, db=database):
        self.db = db


    def _build_filters(self, mode, start_date, end_date, min_magnitude, max_magnitude, country_code):
        """Construye las condiciones SQL y sus parámetros."""

        conditions = []
        params = []

        if mode != "all":
            params.append(mode)
            conditions.append(f"e.mode = ${len(params)}")

        if start_date is not None:
            params.append(start_date)
            conditions.append(f"e.event_time >= ${len(params)}")

        if end_date is not None:
            params.append(end_date)
            conditions.append(f"e.event_time < ${len(params)}")

        if min_magnitude is not None:
            params.append(min_magnitude)
            conditions.append(f"e.magnitude >= ${len(params)}")

        if max_magnitude is not None:
            params.append(max_magnitude)
            conditions.append(f"e.magnitude <= ${len(params)}")

        if country_code == "unassigned":
            conditions.append("e.country_code IS NULL")

        elif country_code is not None:
            params.append(country_code)
            conditions.append(f"e.country_code = ${len(params)}")

        where_clause = (
            " AND ".join(conditions)
            if conditions
            else "TRUE"
        )

        return where_clause, params
    # --------------------------------------------------
    # CONSULTAR TERREMOTOS
    # --------------------------------------------------

    async def get_earthquakes(self, page=1, page_size=20, mode="all", start_date=None, end_date=None, 
        min_magnitude=None, max_magnitude=None, country_code=None):
        """Consulta terremotos aplicando filtros y paginación."""

        if page < 1:
            raise ValueError("La página debe ser mayor que cero")

        if not 1 <= page_size <= 100:
            raise ValueError("Tamaño de página no válido")

        offset = (page - 1) * page_size

        where_clause, params = self._build_filters(
            mode,
            start_date,
            end_date,
            min_magnitude,
            max_magnitude,
            country_code
        )

        async with self.db.pool.acquire() as connection:

            async with connection.transaction(
                readonly=True,
                isolation="repeatable_read"
            ):

                total = await connection.fetchval(
                    f"""
                    SELECT COUNT(*)
                    FROM earthquakes e
                    WHERE {where_clause}
                    """,
                    *params
                )

                events = await connection.fetch(
                    f"""
                    SELECT
                        e.event_id,
                        e.mode,
                        e.source,
                        e.magnitude,
                        e.magnitude_type,
                        e.event_type,
                        e.place,
                        e.longitude,
                        e.latitude,
                        e.depth_km,
                        e.event_time,
                        e.updated,
                        e.country_code,
                        COALESCE(c.name_es, 'Sin país asignado')
                            AS country_name,
                        c.continent,
                        c.subregion
                    FROM earthquakes e
                    LEFT JOIN countries c
                        ON c.adm0_a3 = e.country_code
                    WHERE {where_clause}
                    ORDER BY e.event_time DESC, e.event_id DESC
                    LIMIT ${len(params) + 1}
                    OFFSET ${len(params) + 2}
                    """,
                    *params,
                    page_size,
                    offset
                )

        return {
            "items": [dict(event) for event in events],
            "total_items": total
        }


    async def get_latest_earthquakes(self, limit=20):
        """Obtiene los últimos terremotos registrados."""

        if not 1 <= limit <= 100:
            raise ValueError("Límite de resultados no válido")

        async with self.db.pool.acquire() as connection:

            events = await connection.fetch(
                """
                SELECT
                    e.event_id,
                    e.mode,
                    e.source,
                    e.magnitude,
                    e.magnitude_type,
                    e.event_type,
                    e.place,
                    e.longitude,
                    e.latitude,
                    e.depth_km,
                    e.event_time,
                    e.updated,
                    e.country_code,
                    COALESCE(c.name_es, 'Sin país asignado')
                        AS country_name,
                    c.continent,
                    c.subregion
                FROM earthquakes e
                LEFT JOIN countries c
                    ON c.adm0_a3 = e.country_code
                ORDER BY e.event_time DESC, e.event_id DESC
                LIMIT $1
                """,
                limit
            )

        return [dict(event) for event in events]


earthquake_repository = EarthquakeRepository()
