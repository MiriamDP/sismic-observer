
import logging

from core.database import database
from datetime import datetime, date, time, timedelta, timezone


logger = logging.getLogger("api")


class EarthquakeRepository:
    """Gestiona las consultas y operaciones de persistencia de terremotos."""

    def __init__(self, db=database):
        self.db = db


    def _build_filters(self, start_date, end_date, min_magnitude, max_magnitude,min_depth,
        max_depth, country_code, start_param=1):
        """Construye las condiciones SQL y sus parámetros."""

        conditions = []
        params = []

        start_param = start_param if start_param is not None else 1
        def add_param(value, condition):
                params.append(value)
                conditions.append(
                    condition.format(index=start_param + len(params) - 1)
                )

        if start_date is not None:
            add_param(
                start_date,
                "e.event_time >= ${index}"
            )

        if end_date is not None:
            add_param(
                end_date,
                "e.event_time < ${index}"
            )

        if min_magnitude is not None:
            add_param(
                min_magnitude,
                "e.magnitude >= ${index}"
            )

        if max_magnitude is not None:
            add_param(
                max_magnitude,
                "e.magnitude <= ${index}"
            )

        if min_depth is not None:
            add_param(
                min_depth,
                "e.depth_km >= ${index}"
            )

        if max_depth is not None:
            add_param(
                max_depth,
                "e.depth_km <= ${index}"
            )

        if country_code == "unassigned":
            conditions.append("e.country_code IS NULL")

        elif country_code is not None:
            add_param(
                country_code,
                "e.country_code = ${index}"
            )

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
        min_magnitude=None, max_magnitude=None,min_depth=None,
    max_depth=None,country_code=None):
        """Consulta terremotos aplicando filtros y paginación."""

        if page < 1:
            raise ValueError("La página debe ser mayor que cero")

        if not 1 <= page_size <= 100:
            raise ValueError("Tamaño de página no válido")

        offset = (page - 1) * page_size

        cte = self._build_unique_earthquakes(mode)  
        cte_params = [mode] if mode != "all" else []

        where_clause, params = self._build_filters(
            start_date,
            end_date,
            min_magnitude,
            max_magnitude,
            min_depth,
            max_depth,
            country_code
        )

        query_params = [*cte_params, *params]        

        async with self.db.pool.acquire() as connection:

            async with connection.transaction(
                readonly=True,
                isolation="repeatable_read"
            ):

                total = await connection.fetchval(
                    f"""
                    {cte}
                    SELECT COUNT(*)
                    FROM unique_earthquakes e
                    WHERE {where_clause}
                    """,
                    *params
                )

                events = await connection.fetch(
                    f"""
                    {cte}
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
                    FROM unique_earthquakes e
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

    async def get_earthquake(self,id):
            """Obtiene un terremoto según su id"""
    
            async with self.db.pool.acquire() as connection:
    
                async with connection.transaction(
                    readonly=True,
                    isolation="repeatable_read"
                ):
    
                    earthquake = await connection.fetchrow(
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
                        WHERE e.event_id = $1
                        """,
                        id
                    )

            return dict(earthquake) if earthquake else None
    


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

    def _get_time_granularity(self, start_date, end_date):

        days = (end_date - start_date).days + 1

        if days == 1:
            return "hour"

        if days <= 90:
            return "day"

        return "month"


    def _get_bucket_interval(self, granularity):

        intervals = {
            "hour": "1 hour",
            "day": "1 day",
            "month": "1 month"
        }

        return intervals[granularity]

    async def get_earthquake_metrics(
        self,
        mode="all",
        start_date=None,
        end_date=None,
        min_magnitude=None,
        max_magnitude=None,
        min_depth=None,
        max_depth=None,
        country_code=None
    ):

        if start_date > end_date:
            raise ValueError("Periodo no válido")

        granularity = self._get_time_granularity(
            start_date,
            end_date
        )

        bucket_interval = self._get_bucket_interval(
            granularity
        )

        start_datetime = datetime.combine(
            start_date,
            time.min,
            tzinfo=timezone.utc
        )

        end_datetime = datetime.combine(
            end_date + timedelta(days=1),
            time.min,
            tzinfo=timezone.utc
        )

        # CTE de terremotos únicos
        cte = self._build_unique_earthquakes(mode)

        # Parámetros propios de la CTE
        cte_params = [mode] if mode != "all" else []

        # Filtros adicionales
        where_clause, params = self._build_filters(
            start_datetime,
            end_datetime,
            min_magnitude,
            max_magnitude,
            min_depth,
            max_depth,
            country_code,
            start_param=len(cte_params) + 1
        )

        query_params = [*cte_params, *params]

        async with self.db.pool.acquire() as connection:

            async with connection.transaction(
                readonly=True,
                isolation="repeatable_read"
            ):

                # ------------------------------------
                # 1. RESUMEN GENERAL
                # ------------------------------------

                summary = await connection.fetchrow(
                    f"""
                    {cte}

                    SELECT
                        COUNT(*) AS total_earthquakes,
                        AVG(e.magnitude) AS average_magnitude,
                        AVG(e.depth_km) AS average_depth

                    FROM unique_earthquakes e

                    WHERE {where_clause}
                    """,
                    *query_params
                )

                # ------------------------------------
                # 2. TERREMOTO MÁXIMO
                # ------------------------------------

                max_earthquake = await connection.fetchrow(
                    f"""
                    {cte}

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

                        COALESCE(
                            c.name_es,
                            'Sin país asignado'
                        ) AS country_name,

                        c.continent,
                        c.subregion

                    FROM unique_earthquakes e

                    LEFT JOIN countries c
                        ON c.adm0_a3 = e.country_code

                    WHERE {where_clause}

                    ORDER BY
                        e.magnitude DESC NULLS LAST,
                        e.event_time DESC

                    LIMIT 1
                    """,
                    *query_params
                )

                # ------------------------------------
                # 3. TOP 3 PAÍSES
                # ------------------------------------

                top_countries = await connection.fetch(
                    f"""
                    {cte},

                    filtered AS (

                        SELECT
                            e.*,

                            COALESCE(
                                c.name_es,
                                'Sin país asignado'
                            ) AS country_name,

                            c.continent,
                            c.subregion

                        FROM unique_earthquakes e

                        LEFT JOIN countries c
                            ON c.adm0_a3 = e.country_code

                        WHERE {where_clause}

                    ),

                    country_stats AS (

                        SELECT
                            country_code,
                            country_name,
                            COUNT(*) AS total_earthquakes,
                            AVG(magnitude) AS average_magnitude

                        FROM filtered

                        GROUP BY
                            country_code,
                            country_name

                    ),

                    country_max AS (

                        SELECT
                            *,

                            ROW_NUMBER() OVER (
                                PARTITION BY country_code

                                ORDER BY
                                    magnitude DESC NULLS LAST,
                                    event_time DESC

                            ) AS position

                        FROM filtered

                    )

                    SELECT
                        cs.country_code,
                        cs.country_name,
                        cs.total_earthquakes,
                        cs.average_magnitude,

                        cm.event_id,
                        cm.mode,
                        cm.source,
                        cm.magnitude,
                        cm.magnitude_type,
                        cm.event_type,
                        cm.place,
                        cm.longitude,
                        cm.latitude,
                        cm.depth_km,
                        cm.event_time,
                        cm.updated,
                        cm.continent,
                        cm.subregion

                    FROM country_stats cs

                    LEFT JOIN country_max cm
                        ON cm.country_code IS NOT DISTINCT FROM cs.country_code
                        AND cm.position = 1

                    ORDER BY
                        cs.total_earthquakes DESC,
                        cs.country_name ASC

                    LIMIT 3
                    """,
                    *query_params
                )

                # ------------------------------------
                # 4. DISTRIBUCIÓN TEMPORAL
                # ------------------------------------

                start_param = len(query_params) + 1
                end_param = len(query_params) + 2

                time_distribution = await connection.fetch(
                    f"""
                    {cte},

                    intervals AS (

                        SELECT generate_series(

                            date_trunc(
                                '{granularity}',
                                ${start_param}::timestamptz
                            ),

                            date_trunc(
                                '{granularity}',
                                ${end_param}::timestamptz
                                - interval '1 microsecond'
                            ),

                            interval '{bucket_interval}'

                        ) AS bucket

                    ),

                    earthquake_counts AS (

                        SELECT
                            date_trunc(
                                '{granularity}',
                                e.event_time
                            ) AS bucket,

                            COUNT(*) AS total

                        FROM unique_earthquakes e

                        WHERE {where_clause}

                        GROUP BY bucket

                    )

                    SELECT
                        intervals.bucket AS date,

                        COALESCE(
                            earthquake_counts.total,
                            0
                        ) AS count

                    FROM intervals

                    LEFT JOIN earthquake_counts
                        ON intervals.bucket = earthquake_counts.bucket

                    ORDER BY intervals.bucket
                    """,
                    *query_params,
                    start_datetime,
                    end_datetime
                )

                # ------------------------------------
                # 5. DISPERSIÓN MAGNITUD-PROFUNDIDAD
                # ------------------------------------

                magnitude_depth = await connection.fetch(
                    f"""
                    {cte}

                    SELECT
                        e.magnitude,
                        e.depth_km AS depth

                    FROM unique_earthquakes e

                    WHERE {where_clause}

                        AND e.magnitude IS NOT NULL
                        AND e.depth_km IS NOT NULL

                    ORDER BY hashtextextended(e.event_id, 0)

                    LIMIT 5000
                    """,
                    *query_params
                )

                # ------------------------------------
                # 6. CÁLCULO DE MEDIAS
                # ------------------------------------

                total = summary["total_earthquakes"]

                days = (end_date - start_date).days + 1

                average_per_day = None
                average_per_hour = None

                if days == 1:

                    average_per_hour = round(total / 24, 2)

                else:

                    average_per_day = round(total / days, 2)

                # ------------------------------------
                # 7. CONSTRUIR TERREMOTO MÁXIMO
                # ------------------------------------

                max_earthquake_data = (
                    dict(max_earthquake)
                    if max_earthquake
                    else None
                )

                # ------------------------------------
                # 8. CONSTRUIR TOP 3
                # ------------------------------------

                countries_data = []

                for country in top_countries:

                    country = dict(country)

                    earthquake_data = None

                    if country["event_id"] is not None:

                        earthquake_data = {
                            key: country[key]
                            for key in [
                                "event_id",
                                "mode",
                                "source",
                                "magnitude",
                                "magnitude_type",
                                "event_type",
                                "place",
                                "longitude",
                                "latitude",
                                "depth_km",
                                "event_time",
                                "updated",
                                "continent",
                                "subregion"
                            ]
                        }

                        earthquake_data["country_code"] = (
                            country["country_code"]
                        )

                        earthquake_data["country_name"] = (
                            country["country_name"]
                        )

                    countries_data.append({

                        "country_code": country["country_code"],

                        "country_name": country["country_name"],

                        "total_earthquakes": country["total_earthquakes"],

                        "average_magnitude": (
                            round(country["average_magnitude"], 2)
                            if country["average_magnitude"] is not None
                            else None
                        ),

                        "max_earthquake": earthquake_data

                    })

                # ------------------------------------
                # 9. RESPUESTA
                # ------------------------------------

                return {

                    "summary": {

                        "total_earthquakes": total,

                        "average_magnitude": (
                            round(summary["average_magnitude"], 2)
                            if summary["average_magnitude"] is not None
                            else None
                        ),

                        "average_depth": (
                            round(summary["average_depth"], 2)
                            if summary["average_depth"] is not None
                            else None
                        ),

                        "average_per_day": average_per_day,

                        "average_per_hour": average_per_hour,

                        "max_earthquake": max_earthquake_data

                    },

                    "top_countries": countries_data,

                    "time_distribution": {

                        "granularity": granularity,

                        "data": [
                            {
                                "date": row["date"],
                                "count": row["count"]
                            }
                            for row in time_distribution
                        ]

                    },

                    "magnitude_depth": [

                        {
                            "magnitude": row["magnitude"],
                            "depth": row["depth"]
                        }

                        for row in magnitude_depth

                    ]

                }

    def _build_unique_earthquakes(self, mode="all"):

        if mode == "all":

            return """
                WITH ranked_earthquakes AS (

                    SELECT
                        e.*,

                        ROW_NUMBER() OVER (
                            PARTITION BY e.event_id

                            ORDER BY
                                CASE
                                    WHEN e.mode = 'live' THEN 1
                                    WHEN e.mode = 'replay' THEN 2
                                    ELSE 3
                                END,

                                e.updated DESC NULLS LAST

                        ) AS version_rank

                    FROM earthquakes e

                ),

                unique_earthquakes AS (

                    SELECT *
                    FROM ranked_earthquakes
                    WHERE version_rank = 1

                )
            """

        return """
            WITH unique_earthquakes AS (

                SELECT *
                FROM earthquakes

            )
        """  


earthquake_repository = EarthquakeRepository()
