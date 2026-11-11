
import logging

from datetime import timedelta

from core.database import database


logger = logging.getLogger("api.import.repository")


class ImportRepository:
    """Gestiona las operaciones de persistencia de las importaciones históricas."""

    def __init__(self, db=database):
        self.db = db

    async def update_job(self, job_id, **values):
        """Actualiza los campos de una importación."""

        allowed = {
            "status",
            "events_published",
            "intervals_completed",
            "current_interval_start",
            "current_interval_end",
            "last_completed_interval",
            "error",
            "started_at",
            "finished_at"
        }

        invalid_fields = set(values) - allowed

        if invalid_fields:
            raise ValueError(
                f"Campos no permitidos: {invalid_fields}"
            )

        if not values:
            return

        assignments = ", ".join(
            f"{key} = ${index}"
            for index, key in enumerate(values, start=1)
        )

        params = list(values.values())
        params.append(job_id)

        async with self.db.pool.acquire() as connection:

            await connection.execute(
                f"""
                UPDATE earthquake_import_jobs
                SET {assignments}
                WHERE id = ${len(params)}
                """,
                *params
            )


    async def increment_completed_interval(
        self,
        job_id,
        completed_date
    ):
        """Incrementa el contador de intervalos completados."""

        async with self.db.pool.acquire() as connection:

            await connection.execute("""
                UPDATE earthquake_import_jobs
                SET intervals_completed = intervals_completed + 1,
                    last_completed_interval = $1
                WHERE id = $2
            """,
                completed_date,
                job_id
            )

    async def increment_published_events(self, job_id, count):
        """Incrementa el contador de eventos publicados en Kafka."""

        if count <= 0:
            return

        async with self.db.pool.acquire() as connection:

            await connection.execute("""
                UPDATE earthquake_import_jobs
                SET events_published = events_published + $1
                WHERE id = $2
            """,
                count,
                job_id
            )

    async def create_import_job(
        self,
        start_date,
        end_date,
        min_magnitude
    ):
        """Crea una nueva importación histórica."""

        async with self.db.pool.acquire() as connection:

            job_id = await connection.fetchval("""
                INSERT INTO earthquake_import_jobs (
                    status,
                    start_date,
                    end_date,
                    min_magnitude
                )
                VALUES ('running', $1, $2, $3)
                RETURNING id
            """,
                start_date,
                end_date,
                min_magnitude
            )

        return job_id


    async def get_missing_ranges(self, start_date, end_date, min_magnitude=None):
        """
        Devuelve los intervalos que todavía no están cubiertos
        por importaciones históricas completadas.
        """

        async with self.db.pool.acquire() as connection:

            completed_jobs = await connection.fetch("""
                SELECT start_date, end_date
                FROM earthquake_import_jobs
                WHERE status = 'completed'
                  AND min_magnitude IS NOT DISTINCT FROM $1
                  AND start_date <= $2
                  AND end_date >= $3
                ORDER BY start_date
            """,
                min_magnitude,
                end_date,
                start_date
            )

        # Unir intervalos importados
        merged = []

        for job in completed_jobs:

            job_start = job["start_date"]
            job_end = job["end_date"]

            if (
                not merged
                or job_start > merged[-1][1] + timedelta(days=1)
            ):
                merged.append([job_start, job_end])

            else:
                merged[-1][1] = max(
                    merged[-1][1],
                    job_end
                )

        # Encontrar los intervalos pendientes
        missing = []

        current = start_date

        for imported_start, imported_end in merged:

            if current < imported_start:

                missing.append((
                    current,
                    imported_start - timedelta(days=1)
                ))

            current = max(
                current,
                imported_end + timedelta(days=1)
            )

        if current <= end_date:
            missing.append((current, end_date))

        return missing


    async def get_job_status(self, job_id):
        """Devuelve el estado de una importación."""

        async with self.db.pool.acquire() as connection:

            return await connection.fetchval("""
                SELECT status
                FROM earthquake_import_jobs
                WHERE id = $1
            """,
                job_id
            )


    async def get_latest_job(self):
        """Devuelve la última importación registrada."""

        async with self.db.pool.acquire() as connection:

            job = await connection.fetchrow("""
                SELECT *
                FROM earthquake_import_jobs
                ORDER BY
                    CASE
                        WHEN status = 'running' THEN 0
                        ELSE 1
                    END,
                    id DESC
                LIMIT 1
            """)

        return dict(job) if job else None

    async def get_all_import_jobs(self):
        """Devuelve todas las importaciones históricas registradas."""

        async with self.db.pool.acquire() as connection:

            jobs = await connection.fetch("""
                SELECT *
                FROM earthquake_import_jobs
                ORDER BY id DESC
            """)

        return [dict(job) for job in jobs]


import_repository = ImportRepository()
