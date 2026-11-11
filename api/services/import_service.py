
import asyncio
import logging
import asyncpg

from datetime import date, datetime, time, timedelta, timezone

from services.usgs_service import usgs_service
from repositories.import_repository import import_repository
from kafka.kafka_replay import replay_publisher
from core.config import settings
from kafka.kafka_replay import (
    replay_publisher,
    KafkaPublishError
)


logger = logging.getLogger("api.import.service")


class ImportService:
    """Coordina las importaciones históricas de terremotos."""

    def __init__(self, repository, usgs_service, replay_publisher):

        self.repository = repository
        self.usgs_service = usgs_service
        self.replay_publisher = replay_publisher
        self.active_tasks = {}


    async def import_interval(self, job_id, start, end, min_magnitude):
        """Consulta USGS y publica los eventos en Kafka. Para un intervalo de tiempo dato"""

        features = await self.usgs_service.fetch_events(
            start,
            end,
            min_magnitude
        )

        # Si alcanzamos el límite de USGS, dividimos el intervalo.
        if len(features) >= settings.USGS_LIMIT:

            duration = end - start

            if duration <= timedelta(milliseconds=2):

                raise RuntimeError(
                    "Intervalo demasiado pequeño para dividir "
                    "sin superar el límite de USGS"
                )

            midpoint = start + duration / 2

            first_published = await self.import_interval(
                job_id,
                start,
                midpoint,
                min_magnitude
            )

            second_published = await self.import_interval(
                job_id,
                midpoint + timedelta(milliseconds=1),
                end,
                min_magnitude
            )

            return first_published + second_published

        # Publicar eventos en Kafka.
        
        async def update_progress(count):
            await self.repository.increment_published_events(
                job_id,
                count
            )

        published = await self.replay_publisher.publish_many(
            features,
            on_progress=update_progress,
            batch_size=100
        )

        logger.info(
            "Intervalo publicado: %s - %s | recibidos=%s | publicados=%s",
            start,
            end,
            len(features),
            published
        )

        return published

    # --------------------------------------------------
    # EJECUTAR IMPORTACIÓN HISTÓRICA
    # --------------------------------------------------

    async def run_import(self, job_id, start_date, end_date, min_magnitude):
        """Ejecuta una importación histórica completa."""

        try:

            await self.repository.update_job(
                job_id,
                status="running",
                started_at=datetime.now(timezone.utc),
                error=None
            )

            start = datetime.combine(
                start_date,
                time.min,
                tzinfo=timezone.utc
            )

            end = datetime.combine(
                end_date + timedelta(days=1),
                time.min,
                tzinfo=timezone.utc
            ) - timedelta(milliseconds=1)

            current = start

            while current <= end:

                next_month = (
                    current.replace(day=1)
                    + timedelta(days=32)
                ).replace(day=1)

                interval_end = min(
                    next_month - timedelta(milliseconds=1),
                    end
                )

                await self.repository.update_job(
                    job_id,
                    current_interval_start=current,
                    current_interval_end=interval_end
                )

                await self.import_interval(
                    job_id,
                    current,
                    interval_end,
                    min_magnitude
                )

                await self.repository.increment_completed_interval(
                    job_id,
                    interval_end.date()
                )

                logger.info(
                    "Intervalo completado: job=%s | %s - %s",
                    job_id,
                    current,
                    interval_end
                )

                current = interval_end + timedelta(milliseconds=1)

            # Finalizar importación.
            await self.repository.update_job(
                job_id,
                status="completed",
                finished_at=datetime.now(timezone.utc),
                current_interval_start=None,
                current_interval_end=None
            )

            logger.info(
                "Importación histórica finalizada: job=%s",
                job_id
            )

        except asyncio.CancelledError:

            logger.warning(
                "Importación interrumpida: job=%s",
                job_id
            )

            try:

                await self.repository.update_job(
                    job_id,
                    status="interrupted",
                    finished_at=datetime.now(timezone.utc)
                )

            except Exception:

                logger.exception(
                    "No se pudo actualizar el estado de la "
                    "importación interrumpida: job=%s",
                    job_id
                )

            raise

        except Exception as exc:

            logger.exception(
                "Error en importación histórica: job=%s",
                job_id
            )

            try:

                await self.repository.update_job(
                    job_id,
                    status="failed",
                    error=str(exc)[:2000],
                    finished_at=datetime.now(timezone.utc)
                )

            except Exception:

                logger.exception(
                    "No se pudo actualizar el estado de la "
                    "importación fallida: job=%s",
                    job_id
                )

            raise


    async def automatic_historical_import(self):
        """Detecta e importa los periodos históricos pendientes."""

        logger.info("Iniciando comprobación de importaciones históricas")

        start_date = settings.HISTORICAL_IMPORT_START_DATE

        # El productor live se encarga del día actual.
        end_date = datetime.now(timezone.utc).date() - timedelta(days=1)

        min_magnitude = (
            float(settings.HISTORICAL_IMPORT_MIN_MAGNITUDE)
            if settings.HISTORICAL_IMPORT_MIN_MAGNITUDE.strip()
            else None
        )

        if start_date > end_date:

            logger.info(
                "No hay periodo histórico que importar"
            )

            return

        missing_ranges = await self.repository.get_missing_ranges(
            start_date,
            end_date,
            min_magnitude
        )

        if not missing_ranges:

            logger.info(
                "El histórico está completo: %s - %s",
                start_date,
                end_date
            )

            return

        logger.info(
            "Se han encontrado %s intervalos pendientes",
            len(missing_ranges)
        )

        for missing_start, missing_end in missing_ranges:

            logger.info(
                "Importando periodo pendiente: %s - %s",
                missing_start,
                missing_end
            )

            try:

                job_id = await self.start_import(
                    missing_start,
                    missing_end,
                    min_magnitude
                )

            except asyncpg.UniqueViolationError:

                logger.warning(
                    "Ya existe otra importación en ejecución"
                )

                return

            try:

                await self.run_import(
                    job_id,
                    missing_start,
                    missing_end,
                    min_magnitude
                )

            except asyncio.CancelledError:

                raise

            except Exception:

                logger.exception(
                    "La importación automática %s ha fallado. "
                    "Se detienen las siguientes importaciones.",
                    job_id
                )

                return

        logger.info(
            "Importación histórica automática finalizada"
        )


    async def start_import(self, start_date, end_date, min_magnitude):
        """Valida y programa una importación histórica."""

        if start_date > end_date:
            raise ValueError(
                "La fecha inicial debe ser anterior a la final"
            )

        if end_date > date.today():
            raise ValueError(
                "La fecha final no puede estar en el futuro"
            )

        job_id = await self.repository.create_import_job(
            start_date,
            end_date,
            min_magnitude
        )

        task = asyncio.create_task(
            self.run_import(
                job_id,
                start_date,
                end_date,
                min_magnitude
            ),
            name=f"historical_import_{job_id}"
        )

        self.active_tasks[job_id] = task

        task.add_done_callback(
            lambda completed_task: self.active_tasks.pop(
                job_id,
                None
            )
        )

        return job_id


    async def get_import_status(self):
        """Obtiene el estado de la última importación."""

        return await self.repository.get_latest_job()
    
    async def get_all_import_jobs(self):
        """Obtiene todas las importaciones históricas."""

        return await self.repository.get_all_import_jobs()


import_service = ImportService(
    repository=import_repository,
    usgs_service=usgs_service,
    replay_publisher=replay_publisher
)
