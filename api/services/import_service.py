
import asyncio
import logging
import asyncpg

from datetime import date, datetime, time, timedelta, timezone

from services.usgs_service import usgs_service
from repositories.import_repository import import_repository
from core.config import settings
from kafka.kafka_replay_client import (
    replay_producer_client,
)


logger = logging.getLogger("api.import.service")


class ImportService:
    """Coordina las importaciones históricas de terremotos."""

    def __init__(self, repository, usgs_service, replay_client):

        self.repository = repository
        self.usgs_service = usgs_service
        self.replay_client = replay_client
        self.active_tasks = {}



    async def import_interval(self, job_id, start, end, min_magnitude):
        """Consulta USGS y envía la extracción completa a producer-replay."""

        # 1. Consultar USGS.
        extraction = await self.usgs_service.fetch_events(
            start,
            end,
            min_magnitude
        )

        # 2. Obtener los eventos del GeoJSON.
        geojson = extraction.data
        features = geojson.get("features", [])

        # 3. Si alcanzamos el límite de USGS, dividimos el intervalo.
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

        try:

            # 4. Enviar la extracción completa al productor replay.
            result = await self.replay_client.publish_extraction(
                extraction
            )

            published = result["published"]

            # 5. Actualizar el progreso de la importación.
            if published > 0:

                await self.repository.increment_published_events(
                    job_id,
                    published
                )

            # 6. Detectar publicaciones parciales.
            if result.get("status") != "success" or result.get("failed", 0) > 0:
                raise RuntimeError(
                    "Publicación parcial en producer-replay: "
                    f"confirmados={published}, "
                    f"fallidos={result.get('failed', 0)}"
                )

        except Exception:

            logger.exception(
                "Error procesando intervalo: %s - %s",
                start,
                end
            )

            raise

        logger.info(
            "Intervalo procesado: %s - %s | recibidos=%s | publicados=%s",
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
        """Ejecuta o reanuda una importación histórica."""

        try:

            job = await self.repository.get_job(job_id)

            if job is None:
                raise ValueError(
                    f"No existe la importación {job_id}"
                )

            # Determinar desde dónde continuar.
            if job["current_interval_start"] is not None:

                start = job["current_interval_start"]

            elif job["last_completed_interval"] is not None:

                start = datetime.combine(
                    job["last_completed_interval"] + timedelta(days=1),
                    time.min,
                    tzinfo=timezone.utc
                )

            else:

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
                    "No se pudo actualizar la importación interrumpida"
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
                    "No se pudo actualizar la importación fallida"
                )

            raise



    async def automatic_historical_import(self):
        """Detecta e importa los periodos históricos pendientes."""

        logger.info("Iniciando comprobación de importaciones históricas")

        start_date = settings.HISTORICAL_IMPORT_START_DATE

        # El productor live se encarga del día actual.
        end_date = datetime.now(timezone.utc).date() - timedelta(days=1)

        try:
            self.validate_import_dates(
                start_date,
                end_date
            )

        except ValueError:

            logger.exception(
                "Fechas inválidas para la importación automática"
            )

            return

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

                job_id, task = await self.start_import(
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

                await task

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

        self.validate_import_dates(
            start_date,
            end_date
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

        return job_id, task


    async def get_import_status(self):
        """Obtiene el estado de la última importación."""

        return await self.repository.get_latest_job()
    
    async def get_all_import_jobs(self):
        """Obtiene todas las importaciones históricas."""

        return await self.repository.get_all_import_jobs()

    def validate_import_dates(self, start_date, end_date):
        """Valida las fechas de una importación histórica."""

        today = datetime.now(timezone.utc).date()

        if start_date > end_date:
            raise ValueError(
                "La fecha inicial no puede ser posterior a la fecha final"
            )

        if start_date > today:
            raise ValueError(
                "La fecha inicial no puede estar en el futuro"
            )

        if end_date > today:
            raise ValueError(
                "La fecha final no puede estar en el futuro"
            )

        if end_date == today:
            raise ValueError(
                "No se puede importar el día actual porque "
                "el productor live ya se encarga de él"
            )


    async def shutdown(self):
        """Cancela y espera todas las importaciones activas."""

        tasks = list(self.active_tasks.values())

        if not tasks:
            logger.info("No hay importaciones activas que cancelar")
            return

        logger.info(
            "Cancelando %s importaciones activas",
            len(tasks)
        )

        # Primero solicitar la cancelación de todas.
        for task in tasks:
            if not task.done():
                task.cancel()

        # Después esperar a que TODAS terminen.
        results = await asyncio.gather(
            *tasks,
            return_exceptions=True
        )

        # Registrar posibles excepciones.
        for result in results:
            if isinstance(result, BaseException) and not isinstance(
                result, asyncio.CancelledError
            ):
                logger.error(
                    "Una importación terminó con error durante el apagado: %s",
                    result
                )

        self.active_tasks.clear()

        logger.info("Todas las importaciones activas han finalizado")


        
    async def recover_interrupted_imports(self):
        """Recupera las importaciones históricas interrumpidas."""

        jobs = await self.repository.get_interrupted_jobs()

        logger.info(
            "Importaciones interrumpidas encontradas: %s",
            len(jobs)
        )

        for job in jobs:

            job_id = job["id"]

            start_date = job["start_date"]
            end_date = job["end_date"]

            last_completed = job["last_completed_interval"]

            if last_completed is None:
                resume_from = start_date

            else:
                resume_from = last_completed + timedelta(days=1)

            if resume_from > end_date:

                await self.repository.update_job(
                    job_id,
                    status="completed",
                    finished_at=datetime.now(timezone.utc),
                    current_interval_start=None,
                    current_interval_end=None
                )

                continue

            logger.info(
                "Recuperando importación %s desde %s",
                job_id,
            )

            task = asyncio.create_task(
                self.run_import(
                    job_id,
                    start_date,
                    end_date,
                    job["min_magnitude"],
                ),
                name=f"historical_import_recovery_{job_id}"
            )

            self.active_tasks[job_id] = task

            task.add_done_callback(
                lambda completed_task, current_id=job_id:
                    self.active_tasks.pop(current_id, None)
            )

 
    
    
    async def resume_recoverable_imports(self):
        """Recupera importaciones interrumpidas, fallidas o abandonadas."""

        jobs = await self.repository.get_recoverable_jobs()

        if not jobs:
            logger.info("No hay importaciones pendientes de recuperación")
            return

        logger.info(
            "Importaciones recuperables encontradas: %s",
            len(jobs)
        )

        for job in jobs:

            job_id = job["id"]

            if job_id in self.active_tasks:
                continue

            claimed = await self.repository.claim_recoverable_job(job_id)

            if not claimed:
                continue

            logger.info(
                "Recuperando importación: job=%s | estado anterior=%s",
                job_id,
                job["status"]
            )

            task = asyncio.create_task(
                self.run_import(
                    job_id,
                    job["start_date"],
                    job["end_date"],
                    job["min_magnitude"]
                ),
                name=f"historical_import_recovery_{job_id}"
            )

            self.active_tasks[job_id] = task

            def cleanup(completed_task, current_id=job_id):

                self.active_tasks.pop(current_id, None)

                if completed_task.cancelled():
                    return

                exception = completed_task.exception()

                if exception is not None:
                    logger.error(
                        "Error recuperando importación %s: %s",
                        current_id,
                        exception
                    )

            task.add_done_callback(cleanup)




import_service = ImportService(
    repository=import_repository,
    usgs_service=usgs_service,
    replay_client=replay_producer_client
)
