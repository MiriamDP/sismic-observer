'''Modulo de conexion e inicializacion de la base de datos'''
import asyncio
import logging
import asyncpg

from core.config import settings


logger = logging.getLogger("api")


class Database:
    """Gestiona las conexiones e inicialización de PostgreSQL."""

    def __init__(self):

        self.database_url = settings.DATABASE_URL
        self.pool = None
        self._connect_lock = asyncio.Lock()

        if not self.database_url:
            raise RuntimeError("DATABASE_URL no está configurada")


    async def connect(self):
        """Crea el pool de conexiones."""

        async with self._connect_lock:

            if self.pool is not None:
                return

            self.pool = await asyncpg.create_pool(
                dsn=self.database_url,
                min_size=2,
                max_size=10,
                command_timeout=30
            )

            logger.info("Pool PostgreSQL iniciado")


    async def close(self):
        """Cierra el pool de conexiones."""

        async with self._connect_lock:

            if self.pool:

                await self.pool.close()
                self.pool = None

                logger.info("Pool PostgreSQL cerrado")


    async def init_database(self):
        """Inicializa las tablas e índices necesarios."""

        if self.pool is None:
            raise RuntimeError(
                "No se puede inicializar la base de datos "
                "sin establecer primero la conexión"
            )

        async with self.pool.acquire() as connection:

            async with connection.transaction():

                await connection.execute("""
                    CREATE TABLE IF NOT EXISTS earthquake_import_jobs (

                        id BIGSERIAL PRIMARY KEY,

                        status VARCHAR(30) NOT NULL,

                        start_date DATE NOT NULL,
                        end_date DATE NOT NULL,

                        min_magnitude DOUBLE PRECISION,

                        events_published BIGINT NOT NULL DEFAULT 0,

                        intervals_completed INTEGER NOT NULL DEFAULT 0,

                        current_interval_start TIMESTAMPTZ,
                        current_interval_end TIMESTAMPTZ,

                        last_completed_interval DATE,

                        error TEXT,

                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        started_at TIMESTAMPTZ,
                        finished_at TIMESTAMPTZ,

                        CONSTRAINT valid_import_status CHECK (
                            status IN (
                                'pending',
                                'running',
                                'completed',
                                'failed',
                                'interrupted'
                            )
                        ),

                        CONSTRAINT valid_import_dates CHECK (
                            start_date <= end_date
                        )
                    );
                """)

                await connection.execute("""
                    UPDATE earthquake_import_jobs
                    SET status = 'interrupted',
                        finished_at = NOW()
                    WHERE status = 'running'
                """)

                await connection.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS
                    one_running_earthquake_import
                    ON earthquake_import_jobs (status)
                    WHERE status = 'running'
                """)

                await connection.execute("""
                    CREATE INDEX IF NOT EXISTS
                    idx_import_jobs_created
                    ON earthquake_import_jobs (created_at DESC)
                """)

        logger.info("Base de datos inicializada")


database = Database()
