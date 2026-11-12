
import asyncio
import logging

from contextlib import asynccontextmanager, AsyncExitStack

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from core.config import settings
from core.database import database

from services.usgs_service import usgs_service
from services.import_service import import_service
from services.event_listener import notification_listener

from kafka.kafka_replay import replay_publisher

from routers import (
    health,
    earthquakes,
    imports,
    internal
)


# --------------------------------------------------
# LOGGING
# --------------------------------------------------

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger("api")


# --------------------------------------------------
# LIFESPAN
# --------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):

    logger.info("Iniciando Sismic Observer API")

    async with AsyncExitStack() as stack:

        try:

            # ------------------------------------------
            # POSTGRESQL
            # ------------------------------------------

            await database.connect()
            # ------------------------------------------
            # POSTGRESQL LISTENER
            # ------------------------------------------

            listener_task = asyncio.create_task(
                notification_listener(),
                name="postgres_listener"
            )

            stack.push_async_callback(database.close)

            await database.init_database()

            # ------------------------------------------
            # USGS
            # ------------------------------------------

            await usgs_service.connect()

            stack.push_async_callback(usgs_service.close)

            # ------------------------------------------
            # KAFKA
            # ------------------------------------------

            await replay_publisher.start()

            stack.push_async_callback(replay_publisher.stop)
           # ------------------------------------------
            # RECUPERAR IMPORTS 
            # ------------------------------------------

            await import_service.resume_recoverable_imports()

            # ------------------------------------------
            # REGISTRAR RECURSOS
            # ------------------------------------------

            app.state.database = database
            app.state.replay_publisher = replay_publisher
            app.state.import_service = import_service

            # ------------------------------------------
            # IMPORTACIÓN HISTÓRICA AUTOMÁTICA
            # ------------------------------------------

            historical_task = None

            if settings.HISTORICAL_IMPORT_ENABLED:

                logger.info(
                    "Programando comprobación de importaciones históricas"
                )

                historical_task = asyncio.create_task(
                    import_service.automatic_historical_import(),
                    name="historical_import"
                )

                app.state.historical_import_task = historical_task

            logger.info(
                "Sismic Observer API iniciada correctamente"
            )

            try:

                yield

            
            finally:

                # --------------------------------------
                # CANCELAR IMPORTACIÓN AUTOMÁTICA
                # --------------------------------------

                if historical_task and not historical_task.done():

                    logger.info("Cancelando importación histórica automática")

                    historical_task.cancel()

                    try:
                        await historical_task

                    except asyncio.CancelledError:
                        logger.info("Importación automática cancelada")

                    except Exception:
                        logger.exception(
                            "Error durante la importación automática"
                        )

                # --------------------------------------
                # CANCELAR IMPORTACIONES ACTIVAS
                # --------------------------------------

                logger.info("Deteniendo importaciones activas")

                await import_service.shutdown()

                # --------------------------------------
                # DETENER POSTGRESQL LISTENER
                # --------------------------------------

                logger.info("Deteniendo PostgreSQL listener")

                listener_task.cancel()

                await asyncio.gather(
                    listener_task,
                    return_exceptions=True
                )

                logger.info("Sismic Observer API detenida")

        except Exception:

            logger.exception(
                "Error durante el ciclo de vida de la API"
            )

            raise


# --------------------------------------------------
# APLICACIÓN FASTAPI
# --------------------------------------------------

app = FastAPI(
    title="Sismic Observer API",
    lifespan=lifespan
)


# --------------------------------------------------
# CORS
# --------------------------------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:4200"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)


# --------------------------------------------------
# ROUTERS
# --------------------------------------------------

app.include_router(health.router)
app.include_router(earthquakes.router)
app.include_router(imports.router)
app.include_router(internal.router)
