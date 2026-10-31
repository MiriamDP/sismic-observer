
import os
import json
import logging
import asyncio
import urllib.parse
import urllib.request
from datetime import date, datetime, time, timedelta, timezone
from fastapi.middleware.cors import CORSMiddleware

import psycopg
from psycopg.rows import dict_row
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field

app = FastAPI(title="Sismic Observer API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:4200",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = os.environ["DATABASE_URL"]

USGS_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
USGS_LIMIT = 20000

HISTORICAL_IMPORT_ENABLED = (
    os.getenv("HISTORICAL_IMPORT_ENABLED", "false").lower() == "true"
)

HISTORICAL_IMPORT_START_DATE = date.fromisoformat(
    os.getenv("HISTORICAL_IMPORT_START_DATE", "2000-01-01")
)

HISTORICAL_IMPORT_MIN_MAGNITUDE = os.getenv(
    "HISTORICAL_IMPORT_MIN_MAGNITUDE", ""
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("api")


# --------------------------------------------------
# MODELO DE IMPORTACION
# --------------------------------------------------

class ImportRequest(BaseModel):
    start_date: date
    end_date: date
    min_magnitude: float | None = Field(default=None, ge=-2, le=10)


# --------------------------------------------------
# INICIALIZACION DE BASE DE DATOS
# --------------------------------------------------

def init_database():
    with psycopg.connect(DATABASE_URL) as conn:

        # Misma estructura que utiliza el consumer
        conn.execute("""
            CREATE TABLE IF NOT EXISTS earthquakes (
                id TEXT PRIMARY KEY,
                magnitude DOUBLE PRECISION NOT NULL,
                place TEXT NOT NULL,
                event_time TIMESTAMPTZ NOT NULL,
                longitude DOUBLE PRECISION NOT NULL,
                latitude DOUBLE PRECISION NOT NULL,
                depth DOUBLE PRECISION,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        # Seguimiento de las importaciones
        conn.execute("""
            CREATE TABLE IF NOT EXISTS earthquake_import_jobs (
                id BIGSERIAL PRIMARY KEY,
                status TEXT NOT NULL,
                start_date DATE NOT NULL,
                end_date DATE NOT NULL,
                min_magnitude DOUBLE PRECISION,
                events_processed INTEGER DEFAULT 0,
                intervals_completed INTEGER DEFAULT 0,
                error TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                finished_at TIMESTAMPTZ
            )
        """)

        # Solo puede existir una importación activa
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS
            one_running_earthquake_import
            ON earthquake_import_jobs (status)
            WHERE status = 'running'
        """)

        # Las tareas que estaban ejecutándose al reiniciar
        # la API quedan marcadas como interrumpidas.
        conn.execute("""
            UPDATE earthquake_import_jobs
            SET status = 'interrupted',
                finished_at = NOW()
            WHERE status = 'running'
        """)

    logger.info("Base de datos inicializada")



@app.on_event("startup")
async def startup():

    init_database()

    if not HISTORICAL_IMPORT_ENABLED:
        logger.info("Importación automática desactivada")
        return

    logger.info("Comprobando histórico de terremotos...")

    asyncio.create_task(
        asyncio.to_thread(automatic_historical_import)
    )

# --------------------------------------------------
# HEALTHCHECK
# --------------------------------------------------

@app.get("/health")
def health():
    try:
        with psycopg.connect(
            DATABASE_URL,
            connect_timeout=2,
        ) as conn:
            conn.execute("SELECT 1")

        return {
            "status": "ok",
            "database": "connected"
        }

    except Exception:
        raise HTTPException(
            status_code=503,
            detail="Database unavailable",
        )


# --------------------------------------------------
# CONSULTA A USGS
# --------------------------------------------------

def fetch_usgs_events(start, end, min_magnitude=None):

    params = {
        "format": "geojson",
        "starttime": start.isoformat(),
        "endtime": end.isoformat(),
        "orderby": "time-asc",
        "limit": USGS_LIMIT,
    }

    if min_magnitude is not None:
        params["minmagnitude"] = min_magnitude

    url = USGS_URL + "?" + urllib.parse.urlencode(params)

    request = urllib.request.Request(
        url,
        headers={"User-Agent": "SismicObserver/1.0"}
    )

    with urllib.request.urlopen(request, timeout=60) as response:
        data = json.loads(response.read().decode("utf-8"))

    return data.get("features", [])


# --------------------------------------------------
# GUARDAR TERREMOTOS EN POSTGRESQL
# --------------------------------------------------

def save_events(features):

    rows = []

    for event in features:

        properties = event.get("properties") or {}
        geometry = event.get("geometry") or {}
        coordinates = geometry.get("coordinates")

        if not isinstance(coordinates, list) or len(coordinates) < 2:
            continue

        if (
            not event.get("id")
            or properties.get("mag") is None
            or not properties.get("place")
            or properties.get("time") is None
        ):
            continue

        rows.append((
            event["id"],
            properties["mag"],
            properties["place"],
            datetime.fromtimestamp(
                properties["time"] / 1000,
                tz=timezone.utc
            ),
            coordinates[0],
            coordinates[1],
            coordinates[2] if len(coordinates) > 2 else None
        ))

    if not rows:
        return 0

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:

            cur.executemany("""
                INSERT INTO earthquakes (
                    id,
                    magnitude,
                    place,
                    event_time,
                    longitude,
                    latitude,
                    depth
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, rows)

            return cur.rowcount


# --------------------------------------------------
# ACTUALIZAR ESTADO DE IMPORTACION
# --------------------------------------------------

def update_job(job_id, **values):

    allowed = {
        "status",
        "events_processed",
        "intervals_completed",
        "error",
        "finished_at"
    }

    values = {
        key: value
        for key, value in values.items()
        if key in allowed
    }

    if not values:
        return

    assignments = ", ".join(
        f"{key} = %s" for key in values
    )

    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute(
            f"""
            UPDATE earthquake_import_jobs
            SET {assignments}
            WHERE id = %s
            """,
            (*values.values(), job_id)
        )


# --------------------------------------------------
# IMPORTACION HISTORICA
# --------------------------------------------------

def import_interval(job_id, start, end, min_magnitude):

    features = fetch_usgs_events(
        start,
        end,
        min_magnitude
    )

    # Si alcanzamos el límite, dividimos el intervalo
    # para evitar perder resultados.
    if len(features) >= USGS_LIMIT:

        duration = end - start

        if duration <= timedelta(milliseconds=2):
            raise RuntimeError(
                "Intervalo demasiado pequeño para dividir "
                "sin superar el límite de USGS"
            )

        midpoint = start + duration / 2

        import_interval(
            job_id,
            start,
            midpoint,
            min_magnitude
        )

        import_interval(
            job_id,
            midpoint + timedelta(milliseconds=1),
            end,
            min_magnitude
        )

        return

    inserted = save_events(features)

    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute("""
            UPDATE earthquake_import_jobs
            SET events_processed = events_processed + %s,
                intervals_completed = intervals_completed + 1
            WHERE id = %s
        """, (inserted, job_id))

    logger.info(
        "Intervalo procesado: %s - %s | recibidos=%s | nuevos=%s",
        start,
        end,
        len(features),
        inserted
    )


def run_import(job_id, start_date, end_date, min_magnitude):

    try:

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

        # Dividimos inicialmente en periodos de un mes.
        # Los intervalos demasiado grandes se subdividen
        # automáticamente dentro de import_interval.
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

            import_interval(
                job_id,
                current,
                interval_end,
                min_magnitude
            )

            current = interval_end + timedelta(milliseconds=1)

        update_job(
            job_id,
            status="completed",
            finished_at=datetime.now(timezone.utc)
        )

        logger.info(
            "Importación histórica finalizada: job=%s",
            job_id
        )

    except Exception as exc:

        logger.exception(
            "Error en importación histórica: job=%s",
            job_id
        )

        update_job(
            job_id,
            status="failed",
            error=str(exc)[:2000],
            finished_at=datetime.now(timezone.utc)
        )


# --------------------------------------------------
# ENDPOINT: INICIAR IMPORTACION
# --------------------------------------------------

@app.post("/earthquakes/import-history", status_code=202)
def start_import(
    request: ImportRequest,
    background_tasks: BackgroundTasks
):

    if request.start_date > request.end_date:
        raise HTTPException(
            status_code=400,
            detail="La fecha inicial debe ser anterior a la final"
        )

    if request.end_date > date.today():
        raise HTTPException(
            status_code=400,
            detail="La fecha final no puede estar en el futuro"
        )

    try:

        with psycopg.connect(DATABASE_URL) as conn:

            job = conn.execute("""
                INSERT INTO earthquake_import_jobs (
                    status,
                    start_date,
                    end_date,
                    min_magnitude
                )
                VALUES ('running', %s, %s, %s)
                RETURNING id
            """, (
                request.start_date,
                request.end_date,
                request.min_magnitude
            )).fetchone()

            job_id = job[0]

    except psycopg.errors.UniqueViolation:

        raise HTTPException(
            status_code=409,
            detail="Ya existe una importación en ejecución"
        )

    background_tasks.add_task(
        run_import,
        job_id,
        request.start_date,
        request.end_date,
        request.min_magnitude
    )

    return {
        "message": "Importación histórica iniciada",
        "job_id": job_id,
        "status": "running"
    }


# --------------------------------------------------
# ENDPOINT: CONSULTAR ESTADO
# --------------------------------------------------

@app.get("/earthquakes/import-history/status")
def import_status():

    with psycopg.connect(
        DATABASE_URL,
        row_factory=dict_row
    ) as conn:

        job = conn.execute("""
            SELECT *
            FROM earthquake_import_jobs
            ORDER BY id DESC
            LIMIT 1
        """).fetchone()

    if not job:
        return {"message": "Todavía no hay importaciones"}

    return job


# --------------------------------------------------
# ENDPOINT: CONSULTAR TERREMOTOS
# --------------------------------------------------

@app.get("/earthquakes")
def get_earthquakes(
    limit: int = 100,
    offset: int = 0
):

    if not 1 <= limit <= 1000 or offset < 0:
        raise HTTPException(
            status_code=400,
            detail="Parámetros de paginación inválidos"
        )

    with psycopg.connect(
        DATABASE_URL,
        row_factory=dict_row
    ) as conn:

        rows = conn.execute("""
            SELECT *
            FROM earthquakes
            ORDER BY event_time DESC
            LIMIT %s OFFSET %s
        """, (limit, offset)).fetchall()

    return rows

def check_historical_data():

    with psycopg.connect(DATABASE_URL) as conn:

        result = conn.execute("""
            SELECT MIN(event_time), MAX(event_time), COUNT(*)
            FROM earthquakes
        """).fetchone()

    min_date, max_date, total = result

    if total == 0:
        logger.info("No existen terremotos en PostgreSQL")
        return False

    logger.info(
        "Histórico encontrado: %s terremotos, desde %s hasta %s",
        total,
        min_date,
        max_date
    )

    return min_date.date() <= HISTORICAL_IMPORT_START_DATE


def get_missing_ranges(start_date, end_date, min_magnitude=None):
    """
    Devuelve los intervalos que todavía no están cubiertos
    por importaciones históricas completadas.
    """

    with psycopg.connect(DATABASE_URL) as conn:

        completed_jobs = conn.execute("""
            SELECT start_date, end_date
            FROM earthquake_import_jobs
            WHERE status = 'completed'
              AND min_magnitude IS NOT DISTINCT FROM %s
              AND start_date <= %s
              AND end_date >= %s
            ORDER BY start_date
        """, (
            min_magnitude,
            end_date,
            start_date
        )).fetchall()

    # Unir intervalos ya importados
    merged = []

    for job_start, job_end in completed_jobs:

        if not merged or job_start > merged[-1][1] + timedelta(days=1):
            merged.append([job_start, job_end])

        else:
            merged[-1][1] = max(merged[-1][1], job_end)

    # Encontrar los huecos
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



def create_import_job(start_date, end_date, min_magnitude):

    with psycopg.connect(DATABASE_URL) as conn:

        result = conn.execute("""
            INSERT INTO earthquake_import_jobs (
                status,
                start_date,
                end_date,
                min_magnitude
            )
            VALUES ('running', %s, %s, %s)
            RETURNING id
        """, (
            start_date,
            end_date,
            min_magnitude
        )).fetchone()

    return result[0]


def automatic_historical_import():

    start_date = HISTORICAL_IMPORT_START_DATE

    # Dejamos el día actual fuera del histórico.
    # El productor ya se encarga de recoger terremotos recientes.
    end_date = date.today() - timedelta(days=1)

    min_magnitude = (
        float(HISTORICAL_IMPORT_MIN_MAGNITUDE)
        if HISTORICAL_IMPORT_MIN_MAGNITUDE.strip()
        else None
    )

    if start_date > end_date:
        logger.info("No hay periodo histórico que importar")
        return

    missing_ranges = get_missing_ranges(
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
            job_id = create_import_job(
                missing_start,
                missing_end,
                min_magnitude
            )

        except psycopg.errors.UniqueViolation:
            logger.warning(
                "Ya existe otra importación en ejecución"
            )
            return

        run_import(
            job_id,
            missing_start,
            missing_end,
            min_magnitude
        )

        with psycopg.connect(DATABASE_URL) as conn:
            status = conn.execute("""
                SELECT status
                FROM earthquake_import_jobs
                WHERE id = %s
            """, (job_id,)).fetchone()[0]

        if status != "completed":
            logger.error(
                "La importación %s no terminó correctamente. "
                "Se detienen las siguientes importaciones.",
                job_id
            )
            return

    logger.info("Importación histórica automática finalizada")
