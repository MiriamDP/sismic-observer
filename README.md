# Sismic Observer

Pipeline local para recopilar terremotos de USGS, conservar los datos originales y sus revisiones, enriquecerlos con un catalogo geografico y ofrecer consultas y metricas mediante una API.

Este README describe el backend y su puesta en marcha. No incluye instrucciones ni configuracion propia del panel Angular. Los requisitos completos estan en [REQUERIMIENTOS.md](./REQUERIMIENTOS.md).

## Arquitectura

```text
USGS (feed de ultimas 24 h) ──> productor Python ──> Kafka: earthquakes-live
                                                              │
                                                              v
                                                     consumidor Python
                                                              │
                                                              v
                                                          PostgreSQL
                                                              ^
                                                              │
USGS (catalogo historico) ──> API FastAPI ──> Kafka: earthquakes-replay
                                  │
                                  └── consultas y metricas
```

Los servicios se ejecutan localmente con Docker Compose:

- **Productor**: consulta el feed GeoJSON de USGS cada 60 segundos, guarda la respuesta y la consulta, detecta eventos nuevos/revisados y publica los cambios en Kafka.
- **Kafka**: separa los topics `earthquakes-live` y `earthquakes-replay`. La configuracion conserva los mensajes hasta siete dias.
- **Consumidores**: validan y transforman mensajes, asignan pais por punto-en-poligono y guardan eventos, versiones y rechazos en PostgreSQL. El modo `replay` se mantiene separado de `live`.
- **PostgreSQL**: persiste datos originales, estado del productor, ejecuciones de ingesta, catalogo de paises, estado actual de eventos, revisiones y cuarentena.
- **API FastAPI**: consulta los datos procesados y puede importar el historico USGS y publicarlo en Kafka para su procesamiento.
- **JupyterLab (`notebook`)**: ejecuta los notebooks en un contenedor Python conectado a PostgreSQL. La carpeta `notebooks/` se monta en el contenedor para que los cambios en los ficheros queden en el equipo anfitrion.
- **pgAdmin**: servicio auxiliar incluido en Compose para inspeccionar PostgreSQL.

## Requisitos previos

- Docker Desktop con Docker Compose v2.
- Acceso a Internet desde los contenedores para consultar USGS.
- El catalogo Natural Earth `paises.geojson`. El Compose actual lo monta en modo lectura desde `../sismic-panel/public/datos/paises.geojson` (relativo a la raiz de este repositorio), dentro de los consumidores como `/app/data/paises.geojson`. Comprueba que el fichero exista antes de arrancar; no es necesario instalar ni compilar el panel para ejecutar el backend.

## Puesta en marcha

1. Desde la raiz del repositorio, crea la configuracion local a partir de la plantilla:

   ```powershell
   Copy-Item .env.example .env
   ```

2. Edita `.env` y sustituye los valores de ejemplo por valores validos para PostgreSQL y pgAdmin. No compartas ni subas `.env` al repositorio.

   Variables disponibles:

   | Variable | Uso |
   | --- | --- |
   | `POSTGRES_DB` | Nombre de la base de datos |
   | `POSTGRES_USER` / `POSTGRES_PASSWORD` | Credenciales de PostgreSQL |
   | `PGADMIN_EMAIL` / `PGADMIN_PASSWORD` | Acceso local a pgAdmin |

3. Verifica que existe el catalogo geografico en la ruta indicada arriba.

4. Construye y levanta los servicios:

   ```powershell
   docker compose up --build -d
   ```

   La primera inicializacion de PostgreSQL ejecuta `database/init.sql`, que crea las tablas e indices del productor y consumidor. La base solo ejecuta automaticamente esos scripts cuando el volumen de datos se inicializa por primera vez; editar el SQL no vuelve a aplicarlo sobre un volumen ya creado.

5. Comprueba el estado y los registros:

   ```powershell
   docker compose ps
   docker compose logs -f producer consumer-live api notebook
   ```

   La API queda disponible en `http://127.0.0.1:8000`; su comprobacion de salud es `http://127.0.0.1:8000/health` y la documentacion interactiva de FastAPI, `http://127.0.0.1:8000/docs`. JupyterLab queda disponible en `http://127.0.0.1:8888/lab?token=sismic-local`. pgAdmin se publica localmente en `http://127.0.0.1:5050`. El productor tambien publica su endpoint de salud en el puerto `8082`.

### Ejecutar los notebooks

El servicio `notebook` se inicia junto con el resto de Compose. Para levantar solo JupyterLab y su dependencia PostgreSQL:

```powershell
docker compose up --build -d notebook
```

Abre `http://127.0.0.1:8888/lab?token=sismic-local` en el navegador y selecciona `01_analisis_de_calidad.ipynb` desde la carpeta `notebooks`. El directorio local `notebooks/` esta montado como `/workspace`, por lo que guardar el notebook conserva los cambios en el repositorio. El contenedor recibe `DATABASE_URL` y se conecta a PostgreSQL mediante el nombre de servicio `postgres`; asegúrate de que la base ya tenga datos para ejecutar consultas de analisis.

La celda de configuracion del notebook busca tambien un fichero `.env` dentro de `/workspace`. Para que esa celda encuentre la configuracion local, copia el `.env` de la raiz al directorio montado:

```powershell
Copy-Item .env notebooks\.env
```

`.env` esta excluido de Git; no compartas ni subas esa copia, ya que contiene credenciales.

El token `sismic-local` esta configurado en la imagen del notebook para uso local. El puerto se limita a `127.0.0.1`; no publiques el servicio ni compartas el token en una red accesible por terceros, ya que el notebook tiene acceso a la base de datos.

### Parada

Para parar los contenedores sin borrar los datos persistidos:

```powershell
docker compose down
```

PostgreSQL y Kafka usan volumenes Docker. **No uses `docker compose down -v` salvo que quieras eliminar expresamente todos los datos persistidos.**

### Carga historica

Compose activa la importacion historica automatica de la API desde `2025-01-01` hasta el dia anterior a la fecha actual. En la configuracion actual, `HISTORICAL_IMPORT_MIN_MAGNITUDE` esta vacia, por lo que la importacion no limita la magnitud. Para reproducir la propuesta de los requisitos (2025, mundial, magnitud >= 4), cambia en `docker-compose.yaml`:

```yaml
HISTORICAL_IMPORT_START_DATE: "2025-01-01"
HISTORICAL_IMPORT_MIN_MAGNITUDE: "4"
```

La API divide la descarga en intervalos mensuales y subdivide los que alcanzan el limite de USGS; conserva el progreso para recuperarse de interrupciones. Puede tardar y generar bastante trafico/datos. Consulta su estado con:

```text
GET http://127.0.0.1:8000/api/earthquakes/import-history/status
GET http://127.0.0.1:8000/api/earthquakes/import-history/jobs
```

Tambien se puede solicitar un periodo manual mediante `POST /api/earthquakes/import-history`, con un cuerpo como:

```json
{
  "start_date": "2025-01-01",
  "end_date": "2025-12-31",
  "min_magnitude": 4
}
```

El historico se procesa por el topic de replay y se guarda con `mode = replay`; no debe confundirse con los eventos live. La importacion historica es una instantanea del catalogo consultado, no un registro de todas las revisiones historicas de USGS.

## API disponible

Base local: `http://127.0.0.1:8000`. Los endpoints principales implementados son:

| Metodo y ruta | Funcion |
| --- | --- |
| `GET /health` | Comprueba que la API puede consultar PostgreSQL |
| `GET /api/earthquakes` | Lista paginada; admite modo (`live`, `replay`, `all`), fechas, magnitud, profundidad y pais |
| `GET /api/earthquakes/latest` | Devuelve los eventos mas recientes |
| `GET /api/earthquakes/{id}` | Consulta un evento por identificador |
| `GET /api/earthquakes/metrics` | Devuelve resumen, distribucion temporal, paises y datos de magnitud/profundidad para un periodo |
| `GET /api/earthquakes/metrics/comparison` | Compara metricas de periodos equivalentes en dos anos |
| `GET /api/countries` | Lista el catalogo de paises |
| `POST /api/earthquakes/import-history` | Inicia una importacion historica |
| `GET /api/earthquakes/import-history/status` | Consulta el ultimo trabajo de importacion |
| `GET /api/earthquakes/import-history/jobs` | Lista los trabajos historicos |

FastAPI publica el esquema OpenAPI y la interfaz Swagger en `/docs`.

## Extraccion, transformacion y modelo de datos

### Fuentes y unidades

- **Feed reciente**: `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson`, consultado cada 60 segundos. Es una ventana movil de 24 horas, no el dia natural anterior. Una respuesta contiene `features` GeoJSON.
- **Campos usados**: `id`; `properties.time`, `updated`, `mag`, `magType`, `type`, `place`; y `geometry.coordinates`.
- **Coordenadas**: longitud, latitud y profundidad en kilometros. `time` y `updated` llegan en milisegundos desde Unix epoch y se transforman a UTC.
- **Filtro de tipo**: el productor y el consumidor excluyen los elementos cuyo `properties.type` no sea `earthquake`; se conserva el evento excluido y el motivo.
- **Historico**: API FDSN de eventos USGS, publicada en `earthquakes-replay`. Su periodo y umbral dependen de la configuracion del trabajo de importacion.
- **Geografia**: Natural Earth en GeoJSON; se comprueba la unicidad de `ADM0_A3`, se crea un indice espacial STRtree y la asignacion se hace mediante relacion espacial. Para puntos de frontera se intenta primero interior estricto (`within`) y despues cobertura de frontera (`covered_by`); en varias coincidencias se ordenan los codigos y se elige el primero. Sin coincidencia queda `country_code` nulo (sin pais asignado).

El cliente HTTP del productor reintenta errores de transporte, timeout, JSON y respuestas HTTP 429/5xx con espera exponencial acotada. Las otras respuestas 4xx se propagan como error. Los originales repetidos se deduplican por SHA-256; cada intento queda registrado en `ingestion_runs`.

### Tablas principales

El esquema esta en [database/init.sql](./database/init.sql):

| Tabla | Grano y finalidad |
| --- | --- |
| `raw_extractions` | Una respuesta GeoJSON por huella SHA-256, incluyendo cuerpo y tamano |
| `ingestion_runs` | Una fila por consulta realizada: URL, tiempos, estado HTTP, registros o error |
| `producer_event_state` | Ultima version publicada por `(event_id, mode)` |
| `producer_event_exclusions` | Eventos live excluidos y motivo |
| `countries` | Catalogo por `ADM0_A3` y atributos de Natural Earth |
| `earthquakes` | Estado actual por `(event_id, mode)` |
| `earthquake_versions` | Version original por `(event_id, mode, updated_ms)` |
| `consumer_quarantine` | Mensajes rechazados, motivo y original |
| `earthquake_import_jobs` | Estado/progreso de las importaciones historicas, creado por la API |

Hay indices sobre tiempo, pais, magnitud, revisiones, cuarentena y estado de ingesta. El consumidor inserta version y estado actual en una transaccion; las claves unicas hacen idempotentes los reenvios. Solo actualiza el estado actual si la version entrante tiene un `updated_ms` posterior. El offset Kafka se confirma despues de guardar el resultado, y un mensaje repetido no incrementa el numero de eventos/versiones ya persistidos.

## Evidencias de los requisitos

Esta seccion distingue la evidencia comprobable en el codigo/configuracion actual de los entregables que necesitan resultados experimentales o documentacion adicional. No se presentan como realizadas pruebas ni analisis que no aparecen en los ficheros revisados.

| Area pedida | Evidencia presente en el repositorio | Pendiente para demostrar completamente el requisito |
| --- | --- | --- |
| Arquitectura local | `docker-compose.yaml` define Kafka, PostgreSQL, productor, consumidores y API; los servicios Python tienen Dockerfiles. | El Compose incluye tambien pgAdmin. Verificar la ejecucion conjunta en el equipo de demostracion. |
| Extraccion live | `producer/usgs_client.py` consulta el feed, valida JSON y aplica reintentos; `producer/extraction.py` registra respuestas/consultas y diferencia novedades, revisiones y exclusiones. | Añadir resultados reales de latencia, tipos/completitud y registros de extraccion si se solicita evidencia cuantitativa. |
| Integracion y calidad | `consumer/app/validator.py` valida estructura, geometria, coordenadas, fechas, magnitud, profundidad y modo; `consumer/app/geography.py` integra Natural Earth; `consumer/app/quality.py` identifica faltantes y anomalías. `notebooks/01_analisis_de_calidad.ipynb` contiene el planteamiento de las seis dimensiones y la preparacion del entorno. | `quality.py` no se invoca desde el bucle principal del consumidor. El notebook todavia no contiene calculos/resultados; falta documentar porcentajes antes/despues, resultados por dimension y decisiones. |
| Versionado/idempotencia | `consumer/app/database.py` persiste revisiones y actualiza el estado solo con versiones mas recientes; el commit se realiza despues de persistir. | Adjuntar evidencia de pruebas de duplicado, version posterior, version antigua y reinicio del consumidor. |
| API y analisis | Endpoints de listado, filtros, agregaciones y comparacion en `api/routers/earthquakes.py`; importacion historica en `api/services/import_service.py`. | La comparacion del endpoint admite periodos equivalentes por parametros, pero falta documentar resultados numericos y conclusiones fundamentadas. |
| Complejidad, rendimiento y costes | Existen operaciones con indices SQL, upserts y STRtree espacial. | Faltan mediciones de tiempo/memoria con al menos tres tamaños, comparaciones de indice/busqueda, estimacion de costes, escalado y reflexion documentada. |
| Pruebas de aceptacion y replay | Topic, modo y tablas separan replay de live; la importacion se hace por Kafka y el consumidor reutiliza el mismo procesamiento. | Faltan evidencias registradas/capturas de ejecucion para cada caso de aceptacion y una demostracion identificada de replay. |

Por tanto, el codigo implementa partes importantes del pipeline, pero el README no sustituye la memoria/notebook requerida para el analisis de calidad, resultados, conclusiones, mediciones, costes y evidencias de pruebas.

## Configuracion y limites operativos

- Los contenedores se conectan por los nombres de servicio Compose (`kafka`, `postgres`). El broker tambien anuncia un listener de host en `localhost:9092`; internamente utiliza `kafka:19092`.
- Solo la API y pgAdmin se publican en loopback del host (`127.0.0.1`). No se publican PostgreSQL ni Kafka.
- El feed de 24 horas no permite inferir que USGS elimino un evento cuando deja de aparecer: puede haber salido de la ventana. Para recuperar un hueco tras una parada superior a 24 horas, solicita el periodo al endpoint historico y procesalo como replay.
- No hay transaccion distribuida Kafka/PostgreSQL. El procesamiento es al menos una vez y depende de la idempotencia en base de datos.
- El almacenamiento de Kafka tiene retencion de siete dias; los volumenes de PostgreSQL/Kafka persisten hasta eliminarlos manualmente. El SQL montado en `/docker-entrypoint-initdb.d` solo inicializa una base nueva.
- El Compose actual habilita importacion historica automatica y fija su inicio en 2025-01-01. Ajusta el umbral si necesitas limitar volumen y mantener comparaciones con filtros equivalentes.
- El codigo CORS de la API permite actualmente el origen `http://localhost:4200`; CORS no constituye autenticacion. No publiques credenciales ni tokens, ni expongas directamente el broker o la base de datos.

## Mapa del repositorio

```text
api/                 FastAPI, importacion historica, consultas y metricas
consumer/app/        validacion, transformacion, geografia y persistencia
database/init.sql    esquema inicial del productor y consumidor
notebooks/           JupyterLab y notebooks para analisis de datos
producer/            extraccion USGS, estado de publicaciones y Kafka
docker-compose.yaml  servicios, red, healthchecks y volumenes
.env.example         plantilla de variables locales
REQUERIMIENTOS.md    enunciado y criterios de entrega
```
