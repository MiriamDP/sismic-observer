# Sismic Observer

Pipeline local para recopilar terremotos de USGS, conservar los datos originales y sus revisiones, enriquecerlos con un catalogo geografico y ofrecer consultas y metricas mediante una API.

Este README describe el backend y su puesta en marcha. No incluye instrucciones ni configuracion propia del panel Angular. Los requisitos completos estan en [REQUERIMIENTOS.md](./REQUERIMIENTOS.md).

## Arquitectura

```text
USGS (feed 24 h) ──> producer-live ──> Kafka: earthquakes-live ──> consumer-live ──┐
                                                                                  │
API ──> USGS (historico) ──HTTP──> producer-replay ──> Kafka: earthquakes-replay ──> consumer-replay ─┤
                                                                                  v
                                                                              PostgreSQL
                                                                                  ^
                                                                                  │
                                                              API (consultas y metricas)
```

Los servicios se ejecutan localmente con Docker Compose:

- **`producer-live`**: consulta cada 60 segundos el feed GeoJSON de las ultimas 24 horas, guarda la respuesta y la consulta, detecta eventos nuevos/revisados y publica los cambios en `earthquakes-live`.
- **`producer-replay`**: no hace sondeo periodico. Recibe de la API las extracciones historicas por HTTP (`POST /extract`), conserva su original y metadatos, y publica los eventos en `earthquakes-replay`.
- **Kafka**: separa los topics `earthquakes-live` y `earthquakes-replay`. La configuracion conserva los mensajes hasta siete dias.
- **Consumidores**: validan y transforman mensajes, asignan pais por punto-en-poligono y guardan eventos, versiones y rechazos en PostgreSQL. El modo `replay` se mantiene separado de `live`.
- **PostgreSQL**: persiste datos originales, estado del productor, ejecuciones de ingesta, catalogo de paises, estado actual de eventos, revisiones y cuarentena.
- **API FastAPI**: consulta los datos procesados y coordina las importaciones historicas. Descarga los datos de USGS y los entrega a `producer-replay` por HTTP; no publica directamente en Kafka.
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
   docker compose logs -f producer-live producer-replay consumer-live consumer-replay api notebook
   ```

   La API queda disponible en `http://127.0.0.1:8000`; su comprobacion de salud es `http://127.0.0.1:8000/health` y la documentacion interactiva de FastAPI, `http://127.0.0.1:8000/docs`. JupyterLab queda disponible en `http://127.0.0.1:8888/lab?token=sismic-local`. pgAdmin se publica localmente en `http://127.0.0.1:5050`. Los healthchecks de `producer-live` y `producer-replay` estan en los puertos `8082` y `8083`, respectivamente.

## Panel de usuario
El panel de usuario se encuentra alojado en el siguiente repositorio de GitHub
[Sismic Panel Repositorio](https://github.com/MiriamDP/sismic-panel)

El enlace a la web publicada en Netlify esta en
[Earth Watcher](https://earth-watcher-mdp.netlify.app/today)

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

La API consulta USGS, divide la descarga en intervalos mensuales y subdivide los que alcanzan el limite de USGS. En cada intervalo envia a `producer-replay` por HTTP la extraccion GeoJSON original y sus metadatos; ese productor realiza el filtrado, la deduplicacion, la persistencia de la extraccion y la publicacion confirmada en Kafka. La API conserva el progreso del trabajo para recuperarse de interrupciones, pero no escribe mensajes en Kafka. La API depende de que `producer-replay` este saludable. La carga puede tardar y generar bastante trafico/datos. Consulta el estado con:

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

El historico se procesa por el topic de replay y se guarda con `mode = replay`; no debe confundirse con los eventos live. El recorrido de publicacion es API → HTTP → `producer-replay` → Kafka → `consumer-replay` → PostgreSQL. La importacion historica es una instantanea del catalogo consultado, no un registro de todas las revisiones historicas de USGS.

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
- **Historico**: API FDSN de eventos USGS. La API de este proyecto obtiene cada intervalo y lo remite a `producer-replay` por HTTP; el productor publica los eventos en `earthquakes-replay`. El periodo y umbral dependen de la configuracion del trabajo de importacion.
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

Esta seccion resume la evidencia comprobable en el codigo/configuracion y en el notebook de calidad. Las cifras del notebook son una instantanea de la base de datos consultada durante su ejecucion, no una garantia de resultados invariantes en ejecuciones posteriores.

| Area pedida | Evidencia presente en el repositorio | Pendiente para demostrar completamente el requisito |
| --- | --- | --- |
| Arquitectura local | `docker-compose.yaml` define Kafka, PostgreSQL, `producer-live`, `producer-replay`, consumidores y API; ambos productores usan la imagen Python de `producer/`. | El Compose incluye tambien pgAdmin y JupyterLab. Verificar la ejecucion conjunta en el equipo de demostracion. |
| Extraccion live | `producer/usgs_client.py` consulta el feed, valida JSON y aplica reintentos; `producer/extraction.py` registra respuestas/consultas y diferencia novedades, revisiones y exclusiones. | Añadir resultados reales de latencia, tipos/completitud y registros de extraccion si se solicita evidencia cuantitativa. |
| Integracion y calidad | `consumer/app/validator.py` valida estructura, geometria, coordenadas, fechas, magnitud, profundidad y modo; `consumer/app/geography.py` integra Natural Earth; `consumer/app/quality.py` evalua los eventos transformados antes de persistirlos. El notebook [`01_analisis_de_calidad.ipynb`](./notebooks/01_analisis_de_calidad.ipynb) calcula completitud, exactitud del procesamiento, consistencia, validez, unicidad y actualidad. | El analisis del notebook es una instantanea previa a la integracion de `quality.py` y sus limitaciones se detallan a continuacion; sigue pendiente corregir y volver a medir la latencia live. |
| Versionado/idempotencia | `consumer/app/database.py` persiste revisiones y actualiza el estado solo con versiones mas recientes; el commit se realiza despues de persistir. | Adjuntar evidencia de pruebas de duplicado, version posterior, version antigua y reinicio del consumidor. |
| API y analisis | Endpoints de listado, filtros, agregaciones y comparacion en `api/routers/earthquakes.py`; importacion historica en `api/services/import_service.py`. | La comparacion del endpoint admite periodos equivalentes por parametros, pero falta documentar resultados numericos y conclusiones fundamentadas. |
| Complejidad, rendimiento y costes | Existen operaciones con indices SQL, upserts y STRtree espacial. | Faltan mediciones de tiempo/memoria con al menos tres tamaños, comparaciones de indice/busqueda, estimacion de costes, escalado y reflexion documentada. |
| Pruebas de aceptacion y replay | Topics, modos y tablas separan replay de live. La API entrega extracciones al productor replay por HTTP; este publica en Kafka y el consumidor replay reutiliza el mismo procesamiento que live. | Faltan evidencias registradas/capturas de ejecucion para cada caso de aceptacion y una demostracion identificada de replay. |

### Resultados y consideraciones del análisis de calidad

El notebook analiza datos persistidos en una instantanea de noviembre de 2026: 396.844 eventos actuales, 397.053 versiones y 177 paises. Los recuentos y porcentajes pueden cambiar cuando avanza la ingesta. El notebook contiene el detalle de consultas, cálculos y gráficos.

| Dimensión o comprobación | Resultado de la instantánea | Interpretación |
| --- | --- | --- |
| Completitud estructural del consumidor | 100% en los campos definidos como obligatorios. Magnitud y tipo de magnitud: 99,99% cada uno (21 valores nulos de 396.844). | La magnitud ausente se conserva como nula; no debe sustituirse por cero ni eliminarse automáticamente. |
| Fidelidad de la transformación | 100% para los 396.844 eventos comparables entre el estado actual y el mensaje original de su versión, en los campos comprobados. | Demuestra correspondencia de esos campos con el mensaje persistido, no la exactitud de la fuente USGS frente a la realidad. |
| Validez y unicidad | 100% según las reglas probadas; 396.844 claves actuales únicas por `(event_id, mode)`. | Es un resultado sobre las filas guardadas y las reglas del notebook, no una prueba de todos los casos límite posibles. |
| Integración geográfica | 298.921 eventos con país y 97.923 sin país (24,68%). Los códigos asignados pertenecen al catálogo. | «Sin país asignado» se mantiene como categoría válida, especialmente para eventos marinos; no se atribuye al país más cercano. |
| Consistencia temporal | `updated >= event_time`: 100%. `processed_at >= ingested_at`: falla en 100 filas; 99 corresponden a `live` (99 de 575, 17,22%). La consistencia de `live` en el resumen del notebook es 82,78%, frente a 100% en `replay`. | La media global queda dominada por el gran volumen de `replay` y oculta el problema de `live`. |
| Actualidad del consumidor | El notebook define como dentro del umbral una latencia entre 0 y 60 segundos: 99,95% global, 66,43% `live` y 100% `replay`. | Estos porcentajes no deben interpretarse como una medición fiable de latencia real hasta resolver los tiempos negativos de `live`. |
| Cuarentena del consumidor | 0 filas en la tabla consultada. | Indica que no había rechazos persistidos en esa instantánea; no demuestra el comportamiento ante mensajes inválidos, que requiere pruebas controladas. |

La discrepancia temporal es coherente con la implementación actual: al actualizar un evento existente, el upsert de [`consumer/app/database.py`](./consumer/app/database.py) reemplaza `ingested_at`, pero no `processed_at`, que solo se asigna al insertar la fila. Una revisión puede, por tanto, dejar en el estado actual una fecha de procesamiento antigua y producir una latencia negativa. Hay que definir y actualizar el significado de `processed_at`, separar las métricas `live` y `replay` y volver a ejecutar el análisis antes de presentar la latencia como rendimiento del consumidor.

En el productor, el notebook comprueba que los SHA-256 de las respuestas almacenadas coinciden con los recalculados (100%). En cambio, su comparación del hash de `producer_event_state` con eventos extraídos da 88,83%, pero une por `event_id` sin alinear explícitamente modo y `updated_ms`; puede estar comparando revisiones distintas. Este porcentaje no prueba pérdida ni corrupción y no debe presentarse como exactitud del productor hasta rehacer la comparación sobre la misma versión y una serialización canónica.

El análisis también contabiliza 9.367 exclusiones del productor (1,97% del conjunto utilizado para esa tasa), principalmente eventos cuyo tipo no es `earthquake`. Estas exclusiones son parte del filtro previsto, no necesariamente errores de ingesta. Para la comparación de calidad, conviene separar rechazos por calidad de exclusiones de negocio.

El notebook satisface el análisis de varias dimensiones de calidad y conserva sus resultados, pero no sustituye las evidencias aún pendientes de las demás partes del proyecto: conclusiones del análisis sísmico con filtros y periodos equivalentes, pruebas de aceptación/replay, mediciones de rendimiento con varios tamaños, estimación de costes y reflexión de escalado.

## Configuracion y limites operativos

- Los contenedores se conectan por los nombres de servicio Compose (`kafka`, `postgres`). El broker tambien anuncia un listener de host en `localhost:9092`; internamente utiliza `kafka:19092`.
- La API y pgAdmin se publican en loopback del host (`127.0.0.1`). Compose tambien publica los puertos de `producer-live` (`8082`) y `producer-replay` (`8083`) para sus healthchecks; no expongas esos endpoints a redes no confiables. PostgreSQL y Kafka no tienen puertos publicados por Compose.
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
producer/            productores Python live y replay, estado y publicacion Kafka
docker-compose.yaml  servicios, red, healthchecks y volumenes
.env.example         plantilla de variables locales
REQUERIMIENTOS.md    enunciado y criterios de entrega
```
