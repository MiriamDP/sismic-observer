# API de Sismic Observer

La API FastAPI sirve consultas y métricas sobre los terremotos ya persistidos en PostgreSQL y coordina las importaciones históricas desde USGS. Para importar no publica directamente en Kafka: obtiene cada extracción de USGS y la envía por HTTP a `producer-replay`, que la publica en el topic de replay para que la procese `consumer-replay`.

La configuración del servicio está en [`../docker-compose.yaml`](../docker-compose.yaml), el esquema inicial de productor y consumidor en [`../database/init.sql`](../database/init.sql), y la tabla de trabajos históricos se crea al iniciar la API.

## Posición en el sistema

```text
Consultas:   panel / cliente ──HTTP──► API ──SQL──► PostgreSQL

Histórico:   API ──HTTP──► USGS FDSN
               └──HTTP──► producer-replay ──► Kafka: earthquakes-replay
                                                   └──► consumer-replay ──► PostgreSQL

Actualizaciones live: consumer-live ──► PostgreSQL NOTIFY ──► API ──WebSocket──► clientes
```

La API consulta el estado actual almacenado; no consulta USGS para responder las rutas de listado o métricas. La importación histórica sí consulta USGS. El flujo live es responsabilidad independiente de `producer-live` y `consumer-live`.

## Inicio del servicio

Durante el inicio, la aplicación:

1. Abre un pool asíncrono de conexiones con PostgreSQL.
2. Inicializa `earthquake_import_jobs` y su índice parcial para permitir una sola importación en ejecución.
3. Inicia un listener PostgreSQL para el canal `new_earthquake`, que difunde las notificaciones live por WebSocket.
4. Prepara los clientes HTTP para USGS y `producer-replay`.
5. Reanuda trabajos de importación recuperables que quedaron en estado `running`, `interrupted` o `failed`.
6. Si `HISTORICAL_IMPORT_ENABLED=true`, comprueba e inicia la importación automática de periodos históricos pendientes.

La API depende de PostgreSQL para iniciar. El endpoint `/health` verifica la conexión a PostgreSQL; no comprueba que USGS, `producer-replay` o Kafka estén disponibles.

## Endpoints HTTP y WebSocket

La documentación OpenAPI interactiva está en `http://127.0.0.1:8000/docs` y el esquema OpenAPI en `/openapi.json`.

### Terremotos

| Método y ruta | Función |
| --- | --- |
| `GET /api/earthquakes` | Lista paginada; filtra por modo, fechas, magnitud, profundidad y país. |
| `GET /api/earthquakes/latest` | Devuelve los eventos más recientes; `limit` va de 1 a 100 y tiene valor predeterminado 3. |
| `GET /api/earthquakes/{earthquake_id}` | Busca un evento por identificador. |
| `GET /api/earthquakes/metrics` | Devuelve resumen, máximo, top 3 países, distribución temporal y hasta 5.000 pares magnitud-profundidad para un periodo. |
| `GET /api/earthquakes/metrics/comparison` | Calcula las mismas métricas para el mismo intervalo de calendario en dos años. |
| `GET /api/countries` | Devuelve los países del catálogo Natural Earth. |
| `GET /health` | Comprueba que la API puede ejecutar una consulta a PostgreSQL. |
| `WS /ws/earthquakes` | Envía notificaciones de eventos live recibidas por PostgreSQL. |

#### Filtros de listado

`GET /api/earthquakes` admite:

- `page` (predeterminado `1`) y `page_size` (predeterminado `20`, máximo `100`).
- `mode`: `live`, `replay` o `all` (predeterminado `all`).
- `start_date` y `end_date`: fechas ISO-8601 con hora opcional; el inicio es inclusivo y el final es exclusivo.
- `min_magnitude`, `max_magnitude`, `min_depth`, `max_depth`.
- `country_code`; usa `unassigned` para solicitar los eventos cuyo país es nulo.

El orden es por tiempo de ocurrencia descendente. La respuesta incluye `items`, metadatos de `pagination` y los filtros aplicados. Cada evento puede incluir `country_name`, `continent` y `subregion`; si no hay país asignado, el nombre se presenta como «Sin país asignado».

Cuando `mode=all`, la API devuelve un solo registro por `event_id`, priorizando `live` sobre `replay`; dentro del modo elegido utiliza la versión con `updated` más reciente. Al filtrar por `live` o `replay`, conserva la separación entre ambos modos.

Ejemplo:

```text
GET /api/earthquakes?page=1&page_size=20&mode=live&start_date=2026-11-15T00:00:00Z&end_date=2026-11-16T00:00:00Z&min_magnitude=4&country_code=unassigned
```

#### Métricas

`GET /api/earthquakes/metrics` requiere `start_date` y `end_date` como fechas `YYYY-MM-DD`; ambos extremos de fecha son inclusivos y los límites se construyen desde la medianoche UTC. Admite `mode`, límites de magnitud/profundidad y `country_code`, incluido `unassigned`.

La granularidad temporal se elige según la duración del periodo: horaria para un día, diaria hasta 90 días y mensual para periodos mayores. La distribución incluye intervalos vacíos con recuento cero. El resumen contiene recuentos y promedios, el evento de magnitud máxima, el top 3 de países y una muestra de hasta 5.000 pares magnitud-profundidad; los registros sin magnitud o profundidad no aparecen en esa dispersión.

La ruta de comparación requiere `start_day`, `start_month`, `end_day`, `end_month`, `first_year` y `second_year`. Aplica el mismo tramo de calendario y los mismos filtros geográficos, de magnitud y profundidad a ambos años, siempre sobre `mode=all`. Devuelve `first_period` y `second_period`; no calcula por sí sola una conclusión estadística ni una media histórica diaria.

Ejemplo:

```text
GET /api/earthquakes/metrics/comparison?start_day=1&start_month=1&end_day=31&end_month=1&first_year=2025&second_year=2026&min_magnitude=4
```

### Importaciones históricas

| Método y ruta | Función |
| --- | --- |
| `POST /api/earthquakes/import-history` | Inicia una importación y responde HTTP 202 con `job_id`. |
| `GET /api/earthquakes/import-history/status` | Consulta el trabajo activo o el más reciente. |
| `GET /api/earthquakes/import-history/jobs` | Lista los trabajos históricos registrados. |

Ejemplo de cuerpo para iniciar una importación:

```json
{
  "start_date": "2025-01-01",
  "end_date": "2025-12-31",
  "min_magnitude": 4
}
```

`min_magnitude` es opcional; el modelo acepta valores entre -2 y 10. Las fechas deben estar ordenadas, no pueden ser futuras y el día actual no se puede importar porque corresponde al flujo live. El endpoint responde `400` ante fechas o filtros inválidos y `409` si ya hay otro trabajo en ejecución.

El trabajo continúa en segundo plano después de responder. La API consulta USGS en intervalos mensuales; si una respuesta alcanza el límite de 20.000 eventos, subdivide el intervalo en partes menores. Cada respuesta GeoJSON y sus metadatos se envían a `producer-replay` por HTTP. El productor conserva la extracción, filtra los tipos que no sean `earthquake` y publica los mensajes confirmados en `earthquakes-replay`. El consumidor replay persiste los eventos con `mode = replay`.

El progreso se guarda en `earthquake_import_jobs`: fechas solicitadas, filtro de magnitud, estado, intervalos completados, eventos publicados, intervalo actual y error. Los estados de trabajo observables son `running`, `completed`, `failed` e `interrupted`. Al reiniciarse, la API intenta reanudar trabajos recuperables desde el intervalo guardado. Solo puede ejecutarse un trabajo a la vez.

### WebSocket de eventos live

`WS /ws/earthquakes` mantiene la conexión abierta y recibe mensajes cuando el consumidor live emite `NOTIFY new_earthquake`. Este es un ejemplo esquemático del formato difundido (el objeto del evento incluye más campos):

```json
{
  "type": "earthquakes_update",
  "items": [
    {
      "event_id": "identificador-usgs"
    }
  ]
}
```

Las notificaciones son eventos live, no un mecanismo de consulta histórica ni un registro durable para clientes desconectados. El listener intenta reconectarse a PostgreSQL si pierde la conexión.

## Configuración

En Compose, el servicio recibe estas variables:

| Variable | Uso | Valor actual o predeterminado |
| --- | --- | --- |
| `DATABASE_URL` | DSN de PostgreSQL; obligatoria. | Compose la construye con las variables de PostgreSQL de `.env`. |
| `HISTORICAL_IMPORT_ENABLED` | Activa la comprobación/importación automática al arrancar. | En el código `false`; Compose establece `true`. |
| `HISTORICAL_IMPORT_START_DATE` | Inicio del periodo histórico automático. | `2025-01-01`. |
| `HISTORICAL_IMPORT_MIN_MAGNITUDE` | Umbral opcional de magnitud para la importación automática. | Vacío en Compose, por tanto no limita por magnitud. |
| `REPLAY_PRODUCER_URL` | URL base HTTP de `producer-replay`. | Compose establece `http://producer-replay:8083`. |

`USGS_URL` (endpoint FDSN) y el límite de 20.000 resultados están fijados en el código, no se leen del entorno. El cliente USGS realiza hasta cuatro intentos ante errores de transporte y respuestas HTTP 429, 500, 502, 503 o 504, con espera progresiva y respeto limitado de `Retry-After`. El valor alternativo de `REPLAY_PRODUCER_URL` definido en `core/config.py` no incluye esquema; fuera de Compose, configura una URL completa con `http://` o `https://`.

El modo automático de Compose puede importar desde 2025-01-01 hasta el día UTC anterior a la fecha actual, sin umbral de magnitud. Es una operación potencialmente larga y con volumen considerable. Para limitarla, cambia el umbral o desactiva `HISTORICAL_IMPORT_ENABLED` en `docker-compose.yaml`.

## Puesta en marcha y diagnóstico

Desde la raíz del repositorio, prepara `.env` siguiendo el [README principal](../README.md) y ejecuta:

```powershell
docker compose up --build -d api
```

Compose inicia también las dependencias declaradas: PostgreSQL, Kafka y `producer-replay`. Para revisar estado y logs:

```powershell
docker compose ps api
docker compose logs -f api
```

La API escucha en el puerto `8000` del contenedor y Compose lo publica solo en `127.0.0.1:8000`. URLs locales:

- API: `http://127.0.0.1:8000`
- Salud: `http://127.0.0.1:8000/health`
- Swagger: `http://127.0.0.1:8000/docs`

La tabla de trabajos `earthquake_import_jobs` se crea desde el arranque de la API. Las otras tablas se inicializan con `database/init.sql` cuando PostgreSQL crea por primera vez su volumen; modificar ese SQL no lo vuelve a ejecutar automáticamente sobre una base ya inicializada.

## Consideraciones operativas y de seguridad

- La API consulta PostgreSQL y no expone directamente el broker ni la base de datos. En Compose, el puerto se limita a loopback.
- No hay autenticación/autorización de usuarios implementada en estas rutas; CORS tampoco es un mecanismo de autenticación. Si se publica la API mediante ngrok u otro túnel, sus rutas quedan accesibles desde ese punto público.
- CORS permite actualmente `http://localhost:4200` y `https://earth-watcher-mdp.netlify.app`. Si cambia el origen del panel, actualiza la lista en `main.py`.
- `/health` solo prueba PostgreSQL. Una respuesta saludable no garantiza que USGS o `producer-replay` estén disponibles para una importación nueva.
- `GET /api/earthquakes/{earthquake_id}` no acepta `mode`. Como la clave de datos permite el mismo identificador en `live` y `replay`, la ruta puede ser ambigua si ambos existen; utiliza el listado con filtro de modo cuando necesites distinguirlos.
- Los datos replay son una instantánea del catálogo USGS consultado y no reconstruyen el historial completo de cada revisión pasada.

## Mapa de módulos

| Archivo | Responsabilidad |
| --- | --- |
| [`main.py`](./main.py) | Aplicación FastAPI, CORS, ciclo de vida y registro de routers. |
| [`core/config.py`](./core/config.py) | Configuración por entorno y valores de USGS. |
| [`core/database.py`](./core/database.py) | Pool PostgreSQL e inicialización de `earthquake_import_jobs`. |
| [`routers/earthquakes.py`](./routers/earthquakes.py) | Listado, detalle, filtros, métricas y comparación. |
| [`routers/imports.py`](./routers/imports.py) | Inicio y consulta de trabajos históricos. |
| [`routers/countries.py`](./routers/countries.py) | Endpoint del catálogo de países. |
| [`routers/health.py`](./routers/health.py) | Comprobación de PostgreSQL. |
| [`routers/internal.py`](./routers/internal.py) | WebSocket de actualizaciones live. |
| [`services/earthquake_service.py`](./services/earthquake_service.py) | Coordinación de consultas y métricas. |
| [`repositories/earthquake_repository.py`](./repositories/earthquake_repository.py) | Consultas SQL, filtros y agregaciones. |
| [`services/import_service.py`](./services/import_service.py) | Importación, progreso y reanudación de trabajos. |
| [`services/usgs_service.py`](./services/usgs_service.py) | Cliente HTTP FDSN de USGS y reintentos. |
| [`kafka/kafka_replay_client.py`](./kafka/kafka_replay_client.py) | Cliente HTTP hacia `producer-replay`. |
| [`services/event_listener.py`](./services/event_listener.py) | Listener PostgreSQL y distribución de notificaciones WebSocket. |
| [`Dockerfile`](./Dockerfile) | Imagen Python 3.12 y comando Uvicorn. |
| [`requirements.txt`](./requirements.txt) | Dependencias Python fijadas. |
