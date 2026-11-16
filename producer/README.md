# Productor de Sismic Observer

El productor es el servicio Python que recibe extracciones de terremotos, conserva una copia del GeoJSON original, identifica eventos nuevos o revisados y publica los eventos válidos en Kafka. También expone una pequeña API HTTP para comprobar su estado y, en modo `replay`, recibir extracciones históricas de la API principal.

Este directorio contiene la implementación compartida por los dos servicios del proyecto: `producer-live` y `producer-replay`. La orquestación y los valores concretos de cada servicio están en [`../docker-compose.yaml`](../docker-compose.yaml); el esquema SQL está en [`../database/init.sql`](../database/init.sql).

## Papel en el sistema

```text
USGS (feed de las últimas 24 h)
        │
        ▼
 producer-live ──► Kafka: earthquakes-live ──► consumer-live

API principal ──HTTP──► producer-replay ──► Kafka: earthquakes-replay ──► consumer-replay

Ambos productores ──► PostgreSQL (originales, ejecuciones y estado de publicación)
```

- **`producer-live`** consulta periódicamente el feed GeoJSON de USGS. La respuesta corresponde a una ventana móvil de las últimas 24 horas.
- **`producer-replay`** no consulta USGS ni ejecuta sondeos periódicos. Espera una extracción histórica enviada por HTTP desde la API principal.
- Los dos servicios reutilizan la lógica de validación básica, persistencia, deduplicación y publicación. El modo y el topic de cada instancia se determinan mediante variables de entorno.
- El consumidor procesa los mensajes y persiste el estado enriquecido del terremoto. El productor no escribe en las tablas de terremotos ni ejecuta el enriquecimiento geográfico.

## Ciclo de una extracción

1. En modo `live`, el ciclo descarga el feed configurado en `USGS_URL`. Si hay un timeout, un error de transporte, JSON inválido o una respuesta HTTP `429`/`5xx`, reintenta con espera exponencial acotada. Los demás errores HTTP se propagan sin reintento.
2. En modo `replay`, `POST /extract` recibe desde la API los datos GeoJSON, el cuerpo original y los metadatos de la consulta. El endpoint solo acepta solicitudes cuando `MODE=replay`; serializa el procesamiento para evitar dos extracciones simultáneas en esa instancia.
3. El productor comprueba que la respuesta sea un objeto con una lista `features`. Conserva el cuerpo original y registra la ejecución en PostgreSQL.
4. Recorre las features. Excluye las que no tienen estructura, identificador, propiedades o `updated` válidos, así como las que no son de tipo `earthquake`. Cada exclusión se registra con su motivo y, cuando existe, el evento original.
5. Para cada evento restante, consulta el último estado publicado para su `(event_id, mode)`. Omite una versión más antigua, y también una versión con el mismo `updated` y el mismo contenido. Si `updated` coincide pero cambia el contenido, registra una advertencia y omite el evento.
6. Publica el evento en Kafka y espera la confirmación del broker. Solo después de confirmarla actualiza el estado de publicación en PostgreSQL.
7. Devuelve un resumen de eventos recibidos, publicados, ignorados, excluidos y fallidos. Si falla la publicación de algún evento, el resultado es parcial: el endpoint de replay responde con error HTTP y el ciclo live lo refleja en su estado y en los logs.

El mensaje publicado tiene esta forma:

```json
{
  "source": "USGS",
  "ingested_at": "2026-01-15T12:34:56+00:00",
  "mode": "live",
  "event": {
    "id": "identificador-usgs",
    "type": "Feature",
    "properties": {},
    "geometry": {}
  }
}
```

La clave Kafka es el identificador del evento. El cuerpo del evento mantiene la feature original de USGS; el consumidor es quien la valida en profundidad y la transforma para su almacenamiento.

## Tablas PostgreSQL utilizadas por el productor

El esquema completo se crea desde [`../database/init.sql`](../database/init.sql) al inicializar por primera vez el volumen de PostgreSQL. El productor utiliza estas cuatro tablas:

| Tabla | Qué guarda | Por qué existe |
| --- | --- | --- |
| `raw_extractions` | El cuerpo original de cada respuesta GeoJSON, su SHA-256, tamaño y primeras/últimas fechas de observación. | Preserva la fuente recibida para trazabilidad y análisis. El SHA-256 único permite reconocer respuestas idénticas y actualizar `last_seen` sin guardar otra copia. |
| `ingestion_runs` | Una fila por solicitud o extracción procesada: URL, fuente, horas de inicio y fin, estado HTTP, número de registros y, si aplica, error. Puede referenciar la respuesta de `raw_extractions`. | Permite reconstruir cuándo se consultó o recibió una extracción y revisar el resultado de cada ejecución, aunque el contenido ya existiera. |
| `producer_event_state` | Para cada combinación de identificador y modo, el `updated` más reciente publicado, la huella del payload y la hora de publicación. | Evita volver a publicar duplicados y versiones antiguas después de cada sondeo o reinicio del contenedor. Separar por modo evita que `live` y `replay` se dedupliquen entre sí. |
| `producer_event_exclusions` | Eventos omitidos, con modo, motivo, extracción de origen y JSON original si está disponible. | Hace visibles los registros que el productor descarta en lugar de perderlos silenciosamente. Un índice único parcial evita repetir una exclusión con el mismo identificador, modo y motivo. |

El productor **no** administra directamente `earthquakes`, `earthquake_versions`, `consumer_quarantine` ni `countries`; esas tablas corresponden al consumidor. La API principal, por su parte, utiliza `earthquake_import_jobs` para llevar el progreso de las importaciones históricas.

## Modos y endpoints HTTP

### Modo `live`

- En Compose, consulta el feed `all_day.geojson` cada 60 segundos y publica en `earthquakes-live`.
- Si una extracción tarda más que el intervalo, espera solo el tiempo restante; no agrega 60 segundos completos al tiempo de ejecución.
- `GET /health` comprueba la conexión a PostgreSQL y la disponibilidad del productor Kafka. También exige una extracción exitosa reciente y ningún error consecutivo.

### Modo `replay`

- En Compose, publica en `earthquakes-replay` y queda a la espera de la API principal.
- `POST /extract` acepta un JSON con `data` (GeoJSON), `original_body`, `request_url`, `requested_at`, `completed_at` y `http_status`. Valida que `data.features` sea una lista y que la fecha de finalización no preceda a la de inicio.
- Responde `403` si se llama en un modo distinto de `replay`, `503` si el servicio aún no está preparado y `500` si el procesamiento falla o es parcial.
- `GET /health` comprueba PostgreSQL y Kafka, pero no requiere extracciones recientes: la inactividad es normal en este modo.

### Endpoints comunes

- `GET /` devuelve información del servicio, el modo, el topic y el intervalo de sondeo (solo para `live`).
- `GET /health` devuelve el estado, las dependencias, la última ejecución, los errores consecutivos y el último resumen. Devuelve HTTP `503` cuando el productor no está saludable.

En Compose, `producer-live` publica el puerto `8082` y `producer-replay`, el `8083`. El healthcheck de cada contenedor consulta `/health` en su puerto correspondiente. No se debe exponer estos endpoints a redes no confiables.

## Configuración

El servicio lee estas variables de entorno en [`config.py`](./config.py). Compose ya define valores independientes para cada instancia.

| Variable | Uso | Valor predeterminado en el código |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP` | Dirección del broker Kafka. | `kafka:19092` |
| `KAFKA_TOPIC` | Topic de salida. | `earthquakes-live` |
| `DATABASE_URL` | DSN de PostgreSQL; debe estar configurada. | Sin valor |
| `USGS_URL` | URL consultada por el modo live. | Feed USGS de las últimas 24 horas |
| `POLL_INTERVAL` | Intervalo entre inicios de sondeo live, en segundos. | `60` |
| `SOURCE` | Etiqueta de origen incluida en el mensaje y en el registro de ingesta. | `USGS` |
| `MODE` | Modo de ejecución: `live` o `replay`. | `live` |
| `HTTP_TIMEOUT` | Timeout del cliente HTTP, en segundos. | `30` |
| `MAX_RETRIES` | Número máximo de intentos de descarga live. | `5` |
| `PORT` | Puerto donde Uvicorn sirve la API HTTP. | `8082` en el comando del Dockerfile |

`RAW_RETENTION_DAYS` está declarado en la configuración, pero actualmente no hay una tarea de limpieza automática que lo aplique. Los datos originales no se eliminan por esa variable.

## Ejecución local con Docker Compose

Desde la raíz del repositorio:

```powershell
docker compose up --build -d producer-live producer-replay
docker compose logs -f producer-live producer-replay
```

Estos servicios dependen de que Kafka y PostgreSQL estén saludables. Para ver su estado:

```powershell
docker compose ps producer-live producer-replay
```

En la configuración Compose, ambos servicios se construyen desde este directorio y comparten `DATABASE_URL` y Kafka, pero difieren en `MODE`, `KAFKA_TOPIC`, `USGS_URL` y puerto. Para consultar el estado desde el host:

```text
http://127.0.0.1:8082/health
http://127.0.0.1:8083/health
```

También se puede ejecutar el contenedor junto con el resto del sistema desde la raíz:

```powershell
docker compose up --build -d
```

## Comportamiento y límites importantes

- **Confirmación e idempotencia:** Kafka publica con `acks="all"` y productor idempotente. PostgreSQL se actualiza después de recibir la confirmación de Kafka. La base guarda el estado para que el siguiente ciclo omita lo que ya se publicó.
- **No hay transacción distribuida:** Kafka y PostgreSQL no forman una transacción atómica. Si Kafka confirma el mensaje y falla después la actualización de estado, el evento podría publicarse de nuevo en el siguiente ciclo. El consumidor debe tolerar reenvíos; su persistencia es idempotente.
- **Versiones:** la comparación se basa en `properties.updated` (milisegundos desde Unix epoch) y en una huella SHA-256 del contenido de la feature. El productor no guarda el historial completo de versiones; lo hacen las tablas del consumidor.
- **Ventana live:** el feed de 24 horas no permite inferir que USGS eliminó un evento cuando deja de aparecer. Si se necesita recuperar un periodo anterior a esa ventana, la API debe solicitarlo como importación histórica.
- **Datos históricos:** replay publica la extracción recibida por HTTP; el productor no descarga por sí mismo el histórico ni reconstruye todas las revisiones pasadas de USGS.
- **Persistencia de originales:** el cuerpo se conserva por su hash, por lo que respuestas idénticas comparten la misma fila de `raw_extractions`; cada recepción sigue creando su registro en `ingestion_runs`.

## Mapa de módulos

| Archivo | Responsabilidad |
| --- | --- |
| [`main.py`](./main.py) | Inicializa PostgreSQL, Kafka y HTTP; ejecuta el sondeo live y gestiona el ciclo de vida del servicio. |
| [`extraction.py`](./extraction.py) | Valida la forma básica de la extracción, persiste su original, filtra y deduplica eventos, y coordina la publicación. |
| [`usgs_client.py`](./usgs_client.py) | Descarga el feed live y aplica reintentos ante errores transitorios. |
| [`kafka_client.py`](./kafka_client.py) | Configura la conexión y publica los mensajes con confirmación del broker. |
| [`database.py`](./database.py) | Acceso asíncrono a PostgreSQL y operaciones sobre las tablas del productor. |
| [`api.py`](./api.py) | API HTTP de salud, información y recepción de extracciones históricas. |
| [`config.py`](./config.py) | Carga de variables de entorno. |
| [`Dockerfile`](./Dockerfile) | Imagen Python y comando de arranque con Uvicorn. |
| [`requirements.txt`](./requirements.txt) | Dependencias Python del productor. |
