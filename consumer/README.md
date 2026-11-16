# Consumidor de Sismic Observer

El consumidor lee mensajes de terremotos desde Kafka, valida su estructura, convierte los campos a un modelo tabular, asigna un país usando el catálogo Natural Earth y persiste el evento y sus versiones en PostgreSQL. Se ejecuta como dos servicios con la misma imagen y el mismo código: uno para el flujo reciente (`live`) y otro para las importaciones históricas (`replay`).

La orquestación de ambos servicios está en [`../docker-compose.yaml`](../docker-compose.yaml); el esquema de base de datos se define en [`../database/init.sql`](../database/init.sql).

## Lugar en el sistema

```text
producer-live ──► Kafka: earthquakes-live ──► consumer-live ──┐
                                                              ├──► PostgreSQL ──► API
API ──► producer-replay ──► Kafka: earthquakes-replay ──► consumer-replay ─┘
```

- `consumer-live` procesa mensajes de `earthquakes-live`.
- `consumer-replay` procesa mensajes de `earthquakes-replay`.
- Cada modo tiene un grupo Kafka y conserva sus datos separados mediante la columna `mode`; el mismo identificador puede existir tanto en `live` como en `replay`.
- El consumidor no consulta USGS ni publica en Kafka. Recibe el evento original del productor y prepara los datos para su consulta posterior.

## Mensaje de entrada

El productor publica un objeto JSON con metadatos de ingesta y la feature GeoJSON original:

```json
{
  "source": "USGS",
  "ingested_at": "2026-01-15T12:34:56+00:00",
  "mode": "live",
  "event": {
    "id": "identificador-usgs",
    "type": "Feature",
    "properties": {
      "time": 1768480000000,
      "updated": 1768480100000,
      "mag": 4.2,
      "magType": "mb",
      "type": "earthquake",
      "place": "ejemplo"
    },
    "geometry": {
      "type": "Point",
      "coordinates": [-122.3, 38.1, 5.0]
    }
  }
}
```

`time` y `updated` son milisegundos desde Unix epoch. Las coordenadas siguen el orden GeoJSON: longitud, latitud y profundidad en kilómetros. La magnitud y la profundidad pueden ser nulas.

## Ciclo de procesamiento

Al arrancar, el servicio abre un pool de conexiones con PostgreSQL, carga el catálogo geográfico desde `GEOJSON_PATH`, persiste sus países y, a continuación, inicia el consumidor Kafka.

Para cada mensaje:

1. Decodifica el valor Kafka como UTF-8 y JSON.
2. Valida el envoltorio, la feature, el identificador, la geometría, las coordenadas, los tiempos, el modo, la fuente y `ingested_at`.
3. Intenta guardar en `consumer_quarantine` los mensajes JSON inválidos o que no superan la validación. También pone en cuarentena eventos cuyo `properties.type` no sea `earthquake`. Si la escritura en cuarentena falla, no confirma el offset.
4. Convierte el evento a los campos de la base de datos. Los tiempos se convierten a UTC, y los valores nulos de magnitud o profundidad se conservan como nulos.
5. Busca el país a partir del punto geográfico y añade `country_code`. Los eventos que no coinciden con ningún polígono se conservan con ese campo nulo.
6. En una transacción PostgreSQL, inserta la versión original y actualiza el evento actual si la versión recibida es más reciente.
7. Para mensajes `live`, emite además una notificación PostgreSQL en el canal `new_earthquake`, que puede consumir la API.
8. Confirma el offset Kafka después de completar el procesamiento. Si ocurre un error de base de datos u otro error inesperado, el offset no se confirma y Kafka puede entregar el mensaje de nuevo.

El consumidor desactiva el auto-commit de offsets. Para los grupos sin offsets previos, empieza desde `earliest`; los grupos existentes reanudan desde su offset confirmado.

### Validación y cuarentena

La validación de [`app/validator.py`](./app/validator.py) comprueba:

- Que `event` sea una feature GeoJSON con identificador no vacío, `properties` y geometría `Point`.
- Que haya tres coordenadas; longitud y latitud deben ser numéricas, finitas y estar en rango. La profundidad admite nulo; si tiene valor, debe ser numérica y finita.
- Que magnitud admita nulo y, si tiene valor, sea numérica y finita.
- Que `time` y `updated` sean timestamps enteros válidos en milisegundos.
- Que `mode` sea `live` o `replay`, que `source` no esté vacío y que `ingested_at` sea una fecha ISO con zona horaria.

Los rechazos que se guardan correctamente conservan el mensaje original y una razón en `consumer_quarantine`. Los eventos de otros tipos también se guardan allí, en lugar de tratarse como terremotos. Un offset solo se confirma después de guardar correctamente el rechazo. El decodificador envuelve un cuerpo JSON malformado como `{"raw": "..."}`; en cambio, un JSON válido cuyo valor raíz no sea un objeto (por ejemplo, una lista o una cadena) puede fallar al guardarse, porque la rutina de cuarentena espera un objeto con campos `event` y `mode`. En ese caso no se confirma el offset.

### Transformación y calidad

[`app/transformer.py`](./app/transformer.py) mapea los campos USGS al modelo de persistencia, mantiene `updated_ms` para ordenar versiones y calcula una huella SHA-256 del mensaje. La huella se guarda junto al JSON original de cada versión.

[`app/quality.py`](./app/quality.py) se ejecuta después de validar, transformar y asignar `country_code`, antes de guardar el evento. Comprueba faltantes, valores negativos que requieren revisión, consistencia temporal y asignación geográfica. Sus incidencias se escriben en los logs; no rechazan ni ponen en cuarentena el evento. La falta de país se conserva como `NULL` y solo se registra a nivel DEBUG, porque un terremoto sin país asignado no es necesariamente un error. No se rechazan magnitudes ni profundidades negativas por sí mismas.

### Resultados del análisis post-procesamiento

El notebook [`../notebooks/01_analisis_de_calidad.ipynb`](../notebooks/01_analisis_de_calidad.ipynb) analiza una instantánea de PostgreSQL de noviembre de 2026: 396.844 eventos actuales, 397.053 versiones y 177 países. Es una evaluación post-procesamiento de esos datos y precede a la integración de `quality.py` en el bucle de consumo; no refleja los logs generados por esta llamada. Los resultados detallados y sus implicaciones para el sistema están en la sección [Resultados y consideraciones del análisis de calidad](../README.md#resultados-y-consideraciones-del-análisis-de-calidad).

En esa instantánea, los campos estructurales obligatorios, la validez según las reglas comprobadas, la unicidad por `(event_id, mode)` y la correspondencia de los campos comparados con los mensajes originales alcanzan el 100%. Magnitud y tipo de magnitud tienen 21 nulos cada uno; deben conservarse como nulos. Hay 97.923 eventos sin país asignado (24,68%), que no se consideran errores por sí mismos. La tabla de cuarentena estaba vacía: esto describe los datos observados, pero no sustituye pruebas con mensajes inválidos.

El análisis detecta 100 filas en las que `processed_at < ingested_at`, 99 de ellas en `live`. La causa probable está en el upsert actual: al guardar una revisión se actualiza `ingested_at`, pero `processed_at` conserva el valor de la inserción inicial. Por ello, la latencia negativa y el 66,43% de `live` dentro del umbral de 60 segundos no deben presentarse como una medición fiable hasta corregir la semántica de `processed_at` y volver a ejecutar el notebook. El resultado global de actualidad (99,95%) está dominado por `replay` y no refleja bien este problema específico de `live`.

## Enriquecimiento geográfico

[`app/geography.py`](./app/geography.py) carga un GeoJSON de países con polígonos y multipolígonos:

- Comprueba que cada entidad tenga `ADM0_A3` y que no haya códigos duplicados.
- Omite entidades sin geometría o cuya geometría no sea poligonal; repara geometrías inválidas con Shapely cuando es posible.
- Construye un `STRtree` espacial para consultar los polígonos candidatos.
- Busca primero puntos estrictamente interiores; si no hay coincidencia, incluye puntos en las fronteras. Si varios países coinciden, ordena los códigos `ADM0_A3` y elige el primero.
- Si no hay coincidencia —por ejemplo, un terremoto en el mar— asigna `country_code = NULL`; no busca el país más cercano.

Al iniciar, el consumidor inserta en `countries` los códigos todavía no presentes. Usa `ON CONFLICT DO NOTHING`, así que no actualiza los atributos de países ya guardados si cambia el fichero GeoJSON.

El archivo de catálogo debe existir dentro del contenedor. Compose lo monta en modo lectura desde `../sismic-panel/public/datos/paises.geojson` (relativo a la raíz del repositorio) como `/app/data/paises.geojson`. Si falta o el catálogo no contiene geometrías válidas, el servicio no puede iniciar.

## Persistencia y versionado

El esquema y sus índices están en [`../database/init.sql`](../database/init.sql):

| Tabla | Uso del consumidor |
| --- | --- |
| `countries` | Catálogo de países identificado por `ADM0_A3`, con nombre y región. |
| `earthquakes` | Estado actual por `(event_id, mode)`, con magnitud, coordenadas, fechas, país y fuente. |
| `earthquake_versions` | Mensaje JSON original por `(event_id, mode, updated_ms)` y su huella. |
| `consumer_quarantine` | Mensajes no procesados, su motivo y el JSON original. |

La inserción de versión y la actualización del estado actual se hacen dentro de una transacción. Las claves únicas hacen idempotentes los reenvíos de la misma versión. El estado actual solo cambia cuando `updated_ms` es mayor que el almacenado; una versión más antigua se conserva en el historial, pero no sustituye al estado actual. Si ya existe la misma combinación de identificador, modo y `updated_ms`, la versión se considera duplicada.

Kafka y PostgreSQL no comparten una transacción distribuida. Por tanto, el consumidor ofrece procesamiento al menos una vez: un error después de guardar en PostgreSQL, pero antes de confirmar el offset, puede causar una nueva entrega. Las claves y operaciones idempotentes evitan volver a insertar la misma versión o reemplazar una más reciente.

## Configuración

Las variables se cargan en [`app/config.py`](./app/config.py). Compose define valores separados para cada modo.

| Variable | Descripción | Valor predeterminado en el código |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP` | Broker Kafka. | `kafka:19092` |
| `KAFKA_TOPIC` | Topic que consume esta instancia. | `earthquakes` |
| `KAFKA_GROUP` | Grupo de consumidores Kafka. | `earthquake-processor` |
| `DATABASE_URL` | DSN de PostgreSQL. Obligatoria. | Sin valor; el servicio falla si no se configura. |
| `GEOJSON_PATH` | Ruta del catálogo dentro del contenedor. | `/app/data/paises.geojson` |
| `HEALTH_PORT` | Puerto HTTP para el endpoint de salud. | `8081` |

Configuración actual en Compose:

| Servicio | Topic | Grupo |
| --- | --- | --- |
| `consumer-live` | `earthquakes-live` | `earthquake-live-processor` |
| `consumer-replay` | `earthquakes-replay` | `earthquake-replay-processor` |

Ambos se construyen desde [`Dockerfile`](./Dockerfile), que usa Python 3.12 y las dependencias fijadas en [`requirements.txt`](./requirements.txt). Compose los conecta a Kafka y PostgreSQL por los nombres internos de servicio; no se debe usar `localhost` para esas dependencias desde el contenedor.

## Ejecución y diagnóstico

Desde la raíz del repositorio, prepara `.env` con las variables de PostgreSQL siguiendo el [`README principal`](../README.md), verifica que existe el GeoJSON de países en la ruta montada y levanta los consumidores:

```powershell
docker compose up --build -d consumer-live consumer-replay
```

Compose también inicia las dependencias declaradas por estos servicios. Para inspeccionar su estado y registros:

```powershell
docker compose ps consumer-live consumer-replay
docker compose logs -f consumer-live consumer-replay
```

Cada contenedor expone internamente `GET /health` en el puerto `8081`; Compose lo consulta mediante un healthcheck. Los servicios consumidores no publican ese puerto en el host. La respuesta incluye el topic y el grupo; devuelve HTTP 503 si la tarea de consumo no está activa.

El servicio registra el inicio, la conexión con Kafka, cada resultado de guardado y los rechazos. Los errores inesperados se registran y detienen la tarea de consumo; el healthcheck pasa a estado no saludable y los mensajes cuyo offset no se confirmó pueden volver a procesarse.

Para iniciar todo el sistema en lugar de solo los consumidores:

```powershell
docker compose up --build -d
```

## Mapa de módulos

| Archivo | Responsabilidad |
| --- | --- |
| [`app/main.py`](./app/main.py) | Ciclo de vida del servicio, consumo Kafka, confirmación de offsets, endpoint `/health` y coordinación del procesamiento. |
| [`app/config.py`](./app/config.py) | Lectura de la configuración de entorno. |
| [`app/validator.py`](./app/validator.py) | Validación del mensaje y sus campos. |
| [`app/transformer.py`](./app/transformer.py) | Conversión del mensaje a campos persistibles y cálculo de la huella. |
| [`app/geography.py`](./app/geography.py) | Carga del catálogo Natural Earth y asignación espacial del país. |
| [`app/database.py`](./app/database.py) | Pool PostgreSQL, persistencia transaccional, cuarentena y notificación `new_earthquake`. |
| [`app/quality.py`](./app/quality.py) | Evalúa incidencias después del enriquecimiento geográfico y antes de persistir; registra hallazgos sin rechazar el evento. |
| [`Dockerfile`](./Dockerfile) | Imagen Python y comando de arranque del servicio. |
| [`requirements.txt`](./requirements.txt) | Dependencias Python fijadas. |
