
/*Tabla para conservar el cuerpo original de usgs como texto. Respuesats geojson originales*/
CREATE TABLE IF NOT EXISTS raw_extractions (
    id BIGSERIAL PRIMARY KEY,
    sha256 VARCHAR(64) NOT NULL UNIQUE,
    response_body TEXT NOT NULL,
    first_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    size_bytes BIGINT NOT NULL
);

/*Registro de cada consulta, incluso si la respuesta es igual a la anterior*/
CREATE TABLE IF NOT EXISTS ingestion_runs (
    id BIGSERIAL PRIMARY KEY,
    raw_extraction_id BIGINT REFERENCES raw_extractions(id),
    source VARCHAR(50) NOT NULL DEFAULT 'USGS',
    request_url TEXT NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    http_status INTEGER,
    status VARCHAR(30) NOT NULL,
    records_received INTEGER DEFAULT 0,
    error_message TEXT
);

CREATE INDEX idx_ingestion_runs_requested
ON ingestion_runs(requested_at DESC);

/*Para la deteccion de revisiones aunque se reinicie el contenedor*/
CREATE TABLE IF NOT EXISTS producer_event_state (
    event_id VARCHAR(100) NOT NULL,
    mode VARCHAR(20) NOT NULL,
    last_published_updated_ms BIGINT NOT NULL,
    last_payload_hash VARCHAR(64),
    last_published_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (event_id, mode)
);

CREATE INDEX idx_producer_state_published
ON producer_event_state(last_published_at DESC);

