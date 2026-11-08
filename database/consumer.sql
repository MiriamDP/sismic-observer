/*-----------------------------------*/
/* Para el consumidor                */
/*-----------------------------------*/

/* Catalogo georgrafico*/
CREATE TABLE IF NOT EXISTS countries (
    adm0_a3 VARCHAR(10) PRIMARY KEY,
    name_es VARCHAR(150),
    admin VARCHAR(150),
    continent VARCHAR(100),
    subregion VARCHAR(100)
);

/*Terremotos recibidos*/
CREATE TABLE IF NOT EXISTS earthquakes (
    event_id VARCHAR(100) NOT NULL,
    mode VARCHAR(20) NOT NULL,

    source VARCHAR(50) NOT NULL,

    magnitude NUMERIC,
    magnitude_type VARCHAR(30),
    event_type VARCHAR(50),
    place TEXT,

    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    depth_km DOUBLE PRECISION,

    event_time TIMESTAMPTZ,
    updated TIMESTAMPTZ,
    updated_ms BIGINT NOT NULL,

    ingested_at TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    country_code VARCHAR(10)
        REFERENCES countries(adm0_a3),

    PRIMARY KEY (event_id, mode),

    CHECK (mode IN ('live', 'replay'))
);

/*Versionado de los terremotos del consumidor*/
CREATE TABLE IF NOT EXISTS earthquake_versions (
    event_id VARCHAR(100) NOT NULL,
    mode VARCHAR(20) NOT NULL,
    updated_ms BIGINT NOT NULL,

    original_message JSONB NOT NULL,
    payload_hash VARCHAR(64),

    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (event_id, mode, updated_ms)
);

/*Eventos excluidos por el consumidor*/
CREATE TABLE IF NOT EXISTS consumer_quarantine (
    id BIGSERIAL PRIMARY KEY,

    event_id VARCHAR(100),
    mode VARCHAR(20),

    reason TEXT NOT NULL,

    original_message JSONB NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_earthquakes_event_time
ON earthquakes(event_time);

CREATE INDEX IF NOT EXISTS idx_earthquakes_country
ON earthquakes(country_code);

CREATE INDEX IF NOT EXISTS idx_earthquakes_magnitude
ON earthquakes(magnitude);

CREATE INDEX IF NOT EXISTS idx_earthquakes_updated
ON earthquakes(updated DESC);

CREATE INDEX IF NOT EXISTS idx_versions_event
ON earthquake_versions(event_id, mode, updated_ms DESC);

CREATE INDEX IF NOT EXISTS idx_quarantine_event
ON consumer_quarantine(event_id);