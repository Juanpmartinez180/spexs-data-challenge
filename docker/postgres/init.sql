-- Creación de esquemas para arquitectura Medallion
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS audit;

-- Extensión para logs y performance si fuera necesario
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Creacion de las tablas necesarias para la plataforma
--- Audit table
CREATE TABLE IF NOT EXISTS audit.ingestion_logs (
    log_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    table_name TEXT NOT NULL,
    source_file TEXT,
    execution_start TIMESTAMP NOT NULL,
    execution_end TIMESTAMP,
    status TEXT CHECK (status IN ('success', 'fail', 'processing')),
    records_inserted INTEGER DEFAULT 0,
    error_message TEXT,
    execution_time_seconds INTERVAL GENERATED ALWAYS AS (execution_end - execution_start) STORED
);
--- Bronze tables
CREATE TABLE IF NOT EXISTS bronze.app_usage_data (
    region TEXT,
    origin_coord TEXT,
    destination_coord TEXT,
    datetime TEXT,
    datasource TEXT,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    layer TEXT DEFAULT 'bronze'
);

CREATE TABLE IF NOT EXISTS bronze.cell_phone_data (
    region TEXT,
    origin_coord TEXT,
    destination_coord TEXT,
    datetime TEXT,
    datasource TEXT,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    layer TEXT DEFAULT 'bronze'
);

CREATE TABLE IF NOT EXISTS bronze.car_navigation_data (
    region TEXT,
    origin_coord TEXT,
    destination_coord TEXT,
    datetime TEXT,
    datasource TEXT,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    layer TEXT DEFAULT 'bronze'
);

--- Silver tables
CREATE TABLE IF NOT EXISTS silver.trips_events (
        trip_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        region TEXT,
        origin_geohash TEXT,
        destination_geohash TEXT,
        departure_time TIMESTAMP,
        datasource TEXT,
        similarity_key TEXT, -- Hash combinado para agrupar
        inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        layer TEXT DEFAULT 'silver',
        CONSTRAINT unique_trip_similarity UNIQUE (similarity_key)
);

--- Gold tables
CREATE TABLE IF NOT EXISTS gold.events_fact (
    trip_id UUID PRIMARY KEY,
    origin_geohash TEXT,
    destination_geohash TEXT,
    region TEXT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    layer TEXT DEFAULT 'gold'
);
CREATE TABLE IF NOT EXISTS gold.weekly_region_stats_fact (
    region TEXT,
    week_number INTEGER,
    year INTEGER,
    avg_trips_daily NUMERIC,
    total_trips INTEGER,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    layer TEXT DEFAULT 'gold'
    PRIMARY KEY (region, week_number, year)
);
CREATE TABLE IF NOT EXISTS gold.geolocation_dim (
    region TEXT,
    country TEXT,
    region_bounding_box TEXT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    layer TEXT DEFAULT 'gold',
    PRIMARY KEY (region)
);