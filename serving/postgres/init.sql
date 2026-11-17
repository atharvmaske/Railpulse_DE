CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS monitoring;

CREATE TABLE IF NOT EXISTS gold.trip_delays (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    trip_id TEXT NOT NULL,
    route_id TEXT,
    stop_id TEXT,
    delay_seconds INTEGER,
    event_date DATE NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.vehicle_positions (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    vehicle_id TEXT NOT NULL,
    trip_id TEXT,
    route_id TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    speed DOUBLE PRECISION,
    recorded_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.service_alerts (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    alert_id TEXT NOT NULL,
    cause TEXT,
    effect TEXT,
    header_text TEXT,
    route_id TEXT,
    valid_from TIMESTAMPTZ,
    valid_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS monitoring.pipeline_health (
    id SERIAL PRIMARY KEY,
    metric_name TEXT NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    labels JSONB,
    recorded_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.mart_route_performance (
    id TEXT PRIMARY KEY,
    route_id TEXT NOT NULL,
    event_date DATE NOT NULL,
    total_events INTEGER,
    on_time_count INTEGER,
    minor_delay_count INTEGER,
    moderate_delay_count INTEGER,
    severe_delay_count INTEGER,
    avg_delay_seconds NUMERIC,
    avg_delay_minutes NUMERIC,
    max_delay_seconds INTEGER,
    median_delay_seconds NUMERIC,
    p95_delay_seconds NUMERIC,
    on_time_rate_pct NUMERIC,
    performance_grade TEXT,
    slo_breached BOOLEAN,
    dbt_updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (route_id, event_date)
);

CREATE TABLE IF NOT EXISTS gold.mart_delay_summary (
    event_date DATE PRIMARY KEY,
    active_routes INTEGER,
    total_trip_events BIGINT,
    total_on_time BIGINT,
    total_severe_delays BIGINT,
    network_on_time_rate_pct NUMERIC,
    network_avg_delay_minutes NUMERIC,
    worst_delay_minutes NUMERIC,
    grade_a_routes INTEGER,
    grade_f_routes INTEGER,
    slo_breached_routes INTEGER,
    dbt_updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_trip_delays_date ON gold.trip_delays(event_date);
CREATE INDEX IF NOT EXISTS idx_trip_delays_route ON gold.trip_delays(route_id);
CREATE INDEX IF NOT EXISTS idx_vehicle_pos_vehicle ON gold.vehicle_positions(vehicle_id);
CREATE INDEX IF NOT EXISTS idx_alerts_route ON gold.service_alerts(route_id);
CREATE INDEX IF NOT EXISTS idx_mart_perf_date ON gold.mart_route_performance(event_date);
CREATE INDEX IF NOT EXISTS idx_mart_perf_grade ON gold.mart_route_performance(performance_grade);
CREATE INDEX IF NOT EXISTS idx_mart_summary_date ON gold.mart_delay_summary(event_date);