# RailPulse DE — Real-Time German Transit Data Platform

A production-grade data engineering pipeline that ingests live German railway GTFS-Realtime feeds, processes 130,000+ events per poll, and serves Gold-layer analytics through PostgreSQL and dbt.

## Key Metrics

| Metric | Value |
|---|---|
| Events per poll | ~132,000 |
| Total rows loaded | 1,001,554 |
| Routes tracked | 169,034 |
| Network on-time rate | 93.16% |
| Worst delay detected | 119.9 minutes |
| dbt models | 7 |
| dbt tests passing | 16/16 |
| GTFS Static rows (S3) | 36.7M stop_times |

---

## Stack

| Layer | Technology |
|---|---|
| Ingestion | Python, kafka-python-ng, gtfs-realtime-bindings |
| Message broker | Apache Kafka 3-broker (RF=3, minISR=2) |
| Stream processing | PySpark Structured Streaming (Bronze/Silver) |
| Object storage | AWS S3 (Parquet, partitioned by date) |
| Warehouse | PostgreSQL / Amazon Redshift (target) |
| Transformation | dbt-core 1.7, dbt-postgres |
| Orchestration | Apache Airflow 2.9 (3 DAGs) |
| Monitoring | Prometheus, Grafana |
| CI | GitHub Actions |
| Containerisation | Docker Compose (13 services) |

---

## Project Structure

```
Railpulse_de/
├── ingestion/
│   ├── gtfs_rt/           # Poller, parser, Kafka producer, metrics
│   └── gtfs_static/       # GTFS Static → S3 Parquet loader
├── dbt_transform/
│   ├── models/
│   │   ├── staging/       # stg_trip_updates, stg_vehicle_positions
│   │   ├── intermediate/  # int_route_delays, int_vehicle_activity
│   │   └── marts/         # mart_route_performance, mart_delay_summary
│   └── seeds/             # Sample data for CI
├── airflow/dags/          # railpulse_pipeline, monitoring, backfill
├── scripts/
│   └── pipeline_loop.py   # Kafka → PostgreSQL consumer loop
├── serving/postgres/
│   └── init.sql           # Gold schema + indexes
├── monitoring/            # Prometheus + Grafana provisioning
└── docker-compose.yml     # Full stack (13 services)
```

---

## dbt Lineage

```
Sources (gold.trip_delays, gold.vehicle_positions)
    │
    ▼
Staging (views)
  stg_trip_updates       — delay categorisation, data quality flags
  stg_vehicle_positions  — speed categories, Germany geo validation
  stg_service_alerts     — active alert detection
    │
    ▼
Intermediate (views)
  int_route_delays        — per-route per-day aggregates + percentiles
  int_vehicle_activity    — hourly vehicle summary + last known position
    │
    ▼
Marts (tables)
  mart_route_performance  — A/B/C/D/F grade, SLO breach flag
  mart_delay_summary      — network-wide daily KPIs
```

---

## Airflow DAGs

| DAG | Schedule | Purpose |
|---|---|---|
| `railpulse_pipeline` | `0 2 * * *` | Daily dbt run + data quality checks |
| `railpulse_monitoring` | `*/5 * * * *` | 5-minute pipeline health metrics |
| `railpulse_backfill` | Manual | Historical data replay |

---

## Quick Start

### Prerequisites
- Docker Desktop
- Python 3.11+
- AWS credentials (for S3)

### 1. Clone and configure
```bash
git clone https://github.com/atharvmaske/Railpulse_DE
cd Railpulse_DE
cp .env.example .env
# edit .env with your AWS credentials
```

### 2. Start the stack
```bash
docker compose up -d
```

### 3. Run ingestion (Terminal 1)
```bash
pip install -r requirements.txt
python -m ingestion.run_ingestion
```

### 4. Run pipeline loop (Terminal 2)
```bash
python scripts/pipeline_loop.py
```

### 5. Run dbt transforms
```bash
cd dbt_transform
python -m dbt.cli.main run --profiles-dir .
python -m dbt.cli.main test --profiles-dir .
```

### 6. Load GTFS Static to S3
```bash
python -m ingestion.gtfs_static.main
```

---

## Services & Ports

| Service | Port | URL |
|---|---|---|
| Kafka UI | 8080 | http://localhost:8080 |
| Airflow | 8090 | http://localhost:8090 |
| Grafana | 3000 | http://localhost:3000 |
| Prometheus | 9090 | http://localhost:9090 |
| Schema Registry | 8081 | http://localhost:8081 |
| PostgreSQL | 5432 | — |
| Ingestion metrics | 8000 | http://localhost:8000/metrics |

---

## Data Quality

dbt tests cover:
- `not_null` on all primary keys and business-critical columns
- `unique` on `mart_route_performance.id` and `mart_delay_summary.event_date`
- `accepted_values` on `performance_grade` (A/B/C/D/F)
- `dbt_utils.accepted_range` on `on_time_rate_pct` (0–100) and `total_events` (≥0)

---

## AWS S3 Layout

```
railpulse-static/
└── dims/
    ├── routes/loaded_date=2026-11-17/routes.parquet          (24,782 rows)
    ├── stops/loaded_date=2026-11-17/stops.parquet            (691,644 rows)
    ├── trips/loaded_date=2026-11-17/trips.parquet            (1,795,961 rows)
    ├── stop_times/loaded_date=2026-11-17/stop_times_part*.parquet (36,734,173 rows)
    └── calendar/loaded_date=2026-11-17/calendar.parquet      (4,956 rows)
```

---

## CI Pipeline (GitHub Actions)

```
lint          ruff check on ingestion/ scripts/ serving/
test-ingestion  pytest ingestion/tests/ (parser + GTFS static)
test-dbt      dbt seed + run + test against ephemeral Postgres
validate-compose  docker compose config
```

---

## Author

**Atharva Maske**
[LinkedIn](https://www.linkedin.com/in/atharv-maske-61a983257/) | [GitHub](https://github.com/atharvmaske) | maskeatharv03@gmail.com
