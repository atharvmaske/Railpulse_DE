import json
import logging
import os
import sys
import time
import subprocess

import psycopg2
import psycopg2.extras
from kafka import KafkaConsumer, TopicPartition

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    stream=sys.stdout,
)

logger = logging.getLogger(__name__)

KAFKA_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092,localhost:9093,localhost:9094"
)
KAFKA_TOPIC = "raw-events"
CONSUMER_GROUP = "pipeline-loop-v2"
POLL_DURATION = int(os.getenv("PIPELINE_POLL_DURATION_S", "60"))
BATCH_SIZE = int(os.getenv("PIPELINE_BATCH_SIZE", "2000"))


def get_conn():
    password = os.getenv("POSTGRES_PASSWORD")

    if not password:
        raise RuntimeError(
            "POSTGRES_PASSWORD environment variable is not configured"
        )

    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "railpulse"),
        user=os.getenv("POSTGRES_USER", "railpulse"),
        password=password,
    )



def ensure_mart_tables(conn) -> None:
    cur = conn.cursor()

    cur.execute("""
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
    """)

    cur.execute("""
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
    """)

    conn.commit()
    cur.close()


def fresh_start(conn) -> None:
    logger.info("FRESH START: truncating all gold tables...")

    cur = conn.cursor()

    try:
        cur.execute(
            "TRUNCATE TABLE gold.trip_delays RESTART IDENTITY CASCADE;"
        )
        cur.execute(
            "TRUNCATE TABLE gold.vehicle_positions RESTART IDENTITY CASCADE;"
        )
        cur.execute("TRUNCATE TABLE gold.mart_route_performance;")
        cur.execute("TRUNCATE TABLE gold.mart_delay_summary;")

        conn.commit()
        logger.info("Gold tables truncated.")

    except Exception as e:
        conn.rollback()
        logger.error(f"Truncate failed: {e}")
        raise

    finally:
        cur.close()


def seek_to_latest() -> None:
    logger.info(
        f"Seeking consumer group '{CONSUMER_GROUP}' to latest offsets..."
    )

    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_SERVERS,
        group_id=CONSUMER_GROUP,
        enable_auto_commit=True,
        auto_offset_reset="latest",
    )

    partitions = consumer.partitions_for_topic(KAFKA_TOPIC)

    if not partitions:
        logger.warning(f"Topic '{KAFKA_TOPIC}' not found — skipping seek")
        consumer.close()
        return

    tps = [
        TopicPartition(KAFKA_TOPIC, p)
        for p in partitions
    ]

    consumer.assign(tps)
    consumer.seek_to_end(*tps)
    consumer.commit()
    consumer.close()

    logger.info(f"Seeked {len(tps)} partitions to end and committed.")


def load_from_kafka(conn) -> int:
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        fetch_max_bytes=52_428_800,
        max_poll_records=500,
    )

    partitions = consumer.partitions_for_topic(KAFKA_TOPIC)

    if partitions:
        tps = [
            TopicPartition(KAFKA_TOPIC, p)
            for p in partitions
        ]
        consumer.assign(tps)

    cur = conn.cursor()
    batch = []
    total_read = 0
    total_inserted = 0
    deadline = time.monotonic() + POLL_DURATION

    try:
        while time.monotonic() < deadline:
            records = consumer.poll(
                timeout_ms=1000,
                max_records=500
            )

            for tp_messages in records.values():
                for msg in tp_messages:
                    event = msg.value
                    total_read += 1

                    if event.get("event_type") != "trip_update":
                        continue

                    tu = event.get("trip_update") or {}
                    trip_id = tu.get("trip_id")

                    if not trip_id:
                        continue

                    ingested_raw = event.get("ingested_at") or ""
                    event_date = (
                        ingested_raw[:10]
                        if ingested_raw
                        else None
                    )

                    if not event_date:
                        continue

                    batch.append((
                        trip_id,
                        tu.get("route_id"),
                        tu.get("stop_id"),
                        tu.get("delay_seconds"),
                        event_date,
                        ingested_raw,
                    ))

                    if len(batch) >= BATCH_SIZE:
                        inserted = _insert_batch(cur, conn, batch)
                        total_inserted += inserted

                        logger.info(
                            f"  Flushed batch → {inserted:,} rows inserted "
                            f"(total so far: {total_inserted:,})"
                        )

                        batch = []

        if batch:
            inserted = _insert_batch(cur, conn, batch)
            total_inserted += inserted

    except Exception as e:
        logger.error(
            f"load_from_kafka error: {e}",
            exc_info=True
        )

        try:
            conn.rollback()
        except Exception:
            pass

    finally:
        consumer.close()
        cur.close()

    logger.info(
        f"Kafka poll done | read={total_read:,} "
        f"trip_updates={total_inserted:,}"
    )

    return total_inserted


def _insert_batch(cur, conn, rows: list) -> int:
    try:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO gold.trip_delays
                (trip_id, route_id, stop_id, delay_seconds, event_date, ingested_at)
            VALUES %s
            ON CONFLICT DO NOTHING
            """,
            rows,
            template="(%s, %s, %s, %s, %s::date, %s::timestamptz)",
            page_size=500,
        )

        conn.commit()
        return len(rows)

    except Exception as e:
        logger.error(f"Batch insert failed: {e}")

        try:
            conn.rollback()
        except Exception:
            pass

        return 0


def rebuild_gold(conn) -> None:
    cur = conn.cursor()

    try:
        logger.info("Rebuilding gold.mart_route_performance...")

        cur.execute(
            "DELETE FROM gold.mart_route_performance;"
        )

        cur.execute("""
            INSERT INTO gold.mart_route_performance
            (id, route_id, event_date, total_events, on_time_count,
             minor_delay_count, moderate_delay_count, severe_delay_count,
             avg_delay_seconds, avg_delay_minutes, max_delay_seconds,
             median_delay_seconds, p95_delay_seconds, on_time_rate_pct,
             performance_grade, slo_breached, dbt_updated_at)
            SELECT
                md5(COALESCE(route_id, trip_id) || '|' || event_date::text),
                COALESCE(route_id, trip_id) AS route_id,
                event_date,
                COUNT(*) AS total_events,
                COUNT(*) FILTER (
                    WHERE COALESCE(delay_seconds, 0) <= 0
                ) AS on_time_count,
                COUNT(*) FILTER (
                    WHERE delay_seconds BETWEEN 1 AND 60
                ) AS minor_delay_count,
                COUNT(*) FILTER (
                    WHERE delay_seconds BETWEEN 61 AND 300
                ) AS moderate_delay_count,
                COUNT(*) FILTER (
                    WHERE delay_seconds > 300
                ) AS severe_delay_count,
                ROUND(
                    AVG(COALESCE(delay_seconds, 0))::numeric, 1
                ) AS avg_delay_seconds,
                ROUND(
                    (AVG(COALESCE(delay_seconds, 0)) / 60.0)::numeric, 2
                ) AS avg_delay_minutes,
                MAX(delay_seconds) AS max_delay_seconds,
                PERCENTILE_CONT(0.5) WITHIN GROUP (
                    ORDER BY COALESCE(delay_seconds, 0)
                ) AS median_delay_seconds,
                PERCENTILE_CONT(0.95) WITHIN GROUP (
                    ORDER BY COALESCE(delay_seconds, 0)
                ) AS p95_delay_seconds,
                ROUND(
                    100.0 * COUNT(*) FILTER (
                        WHERE COALESCE(delay_seconds, 0) <= 0
                    ) / NULLIF(COUNT(*), 0),
                    2
                ) AS on_time_rate_pct,
                CASE
                    WHEN ROUND(
                        100.0 * COUNT(*) FILTER (
                            WHERE COALESCE(delay_seconds, 0) <= 0
                        ) / NULLIF(COUNT(*), 0), 2
                    ) >= 95 THEN 'A'
                    WHEN ROUND(
                        100.0 * COUNT(*) FILTER (
                            WHERE COALESCE(delay_seconds, 0) <= 0
                        ) / NULLIF(COUNT(*), 0), 2
                    ) >= 85 THEN 'B'
                    WHEN ROUND(
                        100.0 * COUNT(*) FILTER (
                            WHERE COALESCE(delay_seconds, 0) <= 0
                        ) / NULLIF(COUNT(*), 0), 2
                    ) >= 75 THEN 'C'
                    WHEN ROUND(
                        100.0 * COUNT(*) FILTER (
                            WHERE COALESCE(delay_seconds, 0) <= 0
                        ) / NULLIF(COUNT(*), 0), 2
                    ) >= 60 THEN 'D'
                    ELSE 'F'
                END AS performance_grade,
                CASE
                    WHEN ROUND(
                        100.0 * COUNT(*) FILTER (
                            WHERE COALESCE(delay_seconds, 0) <= 0
                        ) / NULLIF(COUNT(*), 0), 2
                    ) < 75 THEN true
                    ELSE false
                END AS slo_breached,
                NOW() AS dbt_updated_at
            FROM gold.trip_delays
            WHERE trip_id IS NOT NULL
              AND event_date IS NOT NULL
            GROUP BY COALESCE(route_id, trip_id), event_date
            HAVING COUNT(*) >= 2;
        """)

        conn.commit()

        cur.execute("""
            SELECT COUNT(*), COUNT(DISTINCT route_id)
            FROM gold.mart_route_performance;
        """)

        rows, routes = cur.fetchone()

        logger.info(
            f"  mart_route_performance → {rows:,} rows, {routes} routes"
        )

        logger.info("Rebuilding gold.mart_delay_summary...")

        cur.execute(
            "DELETE FROM gold.mart_delay_summary;"
        )

        cur.execute("""
            INSERT INTO gold.mart_delay_summary
            (event_date, active_routes, total_trip_events, total_on_time,
             total_severe_delays, network_on_time_rate_pct,
             network_avg_delay_minutes, worst_delay_minutes,
             grade_a_routes, grade_f_routes, slo_breached_routes, dbt_updated_at)
            SELECT
                event_date,
                COUNT(DISTINCT route_id) AS active_routes,
                SUM(total_events) AS total_trip_events,
                SUM(on_time_count) AS total_on_time,
                SUM(severe_delay_count) AS total_severe_delays,
                ROUND(AVG(on_time_rate_pct)::numeric, 2)
                    AS network_on_time_rate_pct,
                ROUND(AVG(avg_delay_minutes)::numeric, 2)
                    AS network_avg_delay_minutes,
                ROUND(MAX(max_delay_seconds) / 60.0, 1)
                    AS worst_delay_minutes,
                COUNT(*) FILTER (
                    WHERE performance_grade = 'A'
                ) AS grade_a_routes,
                COUNT(*) FILTER (
                    WHERE performance_grade = 'F'
                ) AS grade_f_routes,
                COUNT(*) FILTER (
                    WHERE slo_breached = true
                ) AS slo_breached_routes,
                NOW()
            FROM gold.mart_route_performance
            GROUP BY event_date;
        """)

        conn.commit()

        cur.execute("""
            SELECT event_date, active_routes, total_trip_events,
                   network_on_time_rate_pct, worst_delay_minutes,
                   grade_a_routes, grade_f_routes
            FROM gold.mart_delay_summary
            ORDER BY event_date DESC
            LIMIT 5;
        """)

        for row in cur.fetchall():
            logger.info(
                f"  {row[0]} | routes={row[1]} | trips={row[2]:,} | "
                f"on_time={row[3]}% | worst={row[4]}min | "
                f"A={row[5]} F={row[6]}"
            )

    except Exception as e:
        logger.error(
            f"rebuild_gold error: {e}",
            exc_info=True
        )

        try:
            conn.rollback()
        except Exception:
            pass

    finally:
        cur.close()

    logger.info("Running dbt run...")

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "dbt.cli.main",
            "run",
            "--profiles-dir",
            "."
        ],
        cwd=os.path.join(
            os.path.dirname(__file__),
            "..",
            "dbt_transform"
        ),
        capture_output=True,
        text=True,
    )

    if result.returncode == 0:
        logger.info("dbt run completed successfully")
    else:
        logger.warning(
            f"dbt run failed:\n{result.stderr[-500:]}"
        )


def main():
    logger.info("=" * 60)
    logger.info("Pipeline loop starting — FRESH START mode")
    logger.info("=" * 60)

    conn = get_conn()

    ensure_mart_tables(conn)
    fresh_start(conn)
    seek_to_latest()

    logger.info("Startup complete. Entering poll loop...")
    logger.info(f"  Consumer group : {CONSUMER_GROUP}")
    logger.info(f"  Cycle window   : {POLL_DURATION}s")
    logger.info(f"  Batch size     : {BATCH_SIZE:,}")

    cycle = 0

    while True:
        cycle += 1

        logger.info(f"━━━ Cycle {cycle} {'━' * 40}")

        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")

        except Exception:
            logger.warning("DB connection lost — reconnecting...")

            try:
                conn.close()
            except Exception:
                pass

            conn = get_conn()
            ensure_mart_tables(conn)

        loaded = load_from_kafka(conn)

        if loaded > 0:
            rebuild_gold(conn)
        else:
            logger.info(
                "No new trip_updates this cycle — skipping mart rebuild"
            )

        logger.info(
            f"Cycle {cycle} complete. Waiting for next Kafka poll...\n"
        )


if __name__ == "__main__":
    main()