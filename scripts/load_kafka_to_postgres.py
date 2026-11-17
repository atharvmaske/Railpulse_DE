import json
import logging
import os
import sys
from datetime import datetime, timezone

import psycopg2
from kafka import KafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    stream=sys.stdout,
)

logger = logging.getLogger(__name__)



def get_pg_conn():
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


def main():
    logger.info("Starting Kafka → PostgreSQL direct loader")

    consumer = KafkaConsumer(
        "raw-events",
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="direct-loader",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        consumer_timeout_ms=70000,
    )

    conn = get_pg_conn()
    cur = conn.cursor()

    trip_updates = []
    vehicle_positions = []
    total = 0

    logger.info("Reading from Kafka raw-events (will stop after 15s silence)...")

    for msg in consumer:
        event = msg.value
        total += 1

        if total % 10000 == 0:
            logger.info(f"Read {total:,} events so far...")

        event_type = event.get("event_type")
        ingested_at = event.get(
            "ingested_at",
            datetime.now(timezone.utc).isoformat()
        )

        if event_type == "trip_update":
            tu = event.get("trip_update") or {}
            trip_updates.append((
                event.get("event_id"),
                tu.get("trip_id"),
                tu.get("route_id"),
                tu.get("stop_id"),
                tu.get("delay_seconds"),
                ingested_at[:10],
                ingested_at,
            ))

        elif event_type == "vehicle_position":
            vp = event.get("vehicle_position") or {}
            vehicle_positions.append((
                event.get("event_id"),
                vp.get("vehicle_id"),
                vp.get("trip_id"),
                vp.get("route_id"),
                vp.get("latitude"),
                vp.get("longitude"),
                vp.get("speed"),
                ingested_at,
            ))

        if len(trip_updates) >= 5000:
            _insert_trip_updates(cur, trip_updates)
            conn.commit()
            logger.info(f"Inserted {len(trip_updates):,} trip updates")
            trip_updates = []

        if len(vehicle_positions) >= 5000:
            _insert_vehicle_positions(cur, vehicle_positions)
            conn.commit()
            vehicle_positions = []

    if trip_updates:
        _insert_trip_updates(cur, trip_updates)
        conn.commit()

    if vehicle_positions:
        _insert_vehicle_positions(cur, vehicle_positions)
        conn.commit()

    consumer.close()
    cur.close()
    conn.close()

    logger.info(f"Done! Total events read: {total:,}")
    logger.info("Trip updates loaded: check gold.trip_delays")
    logger.info("Now run: cd dbt_transform && dbt run --profiles-dir .")


def _insert_trip_updates(cur, rows):
    cur.executemany("""
        INSERT INTO gold.trip_delays
            (id, trip_id, route_id, stop_id, delay_seconds, event_date, ingested_at)
        VALUES
            (gen_random_uuid(), %s, %s, %s, %s, %s::date, %s::timestamptz)
        ON CONFLICT DO NOTHING
    """, [(r[1], r[2], r[3], r[4], r[5], r[6]) for r in rows])


def _insert_vehicle_positions(cur, rows):
    cur.executemany("""
        INSERT INTO gold.vehicle_positions
            (id, vehicle_id, trip_id, route_id, latitude, longitude, speed, recorded_at)
        VALUES
            (gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s::timestamptz)
        ON CONFLICT DO NOTHING
    """, [(r[1], r[2], r[3], r[4], r[5], r[6], r[7]) for r in rows])


if __name__ == "__main__":
    main()