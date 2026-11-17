
import logging
import sys
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("railpulse.ingestion")


def main():
    from ingestion.gtfs_rt.kafka_producer_native import GTFSKafkaProducerNative
    from ingestion.gtfs_rt.poller import GTFSRealtimePoller
    from ingestion.gtfs_rt.metrics import IngestionMetrics

    gtfs_rt_url = os.getenv("GTFS_RT_URL", "https://realtime.gtfs.de/realtime-free.pb")
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    raw_topic     = os.getenv("KAFKA_TOPIC_RAW_EVENTS", "raw-events")
    dlq_topic     = os.getenv("KAFKA_TOPIC_DLQ_EVENTS", "dlq-events")
    poll_interval = int(os.getenv("GTFS_RT_POLL_INTERVAL", "30"))
    metrics_port  = int(os.getenv("METRICS_PORT", "8000"))

    logger.info(f"Starting RailPulse Ingestion")
    logger.info(f"GTFS-RT URL : {gtfs_rt_url}")
    logger.info(f"Kafka       : {kafka_servers}")
    logger.info(f"Poll every  : {poll_interval}s")

    IngestionMetrics.start_metrics_server(port=metrics_port)

    producer = GTFSKafkaProducerNative(
        bootstrap_servers=kafka_servers,
        raw_topic=raw_topic,
        dlq_topic=dlq_topic,
    )
    producer.ensure_topics(kafka_servers)

    poller = GTFSRealtimePoller(
        gtfs_rt_url=gtfs_rt_url,
        producer=producer,
        poll_interval=poll_interval,
        request_timeout=30,
    )
    poller.run_forever()


if __name__ == "__main__":
    main()