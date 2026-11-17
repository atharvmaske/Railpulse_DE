import logging
import sys

from ingestion.gtfs_rt.config import IngestionConfig
from ingestion.gtfs_rt.kafka_producer import GTFSKafkaProducer
from ingestion.gtfs_rt.metrics import IngestionMetrics
from ingestion.gtfs_rt.poller import GTFSRealtimePoller

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("railpulse.ingestion")


def main() -> None:
    cfg = IngestionConfig()
    logger.info(f"Starting RailPulse Ingestion Service | env={cfg.env}")
    logger.info(f"GTFS-RT URL: {cfg.gtfs_rt_url}")
    logger.info(f"Kafka: {cfg.kafka_bootstrap_servers}")

    IngestionMetrics.start_metrics_server(port=cfg.metrics_port)

    producer = GTFSKafkaProducer(
        bootstrap_servers=cfg.kafka_bootstrap_servers,
        raw_topic=cfg.kafka_topic_raw_events,
        dlq_topic=cfg.kafka_topic_dlq_events,
        schema_registry_url=cfg.schema_registry_url,
    )
    producer.ensure_topics(cfg.kafka_bootstrap_servers)

    poller = GTFSRealtimePoller(
        gtfs_rt_url=cfg.gtfs_rt_url,
        producer=producer,
        poll_interval=cfg.gtfs_rt_poll_interval,
        request_timeout=cfg.gtfs_rt_request_timeout,
        api_key=cfg.gtfs_rt_api_key,
    )
    poller.run_forever()


if __name__ == "__main__":
    main()