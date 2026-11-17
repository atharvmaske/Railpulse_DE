from prometheus_client import Counter, Histogram, start_http_server
import logging

logger = logging.getLogger(__name__)


class IngestionMetrics:
    def __init__(self):
        self.events_published = Counter(
            "railpulse_ingestion_events_published_total",
            "Total events successfully published to Kafka",
            ["event_type"],
        )
        self.events_dlq = Counter(
            "railpulse_ingestion_events_dlq_total",
            "Total events sent to DLQ",
        )
        self.fetch_errors = Counter(
            "railpulse_ingestion_fetch_errors_total",
            "Total GTFS-RT fetch failures",
        )
        self.fetch_latency = Histogram(
            "railpulse_ingestion_fetch_latency_seconds",
            "GTFS-RT HTTP fetch latency",
            buckets=[0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0],
        )

    def inc_published(self, event_type: str) -> None:
        self.events_published.labels(event_type=event_type).inc()

    def inc_dlq(self) -> None:
        self.events_dlq.inc()

    def inc_fetch_errors(self) -> None:
        self.fetch_errors.inc()

    def observe_fetch_latency(self, seconds: float) -> None:
        self.fetch_latency.observe(seconds)

    @staticmethod
    def start_metrics_server(port: int = 8000) -> None:
        start_http_server(port)
        logger.info(f"Prometheus metrics server started on port {port}")