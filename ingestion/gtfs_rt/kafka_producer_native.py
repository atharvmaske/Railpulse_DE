import json
import logging
from typing import Optional
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from .models import GTFSEvent

logger = logging.getLogger(__name__)


class GTFSKafkaProducerNative:
    def __init__(
        self,
        bootstrap_servers: str,
        raw_topic: str = "raw-events",
        dlq_topic: str = "dlq-events",
        schema_registry_url: Optional[str] = None,
    ):
        self.raw_topic = raw_topic
        self.dlq_topic = dlq_topic
        self.bootstrap_servers = bootstrap_servers

        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            acks="all",
            retries=5,
            compression_type=None,
            linger_ms=50,
            batch_size=65536,
            value_serializer=lambda v: v if isinstance(v, bytes) else v.encode("utf-8"),
        )
        logger.info(f"Native Kafka producer connected to {bootstrap_servers}")

    def ensure_topics(self, bootstrap_servers: str) -> None:
        try:
            admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
            topics = [
                NewTopic(self.raw_topic, num_partitions=6, replication_factor=1),
                NewTopic(self.dlq_topic, num_partitions=2, replication_factor=1),
            ]
            admin.create_topics(topics)
            logger.info("Topics ensured")
        except TopicAlreadyExistsError:
            logger.debug("Topics already exist")
        except Exception as e:
            logger.debug(f"Topic creation: {e}")

    def publish_event(self, event: GTFSEvent) -> None:
        payload = event.model_dump_json().encode("utf-8")
        key = (
            event.trip_update.trip_id if event.trip_update
            else event.vehicle_position.vehicle_id if event.vehicle_position
            else event.service_alert.alert_id if event.service_alert
            else event.event_id
        ).encode("utf-8")

        self._producer.send(
            topic=self.raw_topic,
            key=key,
            value=payload,
        )

    def publish_dlq(
        self,
        raw_bytes: Optional[bytes],
        error_msg: str,
        entity_id: str = "",
    ) -> None:
        dlq_payload = json.dumps({
            "entity_id": entity_id,
            "error": error_msg,
            "raw_size_bytes": len(raw_bytes) if raw_bytes else 0,
        }).encode("utf-8")

        self._producer.send(
            topic=self.dlq_topic,
            value=dlq_payload,
        )

    def flush(self, timeout: float = 10.0) -> None:
        self._producer.flush(timeout=timeout)