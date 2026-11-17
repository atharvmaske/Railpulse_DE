import json
import logging
from typing import Optional

from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from .models import GTFSEvent

logger = logging.getLogger(__name__)


class GTFSKafkaProducer:
    def __init__(
        self,
        bootstrap_servers: str,
        raw_topic: str = "raw-events",
        dlq_topic: str = "dlq-events",
        schema_registry_url: Optional[str] = None,
    ):
        self.raw_topic = raw_topic
        self.dlq_topic = dlq_topic
        self.schema_registry_url = schema_registry_url

        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "retries": 5,
            "retry.backoff.ms": 500,
            "compression.type": "snappy",
            "linger.ms": 50,
            "batch.size": 65536,
        })

        logger.info(f"Kafka producer connected to {bootstrap_servers}")

    def ensure_topics(self, bootstrap_servers: str) -> None:
        admin = AdminClient({"bootstrap.servers": bootstrap_servers})
        topics = [
            NewTopic(self.raw_topic, num_partitions=6, replication_factor=1),
            NewTopic(self.dlq_topic, num_partitions=2, replication_factor=1),
        ]
        fs = admin.create_topics(topics)
        for topic, future in fs.items():
            try:
                future.result()
                logger.info(f"Topic created: {topic}")
            except Exception as e:
                logger.debug(f"Topic {topic}: {e}")

    def publish_event(self, event: GTFSEvent) -> None:
        payload = event.model_dump_json().encode("utf-8")
        key = (
            event.trip_update.trip_id if event.trip_update
            else event.vehicle_position.vehicle_id if event.vehicle_position
            else event.service_alert.alert_id if event.service_alert
            else event.event_id
        ).encode("utf-8")

        self._producer.produce(
            topic=self.raw_topic,
            key=key,
            value=payload,
            on_delivery=self._delivery_callback,
        )
        self._producer.poll(0)

    def publish_dlq(self, raw_bytes: Optional[bytes], error_msg: str, entity_id: str = "") -> None:
        dlq_payload = json.dumps({
            "entity_id": entity_id,
            "error": error_msg,
            "raw_size_bytes": len(raw_bytes) if raw_bytes else 0,
        }).encode("utf-8")

        self._producer.produce(
            topic=self.dlq_topic,
            value=dlq_payload,
            on_delivery=self._delivery_callback,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> None:
        remaining = self._producer.flush(timeout)
        if isinstance(remaining, int) and remaining > 0:
            logger.warning(f"{remaining} messages still in queue after flush timeout")

    @staticmethod
    def _delivery_callback(err, msg) -> None:
        if err:
            logger.error(f"Delivery failed for {msg.topic()}/{msg.partition()}: {err}")
        else:
            logger.debug(
                f"Delivered to {msg.topic()}[{msg.partition()}] "
                f"offset={msg.offset()} key={msg.key()}"
            )