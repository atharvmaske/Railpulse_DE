import logging
import time
from typing import Optional

import requests

from .kafka_producer import GTFSKafkaProducer
from .parser import GTFSRealtimeParser
from .metrics import IngestionMetrics

logger = logging.getLogger(__name__)


class GTFSRealtimePoller:
    def __init__(
        self,
        gtfs_rt_url: str,
        producer: GTFSKafkaProducer,
        poll_interval: int = 10,
        request_timeout: int = 15,
        api_key: Optional[str] = None,
    ):
        self.gtfs_rt_url = gtfs_rt_url
        self.producer = producer
        self.poll_interval = poll_interval
        self.request_timeout = request_timeout
        self.parser = GTFSRealtimeParser()
        self.metrics = IngestionMetrics()

        self._headers = {}
        if api_key:
            self._headers["Authorization"] = f"Bearer {api_key}"

    def run_forever(self) -> None:
        logger.info(
            f"Starting GTFS-RT poller | url={self.gtfs_rt_url} "
            f"interval={self.poll_interval}s"
        )
        while True:
            start = time.monotonic()
            try:
                self._poll_once()
            except KeyboardInterrupt:
                logger.info("Poller interrupted — shutting down")
                self.producer.flush()
                break
            except Exception as e:
                logger.error(f"Unexpected error in polling loop: {e}", exc_info=True)
                self.metrics.inc_fetch_errors()

            elapsed = time.monotonic() - start
            sleep_for = max(0.0, self.poll_interval - elapsed)
            logger.debug(f"Cycle done in {elapsed:.2f}s, sleeping {sleep_for:.2f}s")
            time.sleep(sleep_for)

    def poll_once(self) -> dict:
        return self._poll_once()

    def _poll_once(self) -> dict:
        raw_bytes = self._fetch()
        if raw_bytes is None:
            return {"fetched": False, "published": 0, "dlq": 0}

        published = 0
        dlq_count = 0

        for event, is_valid, error in self.parser.parse(raw_bytes):
            if is_valid and event is not None:
                self.producer.publish_event(event)
                self.metrics.inc_published(event.event_type.value)
                published += 1
            else:
                self.producer.publish_dlq(
                    raw_bytes=raw_bytes,
                    error_msg=error,
                )
                self.metrics.inc_dlq()
                dlq_count += 1

        self.producer.flush(timeout=5.0)

        logger.info(f"Poll done | published={published} dlq={dlq_count}")
        return {"fetched": True, "published": published, "dlq": dlq_count}

    def _fetch(self) -> Optional[bytes]:
        try:
            resp = requests.get(
                self.gtfs_rt_url,
                headers=self._headers,
                timeout=self.request_timeout,
            )
            resp.raise_for_status()
            self.metrics.observe_fetch_latency(resp.elapsed.total_seconds())
            return resp.content
        except requests.exceptions.Timeout:
            logger.warning(f"GTFS-RT fetch timed out after {self.request_timeout}s")
            self.metrics.inc_fetch_errors()
        except requests.exceptions.HTTPError as e:
            logger.error(f"GTFS-RT HTTP error: {e.response.status_code} {e}")
            self.metrics.inc_fetch_errors()
        except requests.exceptions.RequestException as e:
            logger.error(f"GTFS-RT request failed: {e}")
            self.metrics.inc_fetch_errors()
        return None