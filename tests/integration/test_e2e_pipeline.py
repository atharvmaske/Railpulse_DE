import json
import uuid
import pytest
from datetime import datetime, timezone
from unittest.mock import patch


pytestmark = pytest.mark.integration


def make_trip_update_event(
    trip_id: str = "ICE-101",
    route_id: str = "ICE",
    delay_seconds: int = 120,
    stop_id: str = "FRA",
) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "trip_update",
        "source": "gtfs-rt",
        "version": "2.0",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "replayed": False,
        "trip_update": {
            "trip_id": trip_id,
            "route_id": route_id,
            "direction_id": 0,
            "start_date": "20241101",
            "start_time": "08:00:00",
            "schedule_relationship": None,
            "delay_seconds": delay_seconds,
            "stop_sequence": 3,
            "stop_id": stop_id,
        },
        "vehicle_position": None,
        "service_alert": None,
    }


def make_vehicle_position_event(
    vehicle_id: str = "train-001",
    trip_id: str = "ICE-101",
    lat: float = 50.1109,
    lon: float = 8.6821,
    speed: float = 250.0,
) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "vehicle_position",
        "source": "gtfs-rt",
        "version": "2.0",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "replayed": False,
        "trip_update": None,
        "vehicle_position": {
            "vehicle_id": vehicle_id,
            "trip_id": trip_id,
            "route_id": "ICE",
            "latitude": lat,
            "longitude": lon,
            "bearing": 90.0,
            "speed": speed,
            "current_stop_sequence": 3,
            "current_status": "IN_TRANSIT_TO",
        },
        "service_alert": None,
    }


def make_service_alert_event(
    alert_id: str = "alert-001",
    route_id: str = "ICE",
) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "service_alert",
        "source": "gtfs-rt",
        "version": "2.0",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "replayed": False,
        "trip_update": None,
        "vehicle_position": None,
        "service_alert": {
            "alert_id": alert_id,
            "cause": "TECHNICAL_PROBLEM",
            "effect": "DELAY",
            "header_text": "Signal failure near Frankfurt",
            "description_text": "Delays expected on ICE routes",
            "informed_entity_route_id": route_id,
            "informed_entity_trip_id": None,
        },
    }


class TestEventStructure:

    def test_trip_update_has_required_fields(self):
        event = make_trip_update_event()
        assert event["event_id"]
        assert event["event_type"] == "trip_update"
        assert event["trip_update"]["trip_id"] == "ICE-101"
        assert event["trip_update"]["delay_seconds"] == 120
        assert event["vehicle_position"] is None
        assert event["service_alert"] is None

    def test_vehicle_position_has_required_fields(self):
        event = make_vehicle_position_event()
        assert event["event_type"] == "vehicle_position"
        assert event["vehicle_position"]["latitude"] == pytest.approx(50.1109)
        assert event["vehicle_position"]["speed"] == 250.0
        assert event["trip_update"] is None

    def test_service_alert_has_required_fields(self):
        event = make_service_alert_event()
        assert event["event_type"] == "service_alert"
        assert event["service_alert"]["cause"] == "TECHNICAL_PROBLEM"
        assert event["service_alert"]["effect"] == "DELAY"

    def test_all_events_have_uuid(self):
        events = [
            make_trip_update_event(),
            make_vehicle_position_event(),
            make_service_alert_event(),
        ]
        ids = [e["event_id"] for e in events]
        assert len(set(ids)) == 3

        for eid in ids:
            assert len(eid) == 36
            assert eid.count("-") == 4

    def test_events_serialize_to_json(self):
        for event in [
            make_trip_update_event(),
            make_vehicle_position_event(),
            make_service_alert_event(),
        ]:
            serialized = json.dumps(event)
            deserialized = json.loads(serialized)
            assert deserialized["event_id"] == event["event_id"]


class TestKafkaProducerIntegration:

    def _make_producer(self):
        import sys
        from unittest.mock import MagicMock

        mock_kafka = MagicMock()
        mock_producer_instance = MagicMock()
        mock_kafka.Producer.return_value = mock_producer_instance
        mock_kafka.KafkaException = Exception

        with patch.dict(sys.modules, {
            "confluent_kafka": mock_kafka,
            "confluent_kafka.admin": MagicMock(),
        }):
            if "ingestion.gtfs_rt.kafka_producer" in sys.modules:
                del sys.modules["ingestion.gtfs_rt.kafka_producer"]

            from ingestion.gtfs_rt.kafka_producer import GTFSKafkaProducer

            producer = GTFSKafkaProducer(
                bootstrap_servers="localhost:9092",
                raw_topic="raw-events",
                dlq_topic="dlq-events",
            )
            producer._producer = mock_producer_instance

            return producer, mock_producer_instance

    def test_valid_event_goes_to_raw_topic(self):
        from ingestion.gtfs_rt.models import GTFSEvent, EventType, TripUpdate

        producer, mock_prod = self._make_producer()

        event = GTFSEvent(
            event_id="test-uuid-001",
            event_type=EventType.TRIP_UPDATE,
            ingested_at=datetime.now(timezone.utc),
            trip_update=TripUpdate(trip_id="ICE-101", route_id="ICE"),
        )

        producer.publish_event(event)

        mock_prod.produce.assert_called_once()
        call_kwargs = mock_prod.produce.call_args[1]

        assert call_kwargs["topic"] == "raw-events"

        payload = json.loads(call_kwargs["value"].decode())
        assert payload["event_id"] == "test-uuid-001"
        assert payload["event_type"] == "trip_update"

    def test_invalid_event_goes_to_dlq(self):
        producer, mock_prod = self._make_producer()

        producer.publish_dlq(
            raw_bytes=b"bad-data",
            error_msg="schema validation failed",
            entity_id="entity-123",
        )

        mock_prod.produce.assert_called_once()
        call_kwargs = mock_prod.produce.call_args[1]

        assert call_kwargs["topic"] == "dlq-events"

    def test_dlq_payload_contains_error_info(self):
        producer, mock_prod = self._make_producer()

        producer.publish_dlq(
            raw_bytes=b"bad",
            error_msg="missing trip_id",
            entity_id="ent-999",
        )

        call_kwargs = mock_prod.produce.call_args[1]
        payload = json.loads(call_kwargs["value"].decode())

        assert payload["error"] == "missing trip_id"
        assert payload["entity_id"] == "ent-999"
        assert payload["raw_size_bytes"] == 3


class TestPollerIntegration:

    def _make_gtfs_rt_bytes(self) -> bytes:
        from google.transit import gtfs_realtime_pb2

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"
        feed.header.timestamp = int(datetime.now(timezone.utc).timestamp())

        e1 = feed.entity.add()
        e1.id = "tu-1"
        e1.trip_update.trip.trip_id = "ICE-101"
        e1.trip_update.trip.route_id = "ICE"

        stu = e1.trip_update.stop_time_update.add()
        stu.stop_sequence = 3
        stu.stop_id = "FRA"
        stu.arrival.delay = 120

        e2 = feed.entity.add()
        e2.id = "vp-1"
        e2.vehicle.vehicle.id = "train-001"
        e2.vehicle.trip.trip_id = "ICE-101"
        e2.vehicle.position.latitude = 50.1109
        e2.vehicle.position.longitude = 8.6821
        e2.vehicle.position.speed = 250.0

        return feed.SerializeToString()

    def _make_poller(self):
        import sys
        from unittest.mock import MagicMock

        mock_kafka = MagicMock()
        mock_producer_instance = MagicMock()
        mock_kafka.Producer.return_value = mock_producer_instance
        mock_kafka.KafkaException = Exception

        with patch.dict(sys.modules, {
            "confluent_kafka": mock_kafka,
            "confluent_kafka.admin": MagicMock(),
        }):
            for mod in [
                "ingestion.gtfs_rt.kafka_producer",
                "ingestion.gtfs_rt.poller",
            ]:
                if mod in sys.modules:
                    del sys.modules[mod]

            from ingestion.gtfs_rt.kafka_producer import GTFSKafkaProducer
            from ingestion.gtfs_rt.poller import GTFSRealtimePoller

            producer = GTFSKafkaProducer("localhost:9092")
            producer._producer = mock_producer_instance
            mock_producer_instance.flush.return_value = 0

            poller = GTFSRealtimePoller(
                gtfs_rt_url="http://mock-gtfs-rt/feed",
                producer=producer,
                poll_interval=10,
            )

            return poller, mock_producer_instance

    def test_full_poll_cycle(self):
        poller, mock_prod = self._make_poller()
        gtfs_bytes = self._make_gtfs_rt_bytes()

        with patch.object(poller, "_fetch", return_value=gtfs_bytes):
            result = poller.poll_once()

        assert result["fetched"] is True
        assert result["published"] == 2
        assert result["dlq"] == 0
        assert mock_prod.produce.call_count == 2

    def test_fetch_failure_returns_no_data(self):
        poller, mock_prod = self._make_poller()

        with patch.object(poller, "_fetch", return_value=None):
            result = poller.poll_once()

        assert result["fetched"] is False
        assert result["published"] == 0
        assert mock_prod.produce.call_count == 0

    def test_invalid_entity_goes_to_dlq(self):
        from google.transit import gtfs_realtime_pb2

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"
        feed.header.timestamp = 1700000000

        entity = feed.entity.add()
        entity.id = "empty-entity"

        bad_bytes = feed.SerializeToString()

        poller, mock_prod = self._make_poller()

        with patch.object(poller, "_fetch", return_value=bad_bytes):
            result = poller.poll_once()

        assert result["dlq"] == 1
        assert result["published"] == 0


class TestDataQuality:

    def test_delay_categories(self):
        def classify(delay):
            if delay is None:
                return "unknown"
            if delay <= 0:
                return "on_time"
            if delay <= 60:
                return "minor"
            if delay <= 300:
                return "moderate"
            return "severe"

        assert classify(None) == "unknown"
        assert classify(0) == "on_time"
        assert classify(-30) == "on_time"
        assert classify(60) == "minor"
        assert classify(61) == "moderate"
        assert classify(300) == "moderate"
        assert classify(301) == "severe"
        assert classify(900) == "severe"

    def test_germany_geo_bounds(self):
        def in_germany(lat, lon):
            return 47.0 <= lat <= 55.5 and 5.5 <= lon <= 15.5

        assert in_germany(50.1109, 8.6821)
        assert in_germany(48.1402, 11.5600)
        assert in_germany(52.5200, 13.4050)
        assert in_germany(53.5753, 10.0153)

        assert not in_germany(51.5074, -0.1278)
        assert not in_germany(48.8566, 2.3522)
        assert not in_germany(0.0, 0.0)

    def test_on_time_rate_bounds(self):
        def calc_on_time_rate(on_time, total):
            if total == 0:
                return None
            return round(100.0 * on_time / total, 2)

        assert calc_on_time_rate(0, 0) is None
        assert calc_on_time_rate(100, 100) == 100.0
        assert calc_on_time_rate(0, 100) == 0.0
        assert calc_on_time_rate(75, 100) == 75.0
        assert 0 <= calc_on_time_rate(50, 100) <= 100

    def test_performance_grade_mapping(self):
        def grade(rate):
            if rate >= 95:
                return "A"
            if rate >= 85:
                return "B"
            if rate >= 75:
                return "C"
            if rate >= 60:
                return "D"
            return "F"

        assert grade(100) == "A"
        assert grade(95) == "A"
        assert grade(94.9) == "B"
        assert grade(85) == "B"
        assert grade(75) == "C"
        assert grade(60) == "D"
        assert grade(59.9) == "F"
        assert grade(0) == "F"
