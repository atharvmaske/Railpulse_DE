# ingestion/tests/test_parser.py
import pytest
from google.transit import gtfs_realtime_pb2

from ingestion.gtfs_rt.parser import GTFSRealtimeParser
from ingestion.gtfs_rt.models import EventType
from unittest.mock import patch, MagicMock



def _make_trip_update_feed() -> bytes:
    """Build a minimal GTFS-RT FeedMessage with one TripUpdate."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1700000000

    entity = feed.entity.add()
    entity.id = "test-entity-1"

    tu = entity.trip_update
    tu.trip.trip_id = "ICE-123"
    tu.trip.route_id = "ICE"
    tu.trip.start_date = "20241101"

    stu = tu.stop_time_update.add()
    stu.stop_sequence = 5
    stu.stop_id = "FRA"
    stu.arrival.delay = 180  # 3 min late

    return feed.SerializeToString()


def _make_vehicle_position_feed() -> bytes:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1700000001

    entity = feed.entity.add()
    entity.id = "veh-1"

    vp = entity.vehicle
    vp.vehicle.id = "train-456"
    vp.trip.trip_id = "ICE-123"
    vp.position.latitude = 50.1109
    vp.position.longitude = 8.6821
    vp.position.speed = 180.0

    return feed.SerializeToString()


class TestGTFSRealtimeParser:

    def setup_method(self):
        self.parser = GTFSRealtimeParser()

    def test_parse_trip_update(self):
        raw = _make_trip_update_feed()
        results = list(self.parser.parse(raw))

        assert len(results) == 1
        event, is_valid, err = results[0]
        assert is_valid is True
        assert err == ""
        assert event.event_type == EventType.TRIP_UPDATE
        assert event.trip_update.trip_id == "ICE-123"
        assert event.trip_update.delay_seconds == 180
        assert event.trip_update.stop_id == "FRA"

    def test_parse_vehicle_position(self):
        raw = _make_vehicle_position_feed()
        results = list(self.parser.parse(raw))

        assert len(results) == 1
        event, is_valid, err = results[0]
        assert is_valid is True
        assert event.event_type == EventType.VEHICLE_POSITION
        assert event.vehicle_position.vehicle_id == "train-456"
        assert event.vehicle_position.latitude == pytest.approx(50.1109)

    def test_invalid_bytes_yields_error(self):
        results = list(self.parser.parse(b"not-a-protobuf"))
        assert len(results) == 1
        _, is_valid, err = results[0]
        assert is_valid is False
        assert err != ""

    def test_event_has_uuid_and_timestamp(self):
        raw = _make_trip_update_feed()
        event, _, _ = list(self.parser.parse(raw))[0]
        assert len(event.event_id) == 36  # UUID format
        assert event.ingested_at is not None
        assert event.source == "gtfs-rt"