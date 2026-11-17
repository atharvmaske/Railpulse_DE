import logging
import uuid
from datetime import datetime, timezone
from typing import Generator

from google.transit import gtfs_realtime_pb2

from .models import (
    EventType, GTFSEvent,
    TripUpdate, VehiclePosition, ServiceAlert,
)

logger = logging.getLogger(__name__)


class GTFSRealtimeParser:
    def parse(self, raw_bytes: bytes) -> Generator[tuple[GTFSEvent | None, bool, str], None, None]:
        feed = gtfs_realtime_pb2.FeedMessage()
        try:
            feed.ParseFromString(raw_bytes)
        except Exception as e:
            logger.error(f"Failed to parse FeedMessage protobuf: {e}")
            yield None, False, str(e)
            return

        for entity in feed.entity:
            try:
                event = self._parse_entity(entity)
                yield event, True, ""
            except Exception as e:
                logger.warning(f"Failed to parse entity {entity.id}: {e}")
                yield None, False, f"entity_id={entity.id} error={e}"

    def _parse_entity(self, entity) -> GTFSEvent:
        event_id = str(uuid.uuid4())
        ingested_at = datetime.now(timezone.utc)

        if entity.HasField("trip_update"):
            tu = entity.trip_update
            trip = tu.trip
            stop_time_update = tu.stop_time_update[0] if tu.stop_time_update else None

            return GTFSEvent(
                event_id=event_id,
                event_type=EventType.TRIP_UPDATE,
                ingested_at=ingested_at,
                trip_update=TripUpdate(
                    trip_id=trip.trip_id,
                    route_id=trip.route_id or None,
                    direction_id=trip.direction_id if trip.HasField("direction_id") else None,
                    start_time=trip.start_time or None,
                    start_date=trip.start_date or None,
                    delay_seconds=(
                        stop_time_update.arrival.delay
                        if stop_time_update and stop_time_update.HasField("arrival")
                        else None
                    ),
                    stop_sequence=stop_time_update.stop_sequence if stop_time_update else None,
                    stop_id=stop_time_update.stop_id if stop_time_update else None,
                ),
            )

        elif entity.HasField("vehicle"):
            vp = entity.vehicle
            pos = vp.position

            return GTFSEvent(
                event_id=event_id,
                event_type=EventType.VEHICLE_POSITION,
                ingested_at=ingested_at,
                vehicle_position=VehiclePosition(
                    vehicle_id=vp.vehicle.id if vp.HasField("vehicle") else entity.id,
                    trip_id=vp.trip.trip_id if vp.HasField("trip") else None,
                    route_id=vp.trip.route_id if vp.HasField("trip") else None,
                    latitude=pos.latitude if vp.HasField("position") else None,
                    longitude=pos.longitude if vp.HasField("position") else None,
                    bearing=pos.bearing if vp.HasField("position") else None,
                    speed=pos.speed if vp.HasField("position") else None,
                    current_stop_sequence=vp.current_stop_sequence or None,
                    current_status=str(vp.current_status) if vp.current_status else None,
                ),
            )

        elif entity.HasField("alert"):
            alert = entity.alert
            header = (
                alert.header_text.translation[0].text
                if alert.header_text.translation else None
            )
            desc = (
                alert.description_text.translation[0].text
                if alert.description_text.translation else None
            )
            informed = alert.informed_entity[0] if alert.informed_entity else None

            return GTFSEvent(
                event_id=event_id,
                event_type=EventType.SERVICE_ALERT,
                ingested_at=ingested_at,
                service_alert=ServiceAlert(
                    alert_id=entity.id,
                    cause=str(alert.cause) if alert.cause else None,
                    effect=str(alert.effect) if alert.effect else None,
                    header_text=header,
                    description_text=desc,
                    informed_entity_route_id=informed.route_id if informed else None,
                    informed_entity_trip_id=(
                        informed.trip.trip_id
                        if informed and informed.HasField("trip") else None
                    ),
                ),
            )

        else:
            raise ValueError(f"Unknown entity type for entity_id={entity.id}")