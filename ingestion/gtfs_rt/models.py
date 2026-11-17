from datetime import datetime
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field


class EventType(str, Enum):
    TRIP_UPDATE = "trip_update"
    VEHICLE_POSITION = "vehicle_position"
    SERVICE_ALERT = "service_alert"


class TripUpdate(BaseModel):
    trip_id: str
    route_id: Optional[str] = None
    direction_id: Optional[int] = None
    start_time: Optional[str] = None
    start_date: Optional[str] = None
    schedule_relationship: Optional[str] = None
    delay_seconds: Optional[int] = None
    stop_sequence: Optional[int] = None
    stop_id: Optional[str] = None


class VehiclePosition(BaseModel):
    vehicle_id: str
    trip_id: Optional[str] = None
    route_id: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    bearing: Optional[float] = None
    speed: Optional[float] = None
    current_stop_sequence: Optional[int] = None
    current_status: Optional[str] = None


class ServiceAlert(BaseModel):
    alert_id: str
    cause: Optional[str] = None
    effect: Optional[str] = None
    header_text: Optional[str] = None
    description_text: Optional[str] = None
    informed_entity_route_id: Optional[str] = None
    informed_entity_trip_id: Optional[str] = None


class GTFSEvent(BaseModel):
    event_id: str = Field(..., description="Unique event UUID")
    event_type: EventType
    source: str = Field(default="gtfs-rt", description="Data source identifier")
    version: str = Field(default="2.0", description="GTFS-RT version")
    ingested_at: datetime = Field(default_factory=datetime.utcnow)
    replayed: bool = Field(default=False)

    trip_update: Optional[TripUpdate] = None
    vehicle_position: Optional[VehiclePosition] = None
    service_alert: Optional[ServiceAlert] = None

    model_config = {"json_encoders": {datetime: lambda v: v.isoformat()}}