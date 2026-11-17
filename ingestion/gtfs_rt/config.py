from pydantic_settings import BaseSettings
from typing import Optional


class IngestionConfig(BaseSettings):
    gtfs_rt_url: str
    gtfs_rt_poll_interval: int = 10
    gtfs_rt_api_key: Optional[str] = None
    gtfs_rt_request_timeout: int = 15

    kafka_bootstrap_servers: str
    kafka_topic_raw_events: str = "raw-events"
    kafka_topic_dlq_events: str = "dlq-events"
    schema_registry_url: Optional[str] = None

    metrics_port: int = 8000

    env: str = "development"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False