
from pydantic_settings import BaseSettings
from typing import Optional

class GTFSStaticConfig(BaseSettings):
    gtfs_static_url: str = "https://download.gtfs.de/germany/free/latest.zip"
    s3_endpoint_url: str = "https://s3.amazonaws.com"
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_default_region: str = "eu-central-1"

    s3_bucket_static: str = "railpulse-static"
    gtfs_static_request_timeout: int = 120

    model_config = {
        "env_file": ".env",
        "case_sensitive": False,
        "extra": "ignore",
    }
