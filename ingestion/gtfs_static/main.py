import logging
import sys

from .config import GTFSStaticConfig
from .loader import GTFSStaticLoader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    stream=sys.stdout,
)

logger = logging.getLogger("railpulse.gtfs_static")


def main():
    cfg = GTFSStaticConfig()

    loader = GTFSStaticLoader(
        gtfs_url=cfg.gtfs_static_url,
        s3_endpoint=cfg.s3_endpoint_url,
        aws_key=cfg.aws_access_key_id,
        aws_secret=cfg.aws_secret_access_key,
        bucket=cfg.s3_bucket_static,
        request_timeout=cfg.gtfs_static_request_timeout,
    )

    result = loader.run()

    if result["success"]:
        logger.info("GTFS Static load completed successfully")
        for table, info in result["tables"].items():
            logger.info(f"  {table}: {info['rows']:,} rows — {info['status']}")
    else:
        logger.error("GTFS Static load failed")
        sys.exit(1)


if __name__ == "__main__":
    main()