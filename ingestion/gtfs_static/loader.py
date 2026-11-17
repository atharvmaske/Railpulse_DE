import io
import logging
import zipfile

from datetime import datetime, timezone
from typing import Optional, Union

import boto3
import pandas as pd
import requests

logger = logging.getLogger(__name__)


GTFS_FILES = {
    "routes.txt": {
        "usecols": [
            "route_id",
            "route_short_name",
            "route_long_name",
            "route_type",
        ],
        "dtype": {
            "route_id": str,
            "route_short_name": str,
            "route_type": int,
        },
        "table": "routes",
    },
    "stops.txt": {
        "usecols": [
            "stop_id",
            "stop_name",
            "stop_lat",
            "stop_lon",
        ],
        "dtype": {
            "stop_id": str,
            "stop_name": str,
        },
        "table": "stops",
    },
    "trips.txt": {
        "usecols": [
            "trip_id",
            "route_id",
            "service_id",
            "direction_id",
            "trip_headsign",
        ],
        "required_cols": [
            "trip_id",
            "route_id",
            "service_id",
        ],
        "optional_cols": [
            "direction_id",
            "trip_headsign",
        ],
        "dtype": {
            "trip_id": str,
            "route_id": str,
            "service_id": str,
        },
        "table": "trips",
    },
    "stop_times.txt": {
        "usecols": [
            "trip_id",
            "stop_id",
            "arrival_time",
            "departure_time",
            "stop_sequence",
        ],
        "dtype": {
            "trip_id": str,
            "stop_id": str,
            "stop_sequence": int,
        },
        "table": "stop_times",
        "chunksize": 500_000,
    },
    "calendar.txt": {
        "usecols": [
            "service_id",
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
            "sunday",
            "start_date",
            "end_date",
        ],
        "dtype": {
            "service_id": str,
        },
        "table": "calendar",
    },
}


class GTFSStaticLoader:

    def __init__(
        self,
        gtfs_url: str,
        s3_endpoint: Optional[str],
        aws_key: str,
        aws_secret: str,
        bucket: str = "railpulse-static",
        request_timeout: int = 120,
        region: str = "ap-southeast-2",
    ):
        self.gtfs_url = gtfs_url
        self.bucket = bucket
        self.request_timeout = request_timeout
        self.region = region

        s3_config = {
            "region_name": region,
            "aws_access_key_id": aws_key,
            "aws_secret_access_key": aws_secret,
        }

        if s3_endpoint and s3_endpoint.strip():
            s3_config["endpoint_url"] = s3_endpoint.strip()

        self.s3 = boto3.client("s3", **s3_config)

        logger.info(
            "S3 client configured: bucket=%s region=%s endpoint=%s",
            bucket,
            region,
            s3_endpoint or "AWS default endpoint",
        )

    def run(self, date_str: Optional[str] = None) -> dict:

        if date_str is None:
            date_str = datetime.now(
                timezone.utc
            ).strftime("%Y-%m-%d")

        logger.info(
            "Starting GTFS Static load for date=%s",
            date_str,
        )

        raw_bytes = self._download()

        if raw_bytes is None:
            return {"success": False, "tables": {}}

        results = {}

        with zipfile.ZipFile(io.BytesIO(raw_bytes)) as zf:

            available = zf.namelist()

            logger.info(
                "GTFS zip contains: %s",
                available,
            )

            for filename, cfg in GTFS_FILES.items():

                if filename not in available:
                    logger.warning(
                        "%s not in zip - skipping",
                        filename,
                    )
                    continue

                try:
                    rows = self._process_file(
                        zf,
                        filename,
                        cfg,
                        date_str,
                    )

                    results[cfg["table"]] = {
                        "rows": rows,
                        "status": "ok",
                    }

                    logger.info(
                        "Loaded %s: %s rows",
                        cfg["table"],
                        rows,
                    )

                except Exception as e:
                    logger.exception(
                        "Failed to load %s: %s",
                        filename,
                        e,
                    )

                    results[cfg["table"]] = {
                        "rows": 0,
                        "status": f"error: {e}",
                    }

        logger.info(
            "GTFS Static load complete: %s",
            results,
        )

        return {
            "success": all(
                item["status"] == "ok"
                for item in results.values()
            ),
            "date": date_str,
            "tables": results,
        }

    def _download(self) -> Optional[bytes]:

        logger.info(
            "Downloading GTFS Static from %s",
            self.gtfs_url,
        )

        try:
            resp = requests.get(
                self.gtfs_url,
                timeout=self.request_timeout,
            )

            resp.raise_for_status()

            size_mb = len(resp.content) / 1024 / 1024

            logger.info(
                "Downloaded %.1f MB",
                size_mb,
            )

            return resp.content

        except requests.exceptions.RequestException as e:
            logger.error(
                "Download failed: %s",
                e,
            )

            return None

    def _process_file(
        self,
        zf: zipfile.ZipFile,
        filename: str,
        cfg: dict,
        date_str: str,
    ) -> int:

        table = cfg["table"]
        chunksize = cfg.get("chunksize")

        with zf.open(filename) as f:

            if filename == "trips.txt":

                header = pd.read_csv(
                    f,
                    nrows=0,
                ).columns.tolist()

                missing_required = [
                    col
                    for col in cfg["required_cols"]
                    if col not in header
                ]

                if missing_required:
                    raise ValueError(
                        f"Missing required trips columns: "
                        f"{missing_required}"
                    )

                selected_cols = [
                    col
                    for col in cfg["usecols"]
                    if col in header
                ]

                f.seek(0)

                df = pd.read_csv(
                    f,
                    usecols=selected_cols,
                    dtype={
                        k: v
                        for k, v in cfg["dtype"].items()
                        if k in selected_cols
                    },
                    low_memory=False,
                )

                for col in cfg["optional_cols"]:
                    if col not in df.columns:
                        df[col] = pd.NA

                df = df[cfg["usecols"]]

                df = self._add_metadata(
                    df,
                    date_str,
                )

                self._upload_parquet(
                    df,
                    table,
                    date_str,
                )

                return len(df)

            if chunksize:
                return self._process_chunked(
                    f,
                    cfg,
                    table,
                    date_str,
                    chunksize,
                )

            df = pd.read_csv(
                f,
                usecols=cfg["usecols"],
                dtype=cfg.get("dtype", {}),
                low_memory=False,
            )

            df = self._add_metadata(
                df,
                date_str,
            )

            self._upload_parquet(
                df,
                table,
                date_str,
            )

            return len(df)

    def _process_chunked(
        self,
        f,
        cfg,
        table,
        date_str,
        chunksize,
    ) -> int:

        total = 0
        buffer = []
        part_number = 0
        batch_rows = 2_000_000

        reader = pd.read_csv(
            f,
            usecols=cfg["usecols"],
            dtype=cfg.get("dtype", {}),
            chunksize=chunksize,
            low_memory=False,
        )

        for chunk in reader:

            chunk = self._add_metadata(
                chunk,
                date_str,
            )

            buffer.append(chunk)
            total += len(chunk)

            buffered_rows = sum(
                len(item) for item in buffer
            )

            if buffered_rows >= batch_rows:

                combined = pd.concat(
                    buffer,
                    ignore_index=True,
                )

                self._upload_parquet(
                    combined,
                    table,
                    date_str,
                    part=part_number,
                )

                part_number += 1
                buffer = []

                logger.info(
                    "%s: processed %s rows so far",
                    table,
                    f"{total:,}",
                )

        if buffer:

            combined = pd.concat(
                buffer,
                ignore_index=True,
            )

            self._upload_parquet(
                combined,
                table,
                date_str,
                part="final",
            )

        return total

    def _add_metadata(
        self,
        df: pd.DataFrame,
        date_str: str,
    ) -> pd.DataFrame:

        df["_loaded_date"] = date_str

        df["_loaded_at"] = (
            datetime.now(timezone.utc).isoformat()
        )

        return df

    def _upload_parquet(
        self,
        df: pd.DataFrame,
        table: str,
        date_str: str,
        part: Optional[Union[int, str]] = None,
    ) -> None:

        part_str = (
            f"_part{part}"
            if part is not None
            else ""
        )

        s3_key = (
            f"dims/{table}/"
            f"loaded_date={date_str}/"
            f"{table}{part_str}.parquet"
        )

        buffer = io.BytesIO()

        df.to_parquet(
            buffer,
            index=False,
            engine="pyarrow",
            compression="snappy",
        )

        buffer.seek(0)

        self.s3.upload_fileobj(
            buffer,
            self.bucket,
            s3_key,
        )

        logger.info(
            "Uploaded s3://%s/%s (%s rows)",
            self.bucket,
            s3_key,
            f"{len(df):,}",
        )