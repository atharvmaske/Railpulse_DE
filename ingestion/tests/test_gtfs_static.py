# ingestion/tests/test_gtfs_static.py
import io
import zipfile
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch
from ingestion.gtfs_static.loader import GTFSStaticLoader


def _make_mock_zip() -> bytes:
    """Build an in-memory GTFS zip with minimal CSVs."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("routes.txt",
            "route_id,route_short_name,route_long_name,route_type\n"
            "ICE,ICE,Intercity Express,2\n"
            "RE50,RE50,Regional Express,3\n"
        )
        zf.writestr("stops.txt",
            "stop_id,stop_name,stop_lat,stop_lon\n"
            "FRA,Frankfurt Hbf,50.1109,8.6821\n"
            "MUC,München Hbf,48.1402,11.5600\n"
        )
        zf.writestr("trips.txt",
            "trip_id,route_id,service_id,direction_id,trip_headsign\n"
            "ICE-101,ICE,WD,0,München Hbf\n"
        )
        zf.writestr("calendar.txt",
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
            "WD,1,1,1,1,1,0,0,20240101,20241231\n"
        )
    buf.seek(0)
    return buf.read()


@pytest.fixture
def loader():
    with patch("boto3.client") as mock_boto:
        mock_s3 = MagicMock()
        mock_boto.return_value = mock_s3
        l = GTFSStaticLoader(
            gtfs_url="http://mock-gtfs/static.zip",
            s3_endpoint="http://localhost:9000",
            aws_key="test",
            aws_secret="test",
            bucket="railpulse-static",
        )
        l._mock_s3 = mock_s3
        return l


class TestGTFSStaticLoader:

    def test_process_routes(self, loader):
        zip_bytes = _make_mock_zip()
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            from ingestion.gtfs_static.loader import GTFS_FILES
            cfg = GTFS_FILES["routes.txt"]
            rows = loader._process_file(zf, "routes.txt", cfg, "2024-11-01")
        assert rows == 2

    def test_process_stops(self, loader):
        zip_bytes = _make_mock_zip()
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            from ingestion.gtfs_static.loader import GTFS_FILES
            cfg = GTFS_FILES["stops.txt"]
            rows = loader._process_file(zf, "stops.txt", cfg, "2024-11-01")
        assert rows == 2

    def test_add_metadata_adds_columns(self, loader):
        df = pd.DataFrame({"route_id": ["ICE"], "route_short_name": ["ICE"]})
        result = loader._add_metadata(df, "2024-11-01")
        assert "_loaded_date" in result.columns
        assert "_loaded_at" in result.columns
        assert result["_loaded_date"].iloc[0] == "2024-11-01"

    def test_upload_parquet_called(self, loader):
        df = pd.DataFrame({"stop_id": ["FRA"], "stop_name": ["Frankfurt"]})
        loader._upload_parquet(df, "stops", "2024-11-01")
        loader._mock_s3.upload_fileobj.assert_called_once()
        call_args = loader._mock_s3.upload_fileobj.call_args
        assert call_args[0][1] == "railpulse-static"
        assert "dims/stops" in call_args[0][2]

    def test_missing_file_skipped(self, loader):
        with patch.object(loader, "_download", return_value=_make_mock_zip()):
            result = loader.run(date_str="2024-11-01")
        assert result["success"] is True
        assert "routes" in result["tables"]
        assert "stops" in result["tables"]
        assert result["tables"]["routes"]["status"] == "ok"