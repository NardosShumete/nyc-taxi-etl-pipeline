"""
test_loader.py — Member 3: Tests for scripts/load_to_postgres.py
=================================================================

Test categories
---------------
1.  RENAME_MAP correctness
2.  _prepare_accepted: column names, null handling, generated-col exclusion
3.  _prepare_rejected: column names, type conversions, rejection_reason
4.  Column compatibility: prepared DataFrames vs both DB table schemas
5.  _check_tables_exist: mocked engine behaviour
6.  run() dry-run mode (no DB required)
7.  run() error paths (missing CSV, missing tables)
8.  Chunk-size processing (mocked DB)
9.  PostgreSQL integration: disposable test DB
    - create tables via create_tables.sql
    - insert one synthetic accepted row, one synthetic rejected row
    - verify both tables contain expected data and generated columns populated
    - idempotency / rerun behaviour documented and tested

Run only unit tests (no DB):
    pytest tests/test_loader.py -v -k "not db"

Run including DB integration tests:
    pytest tests/test_loader.py -v

DB tests are automatically skipped when PostgreSQL is unreachable.
"""

import sys
import os
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Path bootstrap
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.load_to_postgres import (
    RENAME_MAP,
    GENERATED_ACCEPTED,
    GENERATED_REJECTED,
    _prepare_accepted,
    _prepare_rejected,
    _check_tables_exist,
    _insert_chunk,
    run,
)
from src.transform import transform_data

# ---------------------------------------------------------------------------
# PostgreSQL availability check (used to skip DB tests when unavailable)
# ---------------------------------------------------------------------------
PG_USER     = "postgres"
PG_PASSWORD = "postgres"
PG_HOST     = "localhost"
PG_PORT     = "5432"
TEST_DB     = "nyc_taxi_test"   # disposable — never touches nyc_taxi or bookrent_test
PSQL        = r"C:\Program Files\PostgreSQL\18\bin\psql.exe"
CREATEDB    = r"C:\Program Files\PostgreSQL\18\bin\createdb.exe"
DROPDB      = r"C:\Program Files\PostgreSQL\18\bin\dropdb.exe"


def _pg_available() -> bool:
    """Return True if PostgreSQL is reachable with the test credentials."""
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT,
            user=PG_USER, password=PG_PASSWORD,
            dbname="postgres", connect_timeout=3,
        )
        conn.close()
        return True
    except Exception:
        return False


pg_available = _pg_available()
skip_if_no_pg = pytest.mark.skipif(
    not pg_available,
    reason="PostgreSQL not reachable — skipping DB integration tests",
)


def _pg_engine(dbname: str = "postgres"):
    """Return a SQLAlchemy engine for the given database."""
    from sqlalchemy import create_engine
    return create_engine(
        f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{dbname}",
        pool_pre_ping=True,
    )


# ---------------------------------------------------------------------------
# Shared test data
# ---------------------------------------------------------------------------

VALID_ROW: dict = {
    "VendorID":              1,
    "tpep_pickup_datetime":  pd.Timestamp("2025-01-15 08:00:00"),
    "tpep_dropoff_datetime": pd.Timestamp("2025-01-15 08:20:00"),
    "passenger_count":       2,
    "trip_distance":         3.5,
    "RatecodeID":            1,
    "store_and_fwd_flag":    "N",
    "PULocationID":          161,
    "DOLocationID":          236,
    "payment_type":          1,
    "fare_amount":           14.50,
    "extra":                 0.50,
    "mta_tax":               0.50,
    "tip_amount":            3.00,
    "tolls_amount":          0.00,
    "improvement_surcharge": 0.30,
    "total_amount":          21.30,
    "congestion_surcharge":  2.50,
    "Airport_fee":           None,   # nullable — normalised to airport_fee by transform
    "cbd_congestion_fee":    0.00,
    "speed_mph":             10.5,
    "duration_min":          20.0,
    "pickup_hour":           8,
    "day_of_week":           "Wednesday",
    "is_weekend":            False,
}

INVALID_ROW: dict = {**VALID_ROW, "fare_amount": -999.0}


def _transform_valid():
    """Run transform_data on one valid row and return result dict."""
    return transform_data(pd.DataFrame([VALID_ROW.copy()]))


def _transform_invalid():
    """Run transform_data on one invalid row and return result dict."""
    return transform_data(pd.DataFrame([INVALID_ROW.copy()]))


# ---------------------------------------------------------------------------
# DB schema column sets (mirrors sql/create_tables.sql — excludes generated cols)
# ---------------------------------------------------------------------------

TAXI_TRIPS_COLS = {
    "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance",
    "ratecode_id", "store_and_fwd_flag",
    "pu_location_id", "do_location_id",
    "payment_type",
    "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "congestion_surcharge",
    "airport_fee", "cbd_congestion_fee",
    "vendor_id",
    "speed_mph", "duration_min", "pickup_hour", "day_of_week", "is_weekend",
}

REJECTED_TRIPS_COLS = {
    "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "vendor_id", "passenger_count", "trip_distance",
    "ratecode_id", "store_and_fwd_flag",
    "pu_location_id", "do_location_id",
    "payment_type",
    "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "congestion_surcharge",
    "airport_fee", "cbd_congestion_fee",
    "speed_mph", "duration_min", "pickup_hour", "day_of_week", "is_weekend",
    "rejection_reason",
}


# ===========================================================================
# 1. RENAME_MAP correctness
# ===========================================================================

class TestRenameMap:

    def test_covers_all_four_mixed_case_columns(self):
        assert set(RENAME_MAP.keys()) == {
            "VendorID", "RatecodeID", "PULocationID", "DOLocationID"
        }

    def test_all_targets_are_lowercase_snake_case(self):
        for src, dst in RENAME_MAP.items():
            assert dst == dst.lower(), f"Target '{dst}' is not lowercase"
            assert "_" in dst or dst.isalpha(), f"Target '{dst}' not snake_case"

    def test_expected_mappings(self):
        assert RENAME_MAP["VendorID"]     == "vendor_id"
        assert RENAME_MAP["RatecodeID"]   == "ratecode_id"
        assert RENAME_MAP["PULocationID"] == "pu_location_id"
        assert RENAME_MAP["DOLocationID"] == "do_location_id"


# ===========================================================================
# 2. _prepare_accepted
# ===========================================================================

class TestPrepareAccepted:

    def _prep(self):
        result = _transform_valid()
        return _prepare_accepted(result["clean_df"])

    def test_mixed_case_columns_renamed(self):
        prep = self._prep()
        for src, dst in RENAME_MAP.items():
            assert dst in prep.columns,  f"Expected '{dst}' in prepared DF"
            assert src not in prep.columns, f"Old name '{src}' still present"

    def test_lowercase_columns_unchanged(self):
        prep = self._prep()
        for col in ("fare_amount", "trip_distance", "store_and_fwd_flag",
                    "pickup_hour", "day_of_week"):
            assert col in prep.columns

    def test_trip_id_absent(self):
        """trip_id is GENERATED ALWAYS — must not be in INSERT."""
        prep = self._prep()
        assert "trip_id" not in prep.columns

    def test_airport_fee_null_is_nan_or_none(self):
        """airport_fee NaN from the source stays as NaN; SQLAlchemy maps it to NULL."""
        prep = self._prep()
        val = prep["airport_fee"].iloc[0]
        assert pd.isna(val), f"Expected NaN/None for airport_fee, got {val!r}"

    def test_airport_fee_with_value_preserved(self):
        row = {**VALID_ROW, "Airport_fee": 1.75}
        result = transform_data(pd.DataFrame([row]))
        prep = _prepare_accepted(result["clean_df"])
        assert float(prep["airport_fee"].iloc[0]) == pytest.approx(1.75)

    def test_no_rejection_reason_column(self):
        prep = self._prep()
        assert "rejection_reason" not in prep.columns

    def test_is_weekend_is_bool(self):
        prep = self._prep()
        assert prep["is_weekend"].dtype == bool

    def test_exactly_25_columns(self):
        prep = self._prep()
        assert len(prep.columns) == 25, (
            f"Expected 25 columns, got {len(prep.columns)}: {list(prep.columns)}"
        )

    def test_does_not_mutate_input(self):
        result = _transform_valid()
        original_cols = list(result["clean_df"].columns)
        _prepare_accepted(result["clean_df"])
        assert list(result["clean_df"].columns) == original_cols


# ===========================================================================
# 3. _prepare_rejected
# ===========================================================================

class TestPrepareRejected:

    def _prep(self):
        result = _transform_invalid()
        return _prepare_rejected(result["rejected_df"])

    def test_mixed_case_columns_renamed(self):
        prep = self._prep()
        for src, dst in RENAME_MAP.items():
            assert dst in prep.columns
            assert src not in prep.columns

    def test_rejected_id_absent(self):
        prep = self._prep()
        assert "rejected_id" not in prep.columns

    def test_rejected_at_absent(self):
        prep = self._prep()
        assert "rejected_at" not in prep.columns

    def test_datetime_columns_are_strings(self):
        """rejected_taxi_trips stores datetimes as TEXT to tolerate NaT."""
        prep = self._prep()
        assert prep["tpep_pickup_datetime"].dtype == object
        assert prep["tpep_dropoff_datetime"].dtype == object
        assert isinstance(prep["tpep_pickup_datetime"].iloc[0], str)

    def test_is_weekend_is_string(self):
        prep = self._prep()
        assert prep["is_weekend"].dtype == object
        assert prep["is_weekend"].iloc[0] in ("True", "False")

    def test_rejection_reason_present_and_not_null(self):
        prep = self._prep()
        assert "rejection_reason" in prep.columns
        assert prep["rejection_reason"].iloc[0] == "negative_fare_amount"

    def test_exactly_26_columns(self):
        """25 source cols + rejection_reason."""
        prep = self._prep()
        assert len(prep.columns) == 26, (
            f"Expected 26 columns, got {len(prep.columns)}: {list(prep.columns)}"
        )

    def test_does_not_mutate_input(self):
        result = _transform_invalid()
        original_cols = list(result["rejected_df"].columns)
        _prepare_rejected(result["rejected_df"])
        assert list(result["rejected_df"].columns) == original_cols


# ===========================================================================
# 4. Column compatibility with DB schema
# ===========================================================================

class TestSchemaCompatibility:

    def test_accepted_columns_match_taxi_trips_schema(self):
        result = _transform_valid()
        prep = _prepare_accepted(result["clean_df"])
        actual  = set(prep.columns)
        missing = TAXI_TRIPS_COLS - actual
        extra   = actual - TAXI_TRIPS_COLS
        assert not missing, f"Columns missing from accepted DF: {missing}"
        assert not extra,   f"Unexpected extra columns in accepted DF: {extra}"

    def test_rejected_columns_match_rejected_trips_schema(self):
        result = _transform_invalid()
        prep = _prepare_rejected(result["rejected_df"])
        actual  = set(prep.columns)
        missing = REJECTED_TRIPS_COLS - actual
        extra   = actual - REJECTED_TRIPS_COLS
        assert not missing, f"Columns missing from rejected DF: {missing}"
        assert not extra,   f"Unexpected extra columns in rejected DF: {extra}"

    def test_no_generated_cols_in_accepted(self):
        result = _transform_valid()
        prep = _prepare_accepted(result["clean_df"])
        for col in GENERATED_ACCEPTED:
            assert col not in prep.columns, f"Generated col '{col}' should not be in INSERT"

    def test_no_generated_cols_in_rejected(self):
        result = _transform_invalid()
        prep = _prepare_rejected(result["rejected_df"])
        for col in GENERATED_REJECTED:
            assert col not in prep.columns, f"Generated col '{col}' should not be in INSERT"


# ===========================================================================
# 5. _check_tables_exist (mocked engine)
# ===========================================================================

class TestCheckTablesExist:

    def _mock_engine(self, table_names: list[str]):
        engine = MagicMock()
        conn   = MagicMock()
        conn.execute.return_value.fetchall.return_value = [
            (t,) for t in table_names
        ]
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__  = MagicMock(return_value=False)
        return engine

    def test_raises_when_no_tables(self):
        engine = self._mock_engine([])
        with pytest.raises(RuntimeError, match="Required tables not found"):
            _check_tables_exist(engine)

    def test_raises_when_only_one_table(self):
        engine = self._mock_engine(["taxi_trips"])
        with pytest.raises(RuntimeError, match="rejected_taxi_trips"):
            _check_tables_exist(engine)

    def test_passes_when_both_tables_present(self):
        engine = self._mock_engine(["taxi_trips", "rejected_taxi_trips"])
        # Should not raise
        _check_tables_exist(engine)

    def test_error_message_names_missing_tables(self):
        engine = self._mock_engine([])
        with pytest.raises(RuntimeError) as exc_info:
            _check_tables_exist(engine)
        msg = str(exc_info.value)
        assert "taxi_trips" in msg or "rejected_taxi_trips" in msg


# ===========================================================================
# 6. run() — dry-run mode (no DB required)
# ===========================================================================

class TestDryRun:

    def _mini_csv(self, tmp_path: Path, n: int = 4) -> Path:
        """Write a small valid CSV with n unique rows."""
        rows = []
        for i in range(n):
            r = VALID_ROW.copy()
            r["PULocationID"] = i + 1   # make each row unique
            r["Airport_fee"]  = r.pop("Airport_fee")  # key already correct
            rows.append(r)
        df = pd.DataFrame(rows)
        csv_path = tmp_path / "mini.csv"
        df.to_csv(csv_path, index=False)
        return csv_path

    def test_dry_run_returns_zero(self, tmp_path):
        csv_path = self._mini_csv(tmp_path)
        assert run(["--input", str(csv_path), "--dry-run"]) == 0

    def test_dry_run_does_not_call_build_engine(self, tmp_path):
        csv_path = self._mini_csv(tmp_path)
        with patch("scripts.load_to_postgres._build_engine") as mock_eng:
            run(["--input", str(csv_path), "--dry-run"])
        mock_eng.assert_not_called()

    def test_dry_run_missing_csv_returns_one(self, tmp_path):
        assert run(["--input", str(tmp_path / "nope.csv"), "--dry-run"]) == 1


# ===========================================================================
# 7. run() — error paths (mocked DB)
# ===========================================================================

class TestRunErrorPaths:

    def test_missing_csv_returns_1(self, tmp_path):
        rc = run(["--input", str(tmp_path / "no_such_file.csv"), "--password", "x"])
        assert rc == 1

    def test_missing_tables_returns_1(self, tmp_path):
        """run() should return 1 when _check_tables_exist raises RuntimeError."""
        # Write a minimal CSV
        df = pd.DataFrame([VALID_ROW])
        csv_path = tmp_path / "t.csv"
        df.to_csv(csv_path, index=False)

        mock_engine = MagicMock()
        mock_conn   = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__  = MagicMock(return_value=False)

        with patch("scripts.load_to_postgres._build_engine", return_value=mock_engine):
            rc = run(["--input", str(csv_path), "--password", "x"])
        assert rc == 1


# ===========================================================================
# 8. Chunk-size processing (mocked DB)
# ===========================================================================

class TestChunkProcessing:

    def _mini_csv(self, tmp_path: Path, n: int = 6) -> Path:
        rows = []
        for i in range(n):
            r = {k: v for k, v in VALID_ROW.items()}
            r["PULocationID"] = i + 1
            rows.append(r)
        df = pd.DataFrame(rows)
        csv_path = tmp_path / "chunk_test.csv"
        df.to_csv(csv_path, index=False)
        return csv_path

    def test_transform_called_once_per_chunk(self, tmp_path):
        csv_path = self._mini_csv(tmp_path, n=6)
        call_counts = {"n": 0}
        original = transform_data

        def counting_transform(df):
            call_counts["n"] += 1
            return original(df)

        with patch("scripts.load_to_postgres._build_engine") as mock_build, \
             patch("scripts.load_to_postgres._check_tables_exist"), \
             patch("scripts.load_to_postgres.transform_data", side_effect=counting_transform), \
             patch("scripts.load_to_postgres._insert_chunk", return_value=(3, 0)):
            mock_build.return_value = MagicMock()
            run(["--input", str(csv_path), "--chunk-size", "3", "--password", "x"])

        # 6 rows / chunk_size 3 = exactly 2 chunks
        assert call_counts["n"] == 2

    def test_failed_chunk_increments_counter(self, tmp_path):
        csv_path = self._mini_csv(tmp_path, n=3)

        with patch("scripts.load_to_postgres._build_engine") as mock_build, \
             patch("scripts.load_to_postgres._check_tables_exist"), \
             patch("scripts.load_to_postgres._insert_chunk",
                   side_effect=RuntimeError("DB down")):
            mock_build.return_value = MagicMock()
            rc = run(["--input", str(csv_path), "--password", "x"])

        # At least one chunk failed → exit code 1
        assert rc == 1

    def test_skip_rejected_flag(self, tmp_path):
        csv_path = self._mini_csv(tmp_path, n=3)
        captured_skip = {}

        def capture_insert(engine, clean, rejected, batch_size, skip_rejected):
            captured_skip["value"] = skip_rejected
            return (len(clean), 0)

        with patch("scripts.load_to_postgres._build_engine") as mock_build, \
             patch("scripts.load_to_postgres._check_tables_exist"), \
             patch("scripts.load_to_postgres._insert_chunk", side_effect=capture_insert):
            mock_build.return_value = MagicMock()
            run(["--input", str(csv_path), "--skip-rejected", "--password", "x"])

        assert captured_skip.get("value") is True


# ===========================================================================
# 9. PostgreSQL integration tests
#    — use disposable database 'nyc_taxi_test', never touch nyc_taxi
# ===========================================================================

@pytest.fixture(scope="module")
def test_db_engine():
    """
    Create the disposable test database, run create_tables.sql, yield the
    engine, then drop the database.  Skipped entirely if PostgreSQL is
    unreachable.
    """
    if not pg_available:
        pytest.skip("PostgreSQL not reachable")

    env = {**os.environ, "PGPASSWORD": PG_PASSWORD}

    # Create test database (ignore error if already exists)
    subprocess.run(
        [CREATEDB, "-U", PG_USER, "-h", PG_HOST, "-p", PG_PORT, TEST_DB],
        env=env, capture_output=True,
    )

    engine = _pg_engine(TEST_DB)

    # Execute create_tables.sql
    sql_file = PROJECT_ROOT / "sql" / "create_tables.sql"
    result = subprocess.run(
        [PSQL, "-U", PG_USER, "-h", PG_HOST, "-p", PG_PORT,
         "-d", TEST_DB, "-f", str(sql_file)],
        env=env, capture_output=True, text=True,
    )
    if result.returncode != 0:
        pytest.fail(f"create_tables.sql failed:\n{result.stderr}")

    yield engine

    # Teardown: drop the test database
    engine.dispose()
    subprocess.run(
        [DROPDB, "-U", PG_USER, "-h", PG_HOST, "-p", PG_PORT, TEST_DB],
        env=env, capture_output=True,
    )


@skip_if_no_pg
class TestPostgresIntegration:

    def test_db_create_tables_sql_is_idempotent(self, test_db_engine):
        """Re-running create_tables.sql must not raise an error."""
        env = {**os.environ, "PGPASSWORD": PG_PASSWORD}
        sql_file = PROJECT_ROOT / "sql" / "create_tables.sql"
        result = subprocess.run(
            [PSQL, "-U", PG_USER, "-h", PG_HOST, "-p", PG_PORT,
             "-d", TEST_DB, "-f", str(sql_file)],
            env=env, capture_output=True, text=True,
        )
        assert result.returncode == 0, f"Second run of create_tables.sql failed:\n{result.stderr}"

    def test_both_tables_exist(self, test_db_engine):
        _check_tables_exist(test_db_engine)   # raises if tables missing

    def test_insert_one_accepted_row(self, test_db_engine):
        """Insert a single synthetic accepted row and verify it appears in taxi_trips."""
        result = transform_data(pd.DataFrame([VALID_ROW.copy()]))
        clean   = result["clean_df"]
        assert len(clean) == 1, "Expected 1 clean row from VALID_ROW"

        n_acc, n_rej = _insert_chunk(
            test_db_engine, clean,
            result["rejected_df"],
            batch_size=1000,
            skip_rejected=False,
        )
        assert n_acc == 1
        assert n_rej == 0

        # Verify in DB
        rows = pd.read_sql(
            "SELECT * FROM taxi_trips ORDER BY trip_id DESC LIMIT 1",
            test_db_engine,
        )
        assert len(rows) == 1
        # trip_id must be server-generated (> 0)
        assert rows["trip_id"].iloc[0] > 0
        # Spot-check a few values
        assert float(rows["fare_amount"].iloc[0]) == pytest.approx(14.50, abs=0.01)
        assert float(rows["trip_distance"].iloc[0]) == pytest.approx(3.5, abs=0.001)
        assert rows["store_and_fwd_flag"].iloc[0].strip() == "N"
        assert rows["day_of_week"].iloc[0] == "Wednesday"
        assert rows["is_weekend"].iloc[0] == False
        # airport_fee should be NULL
        assert pd.isna(rows["airport_fee"].iloc[0])

    def test_insert_one_rejected_row(self, test_db_engine):
        """Insert a synthetic rejected row and verify it appears in rejected_taxi_trips."""
        result = transform_data(pd.DataFrame([INVALID_ROW.copy()]))
        rejected = result["rejected_df"]
        assert len(rejected) == 1

        n_acc, n_rej = _insert_chunk(
            test_db_engine, result["clean_df"],
            rejected,
            batch_size=1000,
            skip_rejected=False,
        )
        assert n_rej == 1

        rows = pd.read_sql(
            "SELECT * FROM rejected_taxi_trips ORDER BY rejected_id DESC LIMIT 1",
            test_db_engine,
        )
        assert len(rows) == 1
        # rejected_id must be server-generated
        assert rows["rejected_id"].iloc[0] > 0
        # rejected_at must be a non-null timestamp
        assert pd.notna(rows["rejected_at"].iloc[0])
        # rejection_reason must match
        assert rows["rejection_reason"].iloc[0] == "negative_fare_amount"
        # datetime stored as TEXT
        assert isinstance(rows["tpep_pickup_datetime"].iloc[0], str)
        assert rows["tpep_pickup_datetime"].iloc[0].startswith("2025-01-15")

    def test_rename_mapping_applied_in_db(self, test_db_engine):
        """Verify the four renamed columns exist in taxi_trips and hold correct values."""
        rows = pd.read_sql(
            "SELECT vendor_id, ratecode_id, pu_location_id, do_location_id "
            "FROM taxi_trips ORDER BY trip_id DESC LIMIT 1",
            test_db_engine,
        )
        assert len(rows) == 1
        assert int(rows["vendor_id"].iloc[0])      == 1
        assert int(rows["ratecode_id"].iloc[0])    == 1
        assert int(rows["pu_location_id"].iloc[0]) == 161
        assert int(rows["do_location_id"].iloc[0]) == 236

    def test_check_constraints_reject_bad_data(self, test_db_engine):
        """
        Attempting to insert a row that violates a CHECK constraint must fail.
        This confirms the schema constraints are active.
        """
        import sqlalchemy as sa
        # Prepare an accepted row then manually corrupt passenger_count
        result = transform_data(pd.DataFrame([VALID_ROW.copy()]))
        prep = _prepare_accepted(result["clean_df"])
        prep = prep.copy()
        prep["passenger_count"] = 99   # violates CHECK (BETWEEN 1 AND 6)

        with pytest.raises(Exception):
            with test_db_engine.begin() as conn:
                prep.to_sql(
                    "taxi_trips", conn,
                    if_exists="append", index=False,
                    method="multi",
                )

    def test_rerun_inserts_duplicates_no_automatic_dedup(self, test_db_engine):
        """
        DOCUMENTED RERUN BEHAVIOUR: re-inserting the same row adds a new trip_id.
        The schema has no UNIQUE constraint on source data, so reruns duplicate.
        This test documents and verifies that known behaviour.
        To prevent duplicates, TRUNCATE both tables before reloading.
        """
        from sqlalchemy import text as sa_text
        # Count rows before
        with test_db_engine.connect() as conn:
            count_before = conn.execute(
                sa_text("SELECT COUNT(*) FROM taxi_trips")
            ).scalar()

        # Insert the same accepted row again
        result = transform_data(pd.DataFrame([VALID_ROW.copy()]))
        _insert_chunk(
            test_db_engine, result["clean_df"],
            result["rejected_df"], 1000, False,
        )

        with test_db_engine.connect() as conn:
            count_after = conn.execute(
                sa_text("SELECT COUNT(*) FROM taxi_trips")
            ).scalar()

        # Count increased → duplicate inserted
        assert count_after > count_before, (
            "Expected duplicate insert to increase row count. "
            "To prevent duplicates on full reload, run: "
            "TRUNCATE taxi_trips, rejected_taxi_trips RESTART IDENTITY;"
        )
