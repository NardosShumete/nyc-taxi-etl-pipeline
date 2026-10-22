"""
inspect_dataset.py — Member 1: Dataset Inspection Script
NYC Taxi ETL Pipeline

Usage
-----
    python scripts/inspect_dataset.py --input data/raw/<filename>
    python scripts/inspect_dataset.py --input data/raw/<filename> --chunksize 500000

Outputs a reproducible quality report to stdout.  If the dataset has
≥ 2 000 000 rows the script confirms that requirement; otherwise it
explicitly states the actual count and flags the shortfall.

For large CSV files, chunked processing is used to avoid loading the
entire file into memory.  For Parquet files, pyarrow enables efficient
column-oriented reading.

Do not commit raw data files.  See .gitignore.
"""

import argparse
import logging
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

REQUIRED_MIN_ROWS = 2_000_000

# Columns expected by the assignment description (normalised names).
# The actual file uses "Airport_fee" (capital A); the inspection script
# notes this as a casing discrepancy rather than a missing column.
EXPECTED_COLUMNS = {
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",   # actual file: "Airport_fee" (capital A) — normalised in transform.py
    "speed_mph",
    "duration_min",
}

# Additional columns present in the actual dataset beyond the assignment spec.
# These are documented here for Member 2 (schema) and Member 3 (loading).
EXTRA_ACTUAL_COLUMNS = {
    "VendorID",
    "improvement_surcharge",
    "cbd_congestion_fee",
    "pickup_hour",
    "day_of_week",
    "is_weekend",
    "Airport_fee",  # casing variant of airport_fee
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _separator(char: str = "─", width: int = 72) -> str:
    return char * width


def _print_section(title: str) -> None:
    print(f"\n{_separator()}")
    print(f"  {title}")
    print(_separator())


def detect_format(path: Path) -> str:
    """Return 'parquet' or 'csv' based on the file extension."""
    suffix = path.suffix.lower()
    if suffix in (".parquet", ".pq"):
        return "parquet"
    if suffix in (".csv", ".tsv", ".txt"):
        return "csv"
    # Try to sniff the first few bytes
    with open(path, "rb") as fh:
        header = fh.read(4)
    if header[:4] == b"PAR1":
        return "parquet"
    return "csv"


# ---------------------------------------------------------------------------
# Parquet inspection (full file, memory-efficient column reads via pyarrow)
# ---------------------------------------------------------------------------

def inspect_parquet(path: Path) -> None:
    logger.info("Reading Parquet file: %s", path)

    df = pd.read_parquet(path, engine="pyarrow")

    _print_section("FILE INFORMATION")
    print(f"  Path       : {path.resolve()}")
    print(f"  Format     : Parquet")
    print(f"  File size  : {path.stat().st_size / 1e6:.2f} MB")

    _report_dataframe(df, path)


# ---------------------------------------------------------------------------
# CSV inspection — chunked to handle large files
# ---------------------------------------------------------------------------

def inspect_csv(path: Path, chunksize: int) -> None:
    logger.info("Reading CSV file in chunks of %d rows: %s", chunksize, path)

    _print_section("FILE INFORMATION")
    print(f"  Path       : {path.resolve()}")
    print(f"  Format     : CSV")
    print(f"  File size  : {path.stat().st_size / 1e6:.2f} MB")
    print(f"  Chunk size : {chunksize:,} rows")

    # First pass: collect schema, count rows, accumulate null counts, min/max
    total_rows = 0
    null_counts: dict = {}
    numeric_stats: dict = {}   # {col: {"min":, "max":, "sum":, "count":}}
    dtypes: dict = {}
    columns: list = []
    first_chunk = True

    datetime_cols = ("tpep_pickup_datetime", "tpep_dropoff_datetime")

    print("\n  Scanning file (this may take a moment for large files)…")

    for chunk in pd.read_csv(
        path,
        chunksize=chunksize,
        low_memory=False,
        parse_dates=list(datetime_cols),
    ):
        if first_chunk:
            columns = list(chunk.columns)
            dtypes = {c: str(chunk[c].dtype) for c in columns}
            for col in columns:
                null_counts[col] = 0
                if pd.api.types.is_numeric_dtype(chunk[col]):
                    numeric_stats[col] = {
                        "min": float("inf"),
                        "max": float("-inf"),
                        "sum": 0.0,
                        "count": 0,
                    }
            first_chunk = False

        total_rows += len(chunk)

        for col in columns:
            null_counts[col] += int(chunk[col].isna().sum())

        for col, stats in numeric_stats.items():
            if col in chunk.columns:
                valid = chunk[col].dropna()
                if len(valid) > 0:
                    stats["min"] = min(stats["min"], float(valid.min()))
                    stats["max"] = max(stats["max"], float(valid.max()))
                    stats["sum"] += float(valid.sum())
                    stats["count"] += len(valid)

    # Compute derived stats
    numeric_summary = {}
    for col, stats in numeric_stats.items():
        if stats["count"] > 0:
            numeric_summary[col] = {
                "min": stats["min"],
                "max": stats["max"],
                "mean": stats["sum"] / stats["count"],
                "non_null_count": stats["count"],
            }

    # For datetime min/max we need a targeted re-read (small memory cost)
    dt_range: dict = {}
    for dt_col in datetime_cols:
        if dt_col in columns:
            col_min, col_max = _datetime_range_chunked(path, dt_col, chunksize)
            dt_range[dt_col] = {"min": col_min, "max": col_max}

    # Duplicate count — requires loading into memory; skip for very large files
    dup_count: Optional[int] = None  # type: ignore[name-defined]
    file_size_mb = path.stat().st_size / 1e6
    if file_size_mb < 2_000:
        logger.info("Counting exact duplicates (loading full file)…")
        try:
            full_df = pd.read_csv(path, low_memory=False)
            dup_count = int(full_df.duplicated().sum())
            del full_df
        except MemoryError:
            logger.warning("Not enough memory to count duplicates; skipping.")

    _print_csv_report(
        path, total_rows, columns, dtypes, null_counts,
        numeric_summary, dt_range, dup_count,
    )


def _datetime_range_chunked(path: Path, col: str, chunksize: int):
    """Return (min, max) for a datetime column using chunked CSV reading."""
    global_min = pd.NaT
    global_max = pd.NaT
    for chunk in pd.read_csv(path, chunksize=chunksize, usecols=[col],
                              parse_dates=[col], low_memory=False):
        valid = chunk[col].dropna()
        if len(valid) == 0:
            continue
        chunk_min = valid.min()
        chunk_max = valid.max()
        if pd.isna(global_min) or chunk_min < global_min:
            global_min = chunk_min
        if pd.isna(global_max) or chunk_max > global_max:
            global_max = chunk_max
    return global_min, global_max


def _print_csv_report(
    path, total_rows, columns, dtypes, null_counts,
    numeric_summary, dt_range, dup_count,
):
    n_cols = len(columns)

    _print_section("ROW AND COLUMN COUNT")
    print(f"  Total rows   : {total_rows:,}")
    print(f"  Total columns: {n_cols}")
    if total_rows >= REQUIRED_MIN_ROWS:
        print(f"  ✓ Dataset meets the 2,000,000-row requirement.")
    else:
        shortfall = REQUIRED_MIN_ROWS - total_rows
        print(
            f"  ✗ Dataset does NOT meet the 2,000,000-row requirement "
            f"(shortfall: {shortfall:,} rows)."
        )

    _print_section("COLUMN NAMES AND DATA TYPES")
    for col in columns:
        print(f"  {col:<40} {dtypes[col]}")

    _print_section("EXPECTED COLUMN PRESENCE")
    actual_set = set(columns)
    # Treat Airport_fee and airport_fee as equivalent for this check
    actual_normalised = {c.lower() for c in actual_set}
    for expected_col in sorted(EXPECTED_COLUMNS):
        present = expected_col in actual_set or expected_col.lower() in actual_normalised
        status = "✓" if present else "✗ MISSING"
        note = "  (found as 'Airport_fee' — casing normalised in transform.py)" if expected_col == "airport_fee" and "Airport_fee" in actual_set else ""
        print(f"  {status:<10} {expected_col}{note}")
    extra = actual_set - EXPECTED_COLUMNS - EXTRA_ACTUAL_COLUMNS
    if extra:
        print(f"\n  Additional columns not in the expected list: {sorted(extra)}")
    if EXTRA_ACTUAL_COLUMNS & actual_set:
        print(f"\n  Additional columns present in actual file (beyond assignment spec):")
        for col in sorted(EXTRA_ACTUAL_COLUMNS & actual_set):
            print(f"    {col}")

    _print_section("MISSING VALUES")
    print(f"  {'Column':<40} {'Null count':>12}  {'%':>8}")
    print(f"  {'-'*40}  {'-'*12}  {'-'*8}")
    for col in columns:
        nc = null_counts[col]
        pct = 100.0 * nc / total_rows if total_rows > 0 else 0.0
        marker = "  ← has nulls" if nc > 0 else ""
        print(f"  {col:<40} {nc:>12,}  {pct:>7.2f}%{marker}")

    if dup_count is not None:
        _print_section("DUPLICATE ROWS")
        print(f"  Exact duplicate rows: {dup_count:,}")
    else:
        _print_section("DUPLICATE ROWS")
        print("  (Skipped — file too large to load fully into memory for duplicate check.)")

    _print_section("DATETIME RANGE")
    for dt_col, rng in dt_range.items():
        print(f"  {dt_col}:")
        print(f"    Min: {rng['min']}")
        print(f"    Max: {rng['max']}")

    _print_section("NUMERIC SUMMARIES")
    for col, stats in numeric_summary.items():
        print(
            f"  {col:<30}  min={stats['min']:>12.4f}  "
            f"max={stats['max']:>12.4f}  mean={stats['mean']:>12.4f}"
        )

    _print_section("ENGINEERED FEATURE PRESENCE")
    for feat in ("speed_mph", "duration_min"):
        status = "✓ Present" if feat in actual_set else "✗ Not found"
        print(f"  {feat:<20} {status}")

    _print_section("SUSPICIOUS VALUE FLAGS")
    _flag_suspicious_from_summary(numeric_summary)


# ---------------------------------------------------------------------------
# Full-DataFrame reporting (used for Parquet and small CSVs)
# ---------------------------------------------------------------------------

def _report_dataframe(df: pd.DataFrame, path: Path) -> None:
    n_rows, n_cols = df.shape

    _print_section("ROW AND COLUMN COUNT")
    print(f"  Total rows   : {n_rows:,}")
    print(f"  Total columns: {n_cols}")
    if n_rows >= REQUIRED_MIN_ROWS:
        print(f"  ✓ Dataset meets the 2,000,000-row requirement.")
    else:
        shortfall = REQUIRED_MIN_ROWS - n_rows
        print(
            f"  ✗ Dataset does NOT meet the 2,000,000-row requirement "
            f"(shortfall: {shortfall:,} rows)."
        )

    _print_section("COLUMN NAMES AND DATA TYPES")
    for col, dtype in df.dtypes.items():
        print(f"  {col:<40} {dtype}")

    _print_section("EXPECTED COLUMN PRESENCE")
    actual_set = set(df.columns)
    actual_normalised = {c.lower() for c in actual_set}
    for expected_col in sorted(EXPECTED_COLUMNS):
        present = expected_col in actual_set or expected_col.lower() in actual_normalised
        status = "✓" if present else "✗ MISSING"
        note = "  (found as 'Airport_fee' — casing normalised in transform.py)" if expected_col == "airport_fee" and "Airport_fee" in actual_set else ""
        print(f"  {status:<10} {expected_col}{note}")
    extra = actual_set - EXPECTED_COLUMNS - EXTRA_ACTUAL_COLUMNS
    if extra:
        print(f"\n  Additional columns not in the expected list: {sorted(extra)}")
    if EXTRA_ACTUAL_COLUMNS & actual_set:
        print(f"\n  Additional columns present in actual file (beyond assignment spec):")
        for col in sorted(EXTRA_ACTUAL_COLUMNS & actual_set):
            print(f"    {col}")

    _print_section("MISSING VALUES")
    print(f"  {'Column':<40} {'Null count':>12}  {'%':>8}")
    print(f"  {'-'*40}  {'-'*12}  {'-'*8}")
    for col in df.columns:
        nc = int(df[col].isna().sum())
        pct = 100.0 * nc / n_rows if n_rows > 0 else 0.0
        marker = "  ← has nulls" if nc > 0 else ""
        print(f"  {col:<40} {nc:>12,}  {pct:>7.2f}%{marker}")

    _print_section("DUPLICATE ROWS")
    dup_count = int(df.duplicated().sum())
    print(f"  Exact duplicate rows: {dup_count:,}")

    _print_section("DATETIME RANGE")
    for dt_col in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
        if dt_col in df.columns:
            col = pd.to_datetime(df[dt_col], errors="coerce")
            print(f"  {dt_col}:")
            print(f"    Min: {col.min()}")
            print(f"    Max: {col.max()}")

    _print_section("NUMERIC SUMMARIES")
    num_df = df.select_dtypes(include=[np.number])
    if not num_df.empty:
        desc = num_df.describe().T[["min", "max", "mean", "std"]]
        print(desc.to_string())

    _print_section("MEMORY USAGE")
    mem_bytes = df.memory_usage(deep=True).sum()
    print(f"  Approximate memory : {mem_bytes / 1e6:.2f} MB")

    _print_section("ENGINEERED FEATURE PRESENCE")
    for feat in ("speed_mph", "duration_min"):
        status = "✓ Present" if feat in df.columns else "✗ Not found"
        print(f"  {feat:<20} {status}")

    _print_section("SUSPICIOUS VALUE FLAGS")
    num_cols = num_df.columns.tolist()
    suspicious_summary = {}
    for col in num_cols:
        suspicious_summary[col] = {"min": float(df[col].min()), "max": float(df[col].max())}
    _flag_suspicious_from_df(df)


def _flag_suspicious_from_summary(numeric_summary: dict) -> None:
    """Print suspicious-value flags from aggregated stats (CSV chunked path)."""
    checks = {
        "trip_distance": lambda s: (s["min"] <= 0, f"min={s['min']:.2f} — non-positive distances"),
        "speed_mph": lambda s: (s["max"] > 100, f"max={s['max']:.2f} — speed > 100 MPH"),
        "fare_amount": lambda s: (s["min"] < 0, f"min={s['min']:.2f} — negative fares"),
        "tip_amount": lambda s: (s["min"] < 0, f"min={s['min']:.2f} — negative tips"),
        "duration_min": lambda s: (s["min"] <= 0, f"min={s['min']:.2f} — non-positive durations"),
        "passenger_count": lambda s: (
            s["max"] > 6 or s["min"] < 1,
            f"range [{s['min']:.0f}, {s['max']:.0f}] — outside 1–6",
        ),
    }
    any_flag = False
    for col, check_fn in checks.items():
        if col in numeric_summary:
            flagged, msg = check_fn(numeric_summary[col])
            if flagged:
                print(f"  ⚠  {col}: {msg}")
                any_flag = True
    if not any_flag:
        print("  No obvious suspicious values detected in aggregated stats.")


def _flag_suspicious_from_df(df: pd.DataFrame) -> None:
    """Print suspicious-value flags when the full DataFrame is available."""
    any_flag = False

    def _check(col, condition, msg):
        nonlocal any_flag
        if col in df.columns:
            count = int(condition(df[col]).sum())
            if count > 0:
                print(f"  ⚠  {col}: {count:,} rows — {msg}")
                any_flag = True

    _check("trip_distance", lambda s: s.notna() & (s <= 0), "non-positive distance")
    _check("speed_mph",     lambda s: s.notna() & (s > 100), "speed > 100 MPH")
    _check("fare_amount",   lambda s: s.notna() & (s < 0),  "negative fare")
    _check("tip_amount",    lambda s: s.notna() & (s < 0),  "negative tip")
    _check("duration_min",  lambda s: s.notna() & (s <= 0), "non-positive duration")
    _check("passenger_count",
           lambda s: s.notna() & ((s < 1) | (s > 6)),
           "outside 1–6 range")

    if not any_flag:
        print("  No obvious suspicious values detected.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="NYC Taxi ETL Pipeline — Dataset Inspection (Member 1)"
    )
    parser.add_argument(
        "--input", "-i",
        required=True,
        help="Path to the dataset file (CSV or Parquet).",
    )
    parser.add_argument(
        "--chunksize", "-c",
        type=int,
        default=500_000,
        help="Rows per chunk for CSV reading (default: 500000).",
    )
    args = parser.parse_args()

    path = Path(args.input)
    if not path.exists():
        logger.error("File not found: %s", path)
        print(
            f"\nERROR: Dataset file not found at '{path}'.\n"
            "Place the raw dataset in data/raw/ and pass its path with --input.\n"
            "Do not commit the dataset file (it is gitignored)."
        )
        sys.exit(1)

    fmt = detect_format(path)

    print("=" * 72)
    print("  NYC Taxi ETL Pipeline — Dataset Inspection Report")
    print("  Member 1: Dataset Inspection, Data Quality Validation")
    print("=" * 72)

    if fmt == "parquet":
        inspect_parquet(path)
    else:
        inspect_csv(path, chunksize=args.chunksize)

    print(f"\n{_separator('=')}")
    print("  Inspection complete.")
    print(_separator("="))


if __name__ == "__main__":
    # Allow running as: python scripts/inspect_dataset.py --input <file>
    # Ensure the project root is on the Python path when running directly.
    project_root = Path(__file__).resolve().parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    main()
