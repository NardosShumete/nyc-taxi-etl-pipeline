"""
load_to_postgres.py — Member 3: ETL Loader for NYC Taxi Pipeline
=================================================================

Reads the cleaned NYC Taxi CSV in configurable chunks, runs each chunk
through Member 1's transform_data() pipeline, and inserts:
  - accepted rows  → taxi_trips
  - rejected rows  → rejected_taxi_trips

Usage (environment variables):
    set DB_HOST=localhost
    set DB_PORT=5432
    set DB_NAME=nyc_taxi
    set DB_USER=postgres
    set DB_PASSWORD=secret
    python scripts/load_to_postgres.py --input data/raw/NYC_Taxi_Cleaned_Analysis_Ready.csv

Usage (command-line only):
    python scripts/load_to_postgres.py \\
        --input  data/raw/NYC_Taxi_Cleaned_Analysis_Ready.csv \\
        --host   localhost \\
        --port   5432 \\
        --dbname nyc_taxi \\
        --user   postgres \\
        --password secret

Optional flags:
    --chunk-size     Rows per CSV read chunk         (default: 100_000)
    --batch-size     Rows per INSERT batch            (default: 10_000)
    --skip-rejected  Do not load rejected rows        (flag, default: False)
    --dry-run        Parse + transform only, no DB    (flag, default: False)

Exit codes:
    0  — all chunks loaded without error
    1  — one or more errors; see stderr

Design notes — memory
---------------------
transform_data() operates on a single DataFrame in memory.  It does not
stream row-by-row, so the minimum memory footprint per iteration is roughly
one chunk (100k rows × 25 cols ≈ ~50 MB).  We never load the full 586 MB CSV
at once.  Three copies of the chunk exist briefly during transformation
(normalize, type-cast, validate each return a copy); peak usage is therefore
≈ 150 MB per iteration at the default 100k chunk size.  Reduce --chunk-size
on memory-constrained machines.

Design notes — idempotency / rerun behaviour
--------------------------------------------
The schema uses GENERATED ALWAYS AS IDENTITY primary keys, so rerunning this
script on a non-empty table will INSERT duplicate trip data rather than
upsert.  This is intentional for a student ETL project — the simplest safe
approach is to truncate the tables manually before a full reload:

    psql -U <user> -d nyc_taxi -c "TRUNCATE taxi_trips, rejected_taxi_trips RESTART IDENTITY;"

The script never issues DROP TABLE or TRUNCATE automatically.

Design notes — transactions
---------------------------
Each chunk is wrapped in a single database transaction (SQLAlchemy's default
behaviour with engine.begin()).  If any INSERT within a chunk fails, the
entire chunk is rolled back and the error is logged.  The script continues
with the next chunk and reports the error count at the end.  A nonzero exit
code is returned when any chunk fails.
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Project imports — allow running from repo root or scripts/ directory
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.transform import transform_data  # noqa: E402

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Column rename mapping: DataFrame → PostgreSQL
# Four TLC mixed-case column names are lowercased in the DB schema.
# All other column names are identical between the DataFrame and the DB.
# ---------------------------------------------------------------------------
RENAME_MAP: dict[str, str] = {
    "VendorID"    : "vendor_id",
    "RatecodeID"  : "ratecode_id",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
}

# DB-generated columns — must NOT be sent in INSERT statements.
# trip_id    : GENERATED ALWAYS AS IDENTITY in taxi_trips
# rejected_id: GENERATED ALWAYS AS IDENTITY in rejected_taxi_trips
# rejected_at: DEFAULT NOW() in rejected_taxi_trips
GENERATED_ACCEPTED  = {"trip_id"}
GENERATED_REJECTED  = {"rejected_id", "rejected_at"}


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def _parse_args(argv=None) -> argparse.Namespace:
    load_dotenv()  # load .env if present

    p = argparse.ArgumentParser(
        description="Load NYC Taxi CSV into PostgreSQL via transform_data().",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--input", required=True, metavar="CSV_PATH",
        help="Path to the source CSV file.",
    )
    p.add_argument("--host",     default=os.getenv("DB_HOST",     "localhost"))
    p.add_argument("--port",     default=os.getenv("DB_PORT",     "5432"),     type=int)
    p.add_argument("--dbname",   default=os.getenv("DB_NAME",     "nyc_taxi"))
    p.add_argument("--user",     default=os.getenv("DB_USER",     "postgres"))
    p.add_argument("--password", default=os.getenv("DB_PASSWORD", ""))
    p.add_argument(
        "--chunk-size", dest="chunk_size", default=100_000, type=int,
        help="Rows per CSV read chunk.",
    )
    p.add_argument(
        "--batch-size", dest="batch_size", default=10_000, type=int,
        help="Rows per INSERT batch inside to_sql.",
    )
    p.add_argument(
        "--skip-rejected", dest="skip_rejected", action="store_true",
        help="Do not load rejected rows into rejected_taxi_trips.",
    )
    p.add_argument(
        "--dry-run", dest="dry_run", action="store_true",
        help="Parse and transform only; do not connect to the database.",
    )
    return p.parse_args(argv)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def _build_engine(args: argparse.Namespace):
    """Return a SQLAlchemy engine using the supplied connection parameters."""
    url = (
        f"postgresql+psycopg2://{args.user}:{args.password}"
        f"@{args.host}:{args.port}/{args.dbname}"
    )
    return create_engine(url, pool_pre_ping=True)


def _check_tables_exist(engine) -> None:
    """
    Raise RuntimeError if either target table is missing.
    Member 3 must run sql/create_tables.sql before loading.
    """
    required = {"taxi_trips", "rejected_taxi_trips"}
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT tablename FROM pg_tables "
                "WHERE schemaname = 'public' AND tablename = ANY(:names)"
            ),
            {"names": list(required)},
        ).fetchall()
    found = {r[0] for r in rows}
    missing = required - found
    if missing:
        raise RuntimeError(
            f"Required tables not found: {missing}. "
            "Run: psql -U <user> -d nyc_taxi -f sql/create_tables.sql"
        )


# ---------------------------------------------------------------------------
# DataFrame preparation helpers
# ---------------------------------------------------------------------------

def _prepare_accepted(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename mixed-case columns, drop any DB-generated columns if accidentally
    present, and ensure pandas NA/NaN values map to SQL NULL.

    Note on NULL handling:
    - pandas Int64 <NA> → psycopg2 NULL  (handled automatically by SQLAlchemy)
    - float64 NaN       → psycopg2 NULL  (SQLAlchemy maps NaN to NULL for NUMERIC)
    - airport_fee is the only nullable float column; NaN remains NaN here,
      which SQLAlchemy correctly sends as NULL to PostgreSQL NUMERIC(12,2).
    The explicit where() call below converts object-column Nones consistently
    and is kept for clarity, even though float NaN is handled by SQLAlchemy.
    """
    df = df.copy()
    df = df.rename(columns=RENAME_MAP)
    # Drop any generated columns that should not be in the INSERT
    df = df.drop(columns=[c for c in GENERATED_ACCEPTED if c in df.columns])
    # For object-dtype columns, replace None/NaN with None explicitly.
    # Float and Int64 columns are handled correctly by SQLAlchemy/psycopg2.
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].where(pd.notnull(df[col]), None)
    return df


def _prepare_rejected(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename mixed-case columns, drop generated columns, convert datetime
    columns to string (rejected_taxi_trips stores them as TEXT to tolerate
    NaT values), cast is_weekend to str, and normalise object-column NULLs.
    """
    df = df.copy()
    df = df.rename(columns=RENAME_MAP)
    df = df.drop(columns=[c for c in GENERATED_REJECTED if c in df.columns])

    # Cast timestamp columns to str — the DB column is TEXT
    for col in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
        if col in df.columns:
            df[col] = df[col].astype(str)

    # Cast is_weekend to str (DB column is TEXT in rejected table)
    if "is_weekend" in df.columns:
        df["is_weekend"] = df["is_weekend"].astype(str)

    # Normalise object-column NULLs to None for psycopg2
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].where(pd.notnull(df[col]), None)
    return df


# ---------------------------------------------------------------------------
# Core insertion with transaction-per-chunk
# ---------------------------------------------------------------------------

def _insert_chunk(
    engine,
    clean_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    batch_size: int,
    skip_rejected: bool,
) -> tuple[int, int]:
    """
    Insert one chunk's clean and rejected rows inside a single transaction.
    Returns (n_accepted_inserted, n_rejected_inserted).
    Raises on any DB error (caller handles rollback via context manager).
    """
    accepted_prep = _prepare_accepted(clean_df)
    n_accepted = len(accepted_prep)

    with engine.begin() as conn:
        if n_accepted > 0:
            accepted_prep.to_sql(
                name="taxi_trips",
                con=conn,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=batch_size,
            )

        n_rejected = 0
        if not skip_rejected and len(rejected_df) > 0:
            rejected_prep = _prepare_rejected(rejected_df)
            n_rejected = len(rejected_prep)
            rejected_prep.to_sql(
                name="rejected_taxi_trips",
                con=conn,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=batch_size,
            )

    return n_accepted, n_rejected


# ---------------------------------------------------------------------------
# Main loader
# ---------------------------------------------------------------------------

def run(argv=None) -> int:
    """
    Entry point.  Returns 0 on full success, 1 if any chunk failed.
    """
    args = _parse_args(argv)
    csv_path = Path(args.input)

    if not csv_path.exists():
        logger.error("CSV file not found: %s", csv_path)
        return 1

    # ----- dry-run: parse + transform only ---------------------------------
    if args.dry_run:
        logger.info("[DRY RUN] Reading and transforming %s (no DB writes)", csv_path)
        total_input = total_clean = total_rejected = 0
        t0 = time.perf_counter()
        for chunk in pd.read_csv(
            csv_path,
            chunksize=args.chunk_size,
            parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"],
        ):
            result = transform_data(chunk)
            total_input    += result["summary"]["n_input"]
            total_clean    += result["summary"]["n_clean"]
            total_rejected += result["summary"]["n_rejected"]
        elapsed = time.perf_counter() - t0
        logger.info(
            "[DRY RUN] Done in %.1f s | input=%d  accepted=%d  rejected=%d",
            elapsed, total_input, total_clean, total_rejected,
        )
        return 0

    # ----- live run ---------------------------------------------------------
    engine = _build_engine(args)
    try:
        _check_tables_exist(engine)
    except RuntimeError as exc:
        logger.error("%s", exc)
        return 1

    logger.info(
        "Connected to %s@%s:%s/%s",
        args.user, args.host, args.port, args.dbname,
    )
    logger.info(
        "Settings: chunk_size=%d  batch_size=%d  skip_rejected=%s",
        args.chunk_size, args.batch_size, args.skip_rejected,
    )

    # Count total rows for progress bar (fast row count from file size heuristic)
    # Read actual count by counting lines minus header
    logger.info("Counting source rows (scanning file header)...")
    try:
        # Quick estimate: use wc-like approach without loading data
        total_source_rows: int = sum(1 for _ in open(csv_path, "r", encoding="utf-8")) - 1
    except Exception:
        total_source_rows = 0
    logger.info("Source rows: %d", total_source_rows)

    total_input     = 0
    total_accepted  = 0
    total_rejected  = 0
    failed_chunks   = 0
    t_start         = time.perf_counter()
    chunk_num       = 0

    estimated_chunks = max(1, total_source_rows // args.chunk_size + 1) if total_source_rows else None

    with tqdm(
        total=total_source_rows or None,
        unit="rows",
        desc="Loading",
        dynamic_ncols=True,
    ) as pbar:
        for chunk in pd.read_csv(
            csv_path,
            chunksize=args.chunk_size,
            parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"],
        ):
            chunk_num += 1
            n_in_chunk = len(chunk)

            # Transform
            try:
                result = transform_data(chunk)
            except Exception as exc:
                logger.error("Chunk %d: transform_data() failed: %s", chunk_num, exc)
                failed_chunks += 1
                pbar.update(n_in_chunk)
                total_input += n_in_chunk
                continue

            clean_df    = result["clean_df"]
            rejected_df = result["rejected_df"]
            n_clean     = result["summary"]["n_clean"]
            n_rejected  = result["summary"]["n_rejected"]

            # Insert
            try:
                n_ins_accepted, n_ins_rejected = _insert_chunk(
                    engine, clean_df, rejected_df,
                    args.batch_size, args.skip_rejected,
                )
            except Exception as exc:
                logger.error(
                    "Chunk %d: DB insert failed (rolled back): %s",
                    chunk_num, exc,
                )
                failed_chunks += 1
                pbar.update(n_in_chunk)
                total_input    += n_in_chunk
                total_accepted += 0
                total_rejected += 0
                continue

            total_input    += n_in_chunk
            total_accepted += n_ins_accepted
            total_rejected += n_ins_rejected

            pbar.update(n_in_chunk)
            pbar.set_postfix(
                accepted=total_accepted,
                rejected=total_rejected,
                failed_chunks=failed_chunks,
            )

    elapsed = time.perf_counter() - t_start

    # ----- final report -----------------------------------------------------
    print()
    print("=" * 60)
    print("  NYC TAXI ETL LOAD COMPLETE")
    print("=" * 60)
    print(f"  Elapsed time    : {elapsed:.1f}s")
    print(f"  Source rows     : {total_input:>12,}")
    print(f"  Accepted → DB   : {total_accepted:>12,}  (taxi_trips)")
    print(f"  Rejected → DB   : {total_rejected:>12,}  (rejected_taxi_trips)")
    print(f"  Check (acc+rej) : {total_accepted + total_rejected:>12,}  == source? {total_accepted + total_rejected == total_input}")
    print(f"  Failed chunks   : {failed_chunks:>12}")
    print(f"  Meets 2M req.   : {'YES' if total_accepted >= 2_000_000 else 'NO (run verification SQL)'}")
    print("=" * 60)

    if failed_chunks > 0:
        logger.error("%d chunk(s) failed to load. See errors above.", failed_chunks)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(run())
