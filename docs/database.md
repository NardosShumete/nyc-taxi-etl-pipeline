# Database Documentation — PostgreSQL Schema Design

**Member 2 — PostgreSQL Database Schema**

---

## 1. Prerequisites

| Requirement | Minimum version | Notes |
|---|---|---|
| PostgreSQL | 13 | Uses `GENERATED ALWAYS AS IDENTITY`, `CREATE INDEX IF NOT EXISTS`, and `FILTER (WHERE …)` aggregate syntax. All are available since PG 10–12; PG 13 is the recommended baseline. |
| psycopg2-binary | 2.9.9 | Pinned in `requirements.txt`. Used by Member 3's loading layer. |
| SQLAlchemy | 2.0.30 | Pinned in `requirements.txt`. Optional ORM layer for Member 3. |
| python-dotenv | 1.0.1 | Loads connection parameters from `.env`. |

PostgreSQL must be running and accessible before loading begins.

---

## 2. Database Creation

Connect as a PostgreSQL superuser and run the following once:

```sql
CREATE DATABASE nyc_taxi;
```

Then connect to that database for all subsequent steps:

```bash
psql -U <your_pg_user> -d nyc_taxi
```

Or with a connection string:

```bash
psql "postgresql://<user>:<password>@<host>:<port>/nyc_taxi"
```

---

## 3. Connection Parameters

Store connection details in a `.env` file at the project root (**never commit this file** — it is already listed in `.gitignore`).

```dotenv
# .env  — example only, replace with real values
DB_HOST=localhost
DB_PORT=5432
DB_NAME=nyc_taxi
DB_USER=postgres
DB_PASSWORD=your_password_here
```

Python loading code should read these with `python-dotenv`:

```python
import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "nyc_taxi")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")
```

---

## 4. Executing the SQL Scripts

### 4a. Create tables and indexes

```bash
psql -U <user> -d nyc_taxi -f sql/create_tables.sql
```

Both `CREATE TABLE IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS` are used, so re-running the script is safe and idempotent — it will not raise an error if the objects already exist.

### 4b. Run verification queries

After Member 3 has completed loading:

```bash
psql -U <user> -d nyc_taxi -f sql/verification_queries.sql
```

Or run individual sections in psql or a GUI tool (pgAdmin, DBeaver, TablePlus).

---

## 5. Schema Design and Type Choices

### 5a. Primary key

`trip_id BIGINT GENERATED ALWAYS AS IDENTITY` is a server-generated surrogate key.

The source dataset has no natural unique key. Using a surrogate key avoids any risk of duplicates across partial loads and is safe for reloads. `BIGINT` supports up to 9.2 × 10¹⁸ rows, which is future-proof well beyond the ~3.9 M source rows.

### 5b. Timestamps

`TIMESTAMP WITHOUT TIME ZONE` — all TLC records are implicitly in America/New_York local time. No UTC offset is stored in the source file. Storing `TIMESTAMP` rather than `TIMESTAMPTZ` avoids an implicit timezone conversion that would require a policy decision the team has not yet made.

The verified date range is 2007-12-05 through 2025-12-01. Standard `TIMESTAMP` handles this without issue.

### 5c. Integer fields

| Column | Type | Reason |
|---|---|---|
| `passenger_count` | `SMALLINT` | Range 1–6; max SMALLINT 32767. |
| `RatecodeID` | `SMALLINT` | Valid values 1–6, 99. |
| `PULocationID`, `DOLocationID` | `SMALLINT` | Range 1–265. |
| `payment_type` | `SMALLINT` | Values 1–6. |
| `VendorID` | `SMALLINT` | Values 1–2. |
| `pickup_hour` | `SMALLINT` | Range 0–23. |

`SMALLINT` uses 2 bytes vs `INTEGER`'s 4 bytes, saving ~30 MB over 3.9 M rows for these six columns combined.

### 5d. Monetary columns

All financial columns use `NUMERIC(12, 2)`: exact decimal arithmetic, no floating-point rounding errors, supports values up to 9,999,999,999.99. This comfortably covers the validated maximums ($10 k fare, $15 k total).

`FLOAT` is explicitly avoided for financial data to prevent cent-level rounding errors.

### 5e. Distance, duration, speed

| Column | Type | Notes |
|---|---|---|
| `trip_distance` | `NUMERIC(10, 4)` | Up to 999,999.9999 miles; 4 decimal places for sub-mile precision. |
| `duration_min` | `NUMERIC(10, 4)` | Validated range 0 < x ≤ 1440 (24 h); sub-minute precision needed. |
| `speed_mph` | `NUMERIC(8, 4)` | Validated range 0 < x ≤ 100; `NUMERIC` avoids float imprecision in cross-checks. |

### 5f. String columns

| Column | Type | Notes |
|---|---|---|
| `store_and_fwd_flag` | `CHAR(1)` | Always 'Y' or 'N'; fixed width optimal. |
| `day_of_week` | `VARCHAR(9)` | Longest day name is "Wednesday" (9 chars); constraint enumerates all 7 names. |

### 5g. Boolean

`is_weekend BOOLEAN NOT NULL` — the engineered feature from Member 1 maps cleanly to a native PostgreSQL boolean.

---

## 6. Constraints

| Column | Constraint | Rationale |
|---|---|---|
| `passenger_count` | `BETWEEN 1 AND 6` | TLC valid range; validated by Member 1 before insert. |
| `trip_distance` | `> 0` | Non-positive distances are rejected by Member 1. |
| `RatecodeID` | `IN (1,2,3,4,5,6,99)` | TLC specification. |
| `store_and_fwd_flag` | `IN ('Y','N')` | Only two valid values. |
| `PULocationID`, `DOLocationID` | `BETWEEN 1 AND 265` | TLC zone range. |
| `payment_type` | `BETWEEN 1 AND 6` | TLC specification. |
| `fare_amount` | `>= 0` | Negative fares rejected by Member 1. |
| `tip_amount` | `>= 0` | Negative tips rejected by Member 1. |
| `speed_mph` | `> 0` | Non-positive speed rejected by Member 1. |
| `duration_min` | `> 0` | Non-positive duration rejected by Member 1. |
| `pickup_hour` | `BETWEEN 0 AND 23` | Valid hour range. |
| `day_of_week` | `IN ('Monday', …, 'Sunday')` | Exactly 7 valid day names. |

**Columns with no lower-bound constraint**: `extra`, `mta_tax`, `cbd_congestion_fee`, `tolls_amount`, `total_amount`, `congestion_surcharge`, `improvement_surcharge`. Negative values are intentionally permitted for `extra`, `mta_tax`, and `cbd_congestion_fee` because they represent legitimate correction entries (documented in `docs/dataset.md`, Section 9).

**No foreign keys** to external taxi-zone lookup tables — those tables are not part of this assignment's scope.

---

## 7. Indexes

| Index | Column | Justification |
|---|---|---|
| `idx_taxi_trips_pickup_dt` | `tpep_pickup_datetime` | Time-range filters, yearly groupings (`GROUP BY EXTRACT(YEAR …)`), and `MIN`/`MAX` lookups in verification queries. |
| `idx_taxi_trips_pu_location` | `PULocationID` | Zone-level pickup aggregations. |
| `idx_taxi_trips_do_location` | `DOLocationID` | Zone-level drop-off aggregations. |
| `idx_rejected_reason` | `rejection_reason` | Rejection-reason breakdown queries in Section 10 of `verification_queries.sql`. |

Indexes on low-cardinality or rarely-filtered columns (`payment_type`, `RatecodeID`, `is_weekend`) are intentionally omitted at this stage. Bulk-loading ~3.9 M rows with many indexes is noticeably slower. Additional indexes can be created after the initial load with `CREATE INDEX CONCURRENTLY` if query profiling shows they help.

---

## 8. Rejected-Row Audit Table

`rejected_taxi_trips` stores every row that Member 1's `validate_trips()` function rejects, along with the `rejection_reason` string from the returned `rejected_df`.

All source-column types are permissive (`TEXT`, `NUMERIC`) because rejected rows may contain malformed or out-of-range values that the strict `taxi_trips` constraints would refuse. Timestamps are stored as `TEXT` to survive values like `'NaT'`.

Loading rejected rows is **optional** for Member 3 but strongly recommended for audit completeness. See Section 9 below for the loading API.

---

## 9. Instructions for Member 3 — Loading Rows

### 9a. Accepted trips

Feed `result["clean_df"]` from `transform_data()` directly to the PostgreSQL loader. The columns in `clean_df` map to `taxi_trips` columns one-to-one (column order is declared in `src/transform.py`'s module docstring).

The `trip_id` column is server-generated — **do not include it in INSERT statements or DataFrame writes**.

Example with `pandas.DataFrame.to_sql` (SQLAlchemy engine):

```python
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

result = transform_data(raw_df)
clean_df = result["clean_df"]

clean_df.to_sql(
    name="taxi_trips",
    con=engine,
    if_exists="append",   # table already created by create_tables.sql
    index=False,           # do not write DataFrame index as a column
    method="multi",        # batch INSERTs for performance
    chunksize=10_000,      # tune for memory / speed tradeoff
)
```

> **Column name quoting**: `RatecodeID`, `PULocationID`, `DOLocationID`, and `VendorID` use mixed case in both the DataFrame and the schema. SQLAlchemy/psycopg2 will quote these automatically when using `to_sql`. If writing raw SQL, always double-quote these identifiers:
> ```sql
> INSERT INTO taxi_trips ("VendorID", "RatecodeID", "PULocationID", "DOLocationID", …)
> VALUES (…);
> ```

### 9b. Rejected trips (optional audit)

```python
rejected_df = result["rejected_df"]

# Convert timestamp columns to string to handle NaT safely
rejected_df = rejected_df.copy()
for col in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
    if col in rejected_df.columns:
        rejected_df[col] = rejected_df[col].astype(str)

rejected_df.to_sql(
    name="rejected_taxi_trips",
    con=engine,
    if_exists="append",
    index=False,
    method="multi",
    chunksize=10_000,
)
```

### 9c. Chunked loading for the full 3.9 M row file

For the full dataset Member 3 should load in chunks to avoid memory pressure:

```python
import pandas as pd

CHUNK_SIZE = 500_000

for chunk in pd.read_csv(
    "data/raw/NYC_Taxi_Cleaned_Analysis_Ready.csv",
    chunksize=CHUNK_SIZE
):
    result = transform_data(chunk)
    result["clean_df"].to_sql("taxi_trips", engine,
                               if_exists="append", index=False,
                               method="multi", chunksize=10_000)
    # optionally: result["rejected_df"].to_sql("rejected_taxi_trips", ...)
```

---

## 10. Running Verification Queries

After loading completes, run all verification sections:

```bash
psql -U <user> -d nyc_taxi -f sql/verification_queries.sql
```

The most important checks are:
- **Section 1b** — confirms ≥ 2,000,000 rows are present.
- **Section 2c** — confirms no drop-off ≤ pickup violations slipped through.
- **Section 3a** — shows null counts across all columns.
- **Section 8a** — confirms no speed/duration inconsistencies.

---

## 11. Known Limitations and Decisions Requiring Team Confirmation

1. **`airport_fee` nullable**: 19.19% of rows are NULL in the source (`Airport_fee`). The schema marks this column `NULL`. If the team decides to default missing values to `0.00`, Member 3 should apply that substitution before insertion and `ALTER TABLE taxi_trips ALTER COLUMN airport_fee SET NOT NULL` can be run after backfilling.

2. **`TIMESTAMP` vs `TIMESTAMPTZ`**: The team should decide whether to store all timestamps in UTC. If so, Member 3 should apply `CONVERT_TZ` or pandas `.dt.tz_localize('America/New_York').dt.tz_convert('UTC')` before insertion, and this schema should be altered to `TIMESTAMPTZ`.

3. **No table partitioning**: The date range spans 2007–2025. With ~3.9 M rows PostgreSQL handles this comfortably without partitioning. If the dataset grows significantly, range partitioning on `tpep_pickup_datetime` by year can be added later.

4. **Negative `extra`, `mta_tax`, `cbd_congestion_fee`**: Currently accepted at the DB level. If the team decides to reject these at the application layer as well, Member 1 would need to add validation rules and this schema's constraints would stay unchanged.

5. **PostgreSQL not executed locally**: This schema was validated through careful static review of the SQL syntax and cross-checked against the actual DataFrame schema from `src/transform.py`. The `CREATE TABLE`, `CHECK`, and `CREATE INDEX` statements follow standard PostgreSQL 13+ syntax. Member 3 should execute `sql/create_tables.sql` in their local PostgreSQL instance and report any errors before proceeding with loading.

6. **Mixed-case column identifiers**: `VendorID`, `RatecodeID`, `PULocationID`, `DOLocationID` retain their original mixed-case names from the TLC data dictionary. In PostgreSQL these must always be double-quoted in raw SQL. `to_sql` via SQLAlchemy handles this automatically.

---

## 12. File Reference

| File | Description |
|---|---|
| `sql/create_tables.sql` | Creates `taxi_trips` and `rejected_taxi_trips` tables and indexes. |
| `sql/verification_queries.sql` | Post-load integrity and statistics queries. |
| `docs/database.md` | This document. |
| `src/transform.py` | Member 1's transformation pipeline; defines the `clean_df` column contract. |
| `docs/dataset.md` | Member 1's dataset documentation; source for nullable and type decisions. |
