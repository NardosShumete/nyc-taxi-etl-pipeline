# Database Documentation — PostgreSQL Schema Design

**Member 2 — PostgreSQL Database Schema**

---

## 1. Prerequisites

| Requirement | Minimum version | Notes |
|---|---|---|
| PostgreSQL | 13 | Uses `GENERATED ALWAYS AS IDENTITY` (PG 10+), `CREATE INDEX IF NOT EXISTS` (PG 9.5+), and `FILTER (WHERE …)` aggregate syntax (PG 9.4+). PG 13 is the recommended baseline. |
| psycopg2-binary | 2.9.9 | Pinned in `requirements.txt`. Used by Member 3's loading layer. |
| SQLAlchemy | 2.0.x | Pinned in `requirements.txt`. Used by Member 3 for `to_sql()`. |
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

---

## 3. Connection Parameters

Store connection details in a `.env` file at the project root. **Never commit this file** — it is listed in `.gitignore`.

```dotenv
# .env  — example only, replace with real values
DB_HOST=localhost
DB_PORT=5432
DB_NAME=nyc_taxi
DB_USER=postgres
DB_PASSWORD=your_password_here
```

Read in Python:

```python
import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST     = os.getenv("DB_HOST",     "localhost")
DB_PORT     = os.getenv("DB_PORT",     "5432")
DB_NAME     = os.getenv("DB_NAME",     "nyc_taxi")
DB_USER     = os.getenv("DB_USER",     "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")
```

---

## 4. Executing the SQL Scripts

### 4a. Create tables and indexes

```bash
psql -U <user> -d nyc_taxi -f sql/create_tables.sql
```

Both `CREATE TABLE IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS` are used throughout. The script is **idempotent** — re-running it against a database that already has the tables and indexes will produce no error and no change.

### 4b. Run verification queries

After Member 3 has completed loading:

```bash
psql -U <user> -d nyc_taxi -f sql/verification_queries.sql
```

Or run individual sections in psql or a GUI tool (pgAdmin, DBeaver, TablePlus).

### 4c. Schema changes when tables already exist

`CREATE TABLE IF NOT EXISTS` will silently skip table creation if the table already exists — it will **not** apply column additions or type changes. If the schema needs to be altered after initial creation, use `ALTER TABLE`:

```sql
-- Example: add a new column
ALTER TABLE taxi_trips ADD COLUMN IF NOT EXISTS new_col TEXT;
```

To rebuild from scratch in a development environment:

```sql
DROP TABLE IF EXISTS rejected_taxi_trips;
DROP TABLE IF EXISTS taxi_trips;
-- then re-run sql/create_tables.sql
```

---

## 5. Identifier Casing Strategy

### Why lowercase snake_case

PostgreSQL folds unquoted identifiers to lowercase. A table column defined as `"RatecodeID"` (with double quotes in `CREATE TABLE`) is stored with a mixed-case physical name and **must be double-quoted in every query**. Forgetting the quotes causes a `column "ratecodeid" does not exist` error at runtime.

Using all-lowercase snake_case means:
- No identifier ever needs quoting in SQL queries.
- `to_sql()` with SQLAlchemy reflection works without additional configuration.
- psql tab-completion and most GUI tools work transparently.

### Column rename mapping (DataFrame → Database)

Four TLC data-dictionary columns use mixed-case names in the source CSV and in the `clean_df` / `rejected_df` DataFrames. Member 3 must rename these before inserting:

| DataFrame column | Database column |
|---|---|
| `VendorID` | `vendor_id` |
| `RatecodeID` | `ratecode_id` |
| `PULocationID` | `pu_location_id` |
| `DOLocationID` | `do_location_id` |

All other columns have identical names in both the DataFrame and the database.

---

## 6. Schema Design and Type Choices

### 6a. Primary key

`trip_id BIGINT GENERATED ALWAYS AS IDENTITY` — a server-generated surrogate key. The source dataset has no natural unique key. `BIGINT` supports up to 9.2 × 10¹⁸ rows and is future-proof well beyond the ~3.9 M source rows. `GENERATED ALWAYS` prevents the loader from accidentally overwriting values.

**Member 3 must not include `trip_id` in INSERT statements or `to_sql()` calls.**

### 6b. Timestamps

`TIMESTAMP WITHOUT TIME ZONE` — TLC records carry America/New_York local time with no UTC offset in the source file. Storing `TIMESTAMPTZ` would require a team-agreed conversion policy (open item, Section 11). The verified date range 2007-12-05 through 2025-12-01 is fully representable by `TIMESTAMP`.

### 6c. Integer fields

| Column | DB type | Range | Reason |
|---|---|---|---|
| `passenger_count` | `SMALLINT` | 1–6 | TLC max is 6; SMALLINT max is 32,767 |
| `ratecode_id` | `SMALLINT` | 1–6, 99 | |
| `pu_location_id`, `do_location_id` | `SMALLINT` | 1–265 | TLC zone range |
| `payment_type` | `SMALLINT` | 1–6 | |
| `vendor_id` | `SMALLINT` | 1–2 | |
| `pickup_hour` | `SMALLINT` | 0–23 | |

`SMALLINT` (2 bytes) vs `INTEGER` (4 bytes) saves approximately 35 MB across these six columns over 3.9 M rows.

The source `passenger_count`, `ratecode_id`, `pu_location_id`, `do_location_id`, and `payment_type` are stored as pandas `Int64` (nullable integer) after `convert_data_types()` runs. PostgreSQL accepts these as `SMALLINT` without loss.

### 6d. Monetary columns

All financial columns use `NUMERIC(12, 2)`: exact decimal arithmetic, no floating-point rounding errors, values up to 9,999,999,999.99. This covers the validated maximums of $10,000 fare and $15,000 total.

`FLOAT` / `REAL` are explicitly avoided for financial data to prevent cent-level rounding errors.

### 6e. Distance, duration, speed

| Column | DB type | Precision notes |
|---|---|---|
| `trip_distance` | `NUMERIC(10, 4)` | Up to 999,999.9999 miles; 4 decimal places for sub-mile precision |
| `duration_min` | `NUMERIC(10, 4)` | Range 0–1,440; sub-minute precision required (e.g. 1.5033 min) |
| `speed_mph` | `NUMERIC(8, 4)` | Range 0–100; NUMERIC avoids float imprecision in the Section 8 cross-check |

### 6f. String columns

| Column | DB type | Notes |
|---|---|---|
| `store_and_fwd_flag` | `CHAR(1)` | Always 'Y' or 'N' after `convert_data_types()` normalisation |
| `day_of_week` | `VARCHAR(9)` | Longest day name is "Wednesday" (9 chars); CHECK enforces all 7 names |

### 6g. Boolean

`is_weekend BOOLEAN NOT NULL` — pandas `bool` dtype maps directly to PostgreSQL `BOOLEAN`.

---

## 7. Constraints

| Column | Constraint | Rationale |
|---|---|---|
| `passenger_count` | `BETWEEN 1 AND 6` | TLC valid range; enforced by validate_trips() rule 9 |
| `trip_distance` | `> 0` | Non-positive distances rejected by rule 3 |
| `ratecode_id` | `IN (1,2,3,4,5,6,99)` | TLC specification; rule 10 |
| `store_and_fwd_flag` | `IN ('Y','N')` | Two valid values after normalisation |
| `pu_location_id`, `do_location_id` | `BETWEEN 1 AND 265` | TLC zone range; rule 12 |
| `payment_type` | `BETWEEN 1 AND 6` | TLC specification; rule 11 |
| `fare_amount` | `>= 0` | Negative fares rejected by rule 6 |
| `tip_amount` | `>= 0` | Negative tips rejected by rule 8 |
| `speed_mph` | `> 0` | Non-positive speed rejected by rules 4–5 |
| `duration_min` | `> 0` | Non-positive duration rejected by rule 14 |
| `pickup_hour` | `BETWEEN 0 AND 23` | Valid hour range |
| `day_of_week` | `IN ('Monday', …, 'Sunday')` | All 7 valid day names |

**Columns with no lower-bound constraint**: `extra`, `mta_tax`, `cbd_congestion_fee`, `tolls_amount`, `total_amount`, `congestion_surcharge`, `improvement_surcharge`. Negative values for `extra`, `mta_tax`, and `cbd_congestion_fee` are legitimate correction entries (documented in `docs/dataset.md` Sections 9–10).

**No foreign keys** to external taxi-zone lookup tables — those are not part of this assignment.

---

## 8. Indexes

| Index name | Table column | Purpose |
|---|---|---|
| `idx_taxi_trips_pickup_dt` | `tpep_pickup_datetime` | Time-range queries, yearly `GROUP BY`, `MIN`/`MAX` scans (verification Sections 2, 6) |
| `idx_taxi_trips_pu_location` | `pu_location_id` | Pickup-zone aggregations |
| `idx_taxi_trips_do_location` | `do_location_id` | Drop-off zone aggregations |
| `idx_rejected_reason` | `rejection_reason` | Rejection-reason breakdown (verification Section 10) |

Indexes on low-cardinality columns (`payment_type`, `ratecode_id`, `is_weekend`) and financial columns are intentionally omitted at this stage: bulk-loading 3.9 M rows with many indexes is measurably slower. Add more after initial load using `CREATE INDEX CONCURRENTLY` if profiling shows it helps.

---

## 9. Instructions for Member 3 — Loading Data

### 9a. Complete column rename mapping

Apply this rename before any insert to align DataFrame column names with database column names:

```python
RENAME_MAP = {
    "VendorID"    : "vendor_id",
    "RatecodeID"  : "ratecode_id",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
}
```

All other 21 columns have identical names in the DataFrame and the database.

### 9b. Loading accepted trips (full example)

```python
import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from src.transform import transform_data

load_dotenv()

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

RENAME_MAP = {
    "VendorID"    : "vendor_id",
    "RatecodeID"  : "ratecode_id",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
}

CHUNK_SIZE = 500_000   # tune for available RAM

for chunk in pd.read_csv(
    "data/raw/NYC_Taxi_Cleaned_Analysis_Ready.csv",
    chunksize=CHUNK_SIZE
):
    result = transform_data(chunk)

    clean_df = result["clean_df"].rename(columns=RENAME_MAP)

    clean_df.to_sql(
        name="taxi_trips",
        con=engine,
        if_exists="append",   # table already created by create_tables.sql
        index=False,           # do not write the DataFrame index as a column
        method="multi",        # batched INSERTs (faster than row-by-row)
        chunksize=10_000,      # rows per batch INSERT
    )
```

**Important**: do not pass `trip_id` in the DataFrame — it is `GENERATED ALWAYS` on the server side and will cause an error if supplied.

### 9c. Loading rejected rows (optional audit)

```python
    rejected_df = result["rejected_df"].copy()

    # Rename mixed-case columns to match rejected_taxi_trips schema
    rejected_df = rejected_df.rename(columns=RENAME_MAP)

    # Convert timestamp columns to str to survive NaT values
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

### 9d. Column order does not matter for named inserts

`to_sql()` with SQLAlchemy uses column *names* (not positions) to build INSERT statements. The column order in the DataFrame does not need to match the column order in the CREATE TABLE statement, provided the names match after the rename.

### 9e. Handling `store_and_fwd_flag` CHAR(1) padding

PostgreSQL `CHAR(1)` right-pads values to the declared length on read-back but stores them without padding. A single-character string 'Y' or 'N' will insert and round-trip correctly.

---

## 10. Running Verification Queries

After loading completes, run the full verification suite:

```bash
psql -U <user> -d nyc_taxi -f sql/verification_queries.sql
```

**Key results to check:**

| Section | Query | Expected result |
|---|---|---|
| 1b | `meets_2_million_requirement` | `TRUE` if ≥ 2,000,000 accepted rows loaded |
| 2c | `dropoff_not_after_pickup_violations` | 0 |
| 3a | All null counts except `null_airport_fee` | 0 |
| 3b | `null_airport_fee_pct` | ~19% (validated source figure) |
| 8a | `speed_inconsistency_violations` | 0 |
| 8b | `over_100mph`, `zero_or_negative_mph` | 0 |

Section 1b reports the **actual database count** against the 2-million threshold. The source dataset having 3,946,698 rows does not guarantee 2 million will be accepted — Member 1's validation rules will reject some rows. Section 11 shows both accepted and rejected counts together.

---

## 11. Known Limitations and Open Decisions

1. **Static SQL validation only** — `sql/create_tables.sql` and `sql/verification_queries.sql` were validated through careful static review against the actual `src/transform.py` column contract and `docs/dataset.md`. PostgreSQL execution has not been verified in this environment. Member 3 must execute `sql/create_tables.sql` in a local PostgreSQL 13+ instance and confirm no errors before loading.

2. **`TIMESTAMP` vs `TIMESTAMPTZ`** — The team should decide whether to store timestamps in UTC. If so, Member 3 should apply `.dt.tz_localize('America/New_York').dt.tz_convert('UTC')` before insertion and the schema column type should be changed to `TIMESTAMPTZ`.

3. **`airport_fee` nullable** — 757,387 rows (19.19%) have NULL. If the team decides to default missing values to `0.00`, Member 3 should apply `clean_df["airport_fee"].fillna(0.00)` before insertion and then `ALTER TABLE taxi_trips ALTER COLUMN airport_fee SET NOT NULL` can be run after backfilling.

4. **No table partitioning** — With ~3.9 M rows PostgreSQL handles queries comfortably without partitioning. If the dataset grows significantly, range partitioning on `tpep_pickup_datetime` by year is the logical next step.

5. **`transform.py` module docstring discrepancy** — The `transform_data()` docstring lists 19 columns in `clean_df`, but the actual implementation calls only `drop(columns=["rejection_reason"])` and `reset_index()`. It does not select or reorder columns. The actual `clean_df` therefore contains all 25 source columns. This schema is written to match the actual 25-column output, not the 19-column docstring list.

6. **Negative `extra`, `mta_tax`, `cbd_congestion_fee`** — These are accepted at the DB level to allow correction records. If the team later decides to reject them at the application layer, Member 1 would need to add validation rules; no DB schema change would be required.

---

## 12. Complete Column Reference

| DB column | DataFrame column | PG type | Nullable | Constraint |
|---|---|---|---|---|
| `trip_id` | *(generated)* | `BIGINT IDENTITY` | NO | PK |
| `tpep_pickup_datetime` | `tpep_pickup_datetime` | `TIMESTAMP` | NO | |
| `tpep_dropoff_datetime` | `tpep_dropoff_datetime` | `TIMESTAMP` | NO | |
| `passenger_count` | `passenger_count` | `SMALLINT` | NO | `BETWEEN 1 AND 6` |
| `trip_distance` | `trip_distance` | `NUMERIC(10,4)` | NO | `> 0` |
| `ratecode_id` | `RatecodeID` | `SMALLINT` | NO | `IN (1,2,3,4,5,6,99)` |
| `store_and_fwd_flag` | `store_and_fwd_flag` | `CHAR(1)` | NO | `IN ('Y','N')` |
| `pu_location_id` | `PULocationID` | `SMALLINT` | NO | `BETWEEN 1 AND 265` |
| `do_location_id` | `DOLocationID` | `SMALLINT` | NO | `BETWEEN 1 AND 265` |
| `payment_type` | `payment_type` | `SMALLINT` | NO | `BETWEEN 1 AND 6` |
| `fare_amount` | `fare_amount` | `NUMERIC(12,2)` | NO | `>= 0` |
| `extra` | `extra` | `NUMERIC(12,2)` | NO | *(negatives OK)* |
| `mta_tax` | `mta_tax` | `NUMERIC(12,2)` | NO | *(negatives OK)* |
| `tip_amount` | `tip_amount` | `NUMERIC(12,2)` | NO | `>= 0` |
| `tolls_amount` | `tolls_amount` | `NUMERIC(12,2)` | NO | |
| `improvement_surcharge` | `improvement_surcharge` | `NUMERIC(12,2)` | NO | |
| `total_amount` | `total_amount` | `NUMERIC(12,2)` | NO | |
| `congestion_surcharge` | `congestion_surcharge` | `NUMERIC(12,2)` | NO | |
| `airport_fee` | `airport_fee` | `NUMERIC(12,2)` | **YES** | ~19% NULL |
| `cbd_congestion_fee` | `cbd_congestion_fee` | `NUMERIC(12,2)` | NO | *(negatives OK)* |
| `vendor_id` | `VendorID` | `SMALLINT` | NO | |
| `speed_mph` | `speed_mph` | `NUMERIC(8,4)` | NO | `> 0` |
| `duration_min` | `duration_min` | `NUMERIC(10,4)` | NO | `> 0` |
| `pickup_hour` | `pickup_hour` | `SMALLINT` | NO | `BETWEEN 0 AND 23` |
| `day_of_week` | `day_of_week` | `VARCHAR(9)` | NO | 7 valid day names |
| `is_weekend` | `is_weekend` | `BOOLEAN` | NO | |

---

## 13. File Reference

| File | Description |
|---|---|
| `sql/create_tables.sql` | Creates `taxi_trips` and `rejected_taxi_trips` tables and indexes. Idempotent. |
| `sql/verification_queries.sql` | 11 sections of post-load integrity and statistics queries. |
| `docs/database.md` | This document. |
| `src/transform.py` | Member 1's transformation pipeline; defines the `clean_df` and `rejected_df` column contract. |
| `docs/dataset.md` | Member 1's dataset documentation; source for nullable and type decisions. |
