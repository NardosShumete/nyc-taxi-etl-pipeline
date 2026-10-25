-- =============================================================================
-- create_tables.sql
-- Member 2: PostgreSQL Schema for NYC Taxi ETL Pipeline
--
-- Creates:
--   taxi_trips          -- accepted, validated trip records (main table)
--   rejected_taxi_trips -- audit table for rows rejected by Member 1's
--                          validate_trips() function
--
-- Requires: PostgreSQL 13 or later (uses GENERATED ALWAYS AS IDENTITY)
-- Compatible with: psycopg2 2.9.9, SQLAlchemy 2.0.x (Member 3 loading layer)
--
-- Usage:
--   psql -U <user> -d nyc_taxi -f sql/create_tables.sql
--
-- Schema design notes and Member 3 loading instructions: docs/database.md
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 0.  Database / schema prerequisites
--     Run the following once as a superuser, then connect to nyc_taxi:
--
--       CREATE DATABASE nyc_taxi;
--       \c nyc_taxi
--
--     This file is safe to re-run: all CREATE statements use IF NOT EXISTS.
-- -----------------------------------------------------------------------------


-- =============================================================================
-- IDENTIFIER CASING STRATEGY
--
-- All column names in this schema use lowercase snake_case.
-- PostgreSQL folds unquoted identifiers to lowercase, so using lowercase
-- consistently means no identifier ever needs quoting in queries or application
-- code.
--
-- The source DataFrame (produced by src/transform.py) uses mixed-case names
-- for four columns that come directly from the TLC data dictionary:
--
--   DataFrame column   ->   Database column
--   ─────────────────────────────────────────
--   VendorID           ->   vendor_id
--   RatecodeID         ->   ratecode_id
--   PULocationID       ->   pu_location_id
--   DOLocationID       ->   do_location_id
--
-- Member 3 must apply this rename mapping before calling to_sql().
-- All other DataFrame columns already match their database column names
-- exactly.  See docs/database.md Section 9 for the complete rename dict
-- and loading code.
-- =============================================================================


-- =============================================================================
-- TABLE: taxi_trips
--
-- Stores every row that passes Member 1's validate_trips() quality checks.
-- Source: result["clean_df"] from src/transform.py::transform_data().
--
-- ACTUAL COLUMN COUNT: 25 source columns (all columns the CSV contains after
-- normalize_columns() runs, minus rejection_reason which is dropped for
-- clean_df).  The transform.py module docstring lists only 19 columns but
-- transform_data() does not select or drop the additional 6 columns
-- (vendor_id, improvement_surcharge, cbd_congestion_fee, pickup_hour,
-- day_of_week, is_weekend).  All 25 are included in the schema here.
-- =============================================================================

CREATE TABLE IF NOT EXISTS taxi_trips (

    -- -------------------------------------------------------------------------
    -- Surrogate primary key.
    -- BIGINT covers up to 9.2 × 10^18 rows; safe for ~3.9 M source rows and
    -- any future reloads.  GENERATED ALWAYS prevents accidental overwrites.
    -- Member 3 must NOT include trip_id in INSERT statements or to_sql calls.
    -- -------------------------------------------------------------------------
    trip_id                 BIGINT          GENERATED ALWAYS AS IDENTITY
                                            PRIMARY KEY,

    -- -------------------------------------------------------------------------
    -- Timestamps.
    -- TIMESTAMP WITHOUT TIME ZONE: all TLC records are in America/New_York
    -- local time.  No UTC offset is present in the source.  Converting to
    -- TIMESTAMPTZ requires a team timezone policy decision (see
    -- docs/database.md Section 11, item 2).
    -- Verified date range: 2007-12-05 through 2025-12-01 (docs/dataset.md §5).
    -- -------------------------------------------------------------------------
    tpep_pickup_datetime    TIMESTAMP       NOT NULL,
    tpep_dropoff_datetime   TIMESTAMP       NOT NULL,

    -- -------------------------------------------------------------------------
    -- Passenger count.
    -- Validated range 1–6 by validate_trips() (rule 9).
    -- Source dtype: Int64 (pandas nullable integer after convert_data_types).
    -- -------------------------------------------------------------------------
    passenger_count         SMALLINT        NOT NULL
                                CHECK (passenger_count BETWEEN 1 AND 6),

    -- -------------------------------------------------------------------------
    -- Trip distance (miles).
    -- Validated > 0 and ≤ 500 by validate_trips() (rule 3).
    -- NUMERIC(10,4): supports values up to 999,999.9999; 4 decimal places
    -- for sub-mile precision.
    -- -------------------------------------------------------------------------
    trip_distance           NUMERIC(10, 4)  NOT NULL
                                CHECK (trip_distance > 0),

    -- -------------------------------------------------------------------------
    -- Rate code.
    -- DataFrame column name: RatecodeID  ->  DB column: ratecode_id
    -- Valid TLC values: 1–6 and 99 (Unknown).  Validated by rule 10.
    -- SMALLINT is sufficient (max value 99).
    -- -------------------------------------------------------------------------
    ratecode_id             SMALLINT        NOT NULL
                                CHECK (ratecode_id IN (1, 2, 3, 4, 5, 6, 99)),

    -- -------------------------------------------------------------------------
    -- Store-and-forward flag.
    -- Values normalised to 'Y' or 'N' by convert_data_types().
    -- CHAR(1) is exact; CHECK constraint enforces the two valid values.
    -- -------------------------------------------------------------------------
    store_and_fwd_flag      CHAR(1)         NOT NULL
                                CHECK (store_and_fwd_flag IN ('Y', 'N')),

    -- -------------------------------------------------------------------------
    -- Location IDs.
    -- DataFrame column names: PULocationID, DOLocationID
    -- ->  DB columns: pu_location_id, do_location_id
    -- TLC zone range 1–265.  Validated by rule 12.
    -- SMALLINT covers the range (max value 32,767).
    -- -------------------------------------------------------------------------
    pu_location_id          SMALLINT        NOT NULL
                                CHECK (pu_location_id BETWEEN 1 AND 265),

    do_location_id          SMALLINT        NOT NULL
                                CHECK (do_location_id BETWEEN 1 AND 265),

    -- -------------------------------------------------------------------------
    -- Payment type: TLC codes 1–6.  Validated by rule 11.
    -- -------------------------------------------------------------------------
    payment_type            SMALLINT        NOT NULL
                                CHECK (payment_type BETWEEN 1 AND 6),

    -- -------------------------------------------------------------------------
    -- Financial columns.
    -- NUMERIC(12,2): exact decimal arithmetic, no floating-point rounding.
    -- Supports values up to 9,999,999,999.99 — well above the validated
    -- maximums ($10,000 fare, $15,000 total).
    --
    -- fare_amount:            validated ≥ 0 by rule 6.
    -- tip_amount:             validated ≥ 0 by rule 8.
    -- extra, mta_tax,
    -- cbd_congestion_fee:     negative values PERMITTED — they represent
    --                         legitimate correction entries (docs/dataset.md §9).
    -- tolls_amount, total_amount, congestion_surcharge,
    -- improvement_surcharge:  no lower-bound constraint — correction records
    --                         are accepted at the application layer.
    -- airport_fee:            NULLABLE — 757,387 rows (19.19%) have no
    --                         airport fee because the trip did not originate
    --                         at an airport (docs/dataset.md §9, §10 item 2).
    -- -------------------------------------------------------------------------
    fare_amount             NUMERIC(12, 2)  NOT NULL
                                CHECK (fare_amount >= 0),

    extra                   NUMERIC(12, 2)  NOT NULL,

    mta_tax                 NUMERIC(12, 2)  NOT NULL,

    tip_amount              NUMERIC(12, 2)  NOT NULL
                                CHECK (tip_amount >= 0),

    tolls_amount            NUMERIC(12, 2)  NOT NULL,

    improvement_surcharge   NUMERIC(12, 2)  NOT NULL,

    total_amount            NUMERIC(12, 2)  NOT NULL,

    congestion_surcharge    NUMERIC(12, 2)  NOT NULL,

    airport_fee             NUMERIC(12, 2)  NULL,

    cbd_congestion_fee      NUMERIC(12, 2)  NOT NULL,

    -- -------------------------------------------------------------------------
    -- Vendor ID.
    -- DataFrame column name: VendorID  ->  DB column: vendor_id
    -- 1 = Creative Mobile Technologies, 2 = VeriFone.
    -- SMALLINT sufficient (values 1–2).
    -- -------------------------------------------------------------------------
    vendor_id               SMALLINT        NOT NULL,

    -- -------------------------------------------------------------------------
    -- Publisher-engineered features.
    --
    -- speed_mph:    validated 0 < x ≤ 100 by rules 4 and 5.
    --               NUMERIC(8,4): stores values like 72.3456 precisely.
    -- duration_min: validated 0 < x ≤ 1,440 (24 h) by rule 14.
    --               NUMERIC(10,4): handles sub-minute precision.
    -- pickup_hour:  range 0–23; SMALLINT.
    -- day_of_week:  full day name; longest is "Wednesday" (9 chars).
    --               VARCHAR(9) is exact; CHECK enforces all 7 valid names.
    -- is_weekend:   BOOLEAN, derived from pickup date.
    -- -------------------------------------------------------------------------
    speed_mph               NUMERIC(8, 4)   NOT NULL
                                CHECK (speed_mph > 0),

    duration_min            NUMERIC(10, 4)  NOT NULL
                                CHECK (duration_min > 0),

    pickup_hour             SMALLINT        NOT NULL
                                CHECK (pickup_hour BETWEEN 0 AND 23),

    day_of_week             VARCHAR(9)      NOT NULL
                                CHECK (day_of_week IN (
                                    'Monday', 'Tuesday', 'Wednesday',
                                    'Thursday', 'Friday', 'Saturday', 'Sunday'
                                )),

    is_weekend              BOOLEAN         NOT NULL

);


-- =============================================================================
-- INDEXES on taxi_trips
--
-- Three targeted indexes, one per high-value query pattern.
-- Deliberately limited: bulk-loading 3.9 M rows with many indexes is
-- noticeably slower.  Additional indexes can be added after the initial
-- load with CREATE INDEX CONCURRENTLY.
--
-- 1. idx_taxi_trips_pickup_dt
--    Time-range queries, yearly groupings, MIN/MAX scans.
--    Used by verification_queries.sql Sections 2, 6.
--
-- 2. idx_taxi_trips_pu_location
--    Pickup-zone aggregations — the most common analytical grouping.
--
-- 3. idx_taxi_trips_do_location
--    Drop-off zone aggregations.
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_taxi_trips_pickup_dt
    ON taxi_trips (tpep_pickup_datetime);

CREATE INDEX IF NOT EXISTS idx_taxi_trips_pu_location
    ON taxi_trips (pu_location_id);

CREATE INDEX IF NOT EXISTS idx_taxi_trips_do_location
    ON taxi_trips (do_location_id);


-- =============================================================================
-- TABLE: rejected_taxi_trips
--
-- Audit table for rows that failed Member 1's validate_trips() checks.
-- Populated from result["rejected_df"] returned by transform_data().
--
-- rejected_df structure (src/transform.py):
--   All 25 source columns (after normalize_columns + convert_data_types)
--   PLUS a "rejection_reason" column (str, never None for rejected rows).
--
-- Design decisions:
--   - Datetime columns are stored as TEXT to survive values like 'NaT'
--     that cannot be cast to TIMESTAMP.
--   - All numeric columns use NUMERIC (no precision limit) to accept values
--     that violated the strict constraints in taxi_trips — e.g. a fare of
--     -999.99 cannot go into taxi_trips but must be auditable here.
--   - Mixed-case DataFrame column names are also lowercased here:
--       VendorID   -> vendor_id
--       RatecodeID -> ratecode_id
--       PULocationID -> pu_location_id
--       DOLocationID -> do_location_id
--   - rejection_reason is NOT NULL: every row in rejected_df has one.
--   - rejected_at records the server timestamp of insertion.
-- =============================================================================

CREATE TABLE IF NOT EXISTS rejected_taxi_trips (

    rejected_id             BIGINT          GENERATED ALWAYS AS IDENTITY
                                            PRIMARY KEY,

    tpep_pickup_datetime    TEXT,
    tpep_dropoff_datetime   TEXT,

    vendor_id               NUMERIC,
    passenger_count         NUMERIC,
    trip_distance           NUMERIC,
    ratecode_id             NUMERIC,
    store_and_fwd_flag      TEXT,
    pu_location_id          NUMERIC,
    do_location_id          NUMERIC,
    payment_type            NUMERIC,
    fare_amount             NUMERIC,
    extra                   NUMERIC,
    mta_tax                 NUMERIC,
    tip_amount              NUMERIC,
    tolls_amount            NUMERIC,
    improvement_surcharge   NUMERIC,
    total_amount            NUMERIC,
    congestion_surcharge    NUMERIC,
    airport_fee             NUMERIC,
    cbd_congestion_fee      NUMERIC,
    speed_mph               NUMERIC,
    duration_min            NUMERIC,
    pickup_hour             NUMERIC,
    day_of_week             TEXT,
    is_weekend              TEXT,

    rejection_reason        TEXT            NOT NULL,
    rejected_at             TIMESTAMP       NOT NULL DEFAULT NOW()

);

CREATE INDEX IF NOT EXISTS idx_rejected_reason
    ON rejected_taxi_trips (rejection_reason);
