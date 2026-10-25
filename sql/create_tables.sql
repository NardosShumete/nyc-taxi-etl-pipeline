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
-- Compatible with: psycopg2, SQLAlchemy 2.x (Member 3 loading layer)
--
-- Usage:
--   psql -U <user> -d nyc_taxi -f sql/create_tables.sql
--
-- Schema design notes are in docs/database.md
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 0.  Database / schema prerequisites
--     Run the commands below once as a superuser before executing this file:
--
--       CREATE DATABASE nyc_taxi;
--       \c nyc_taxi
--
--     This file should be executed inside the nyc_taxi database.
-- -----------------------------------------------------------------------------


-- =============================================================================
-- TABLE: taxi_trips
--
-- Stores every row that passes Member 1's validate_trips() quality checks.
-- Column definitions mirror the clean_df column contract declared in the
-- module docstring of src/transform.py, in the same order.
--
-- Column sources:
--   Raw TLC source fields: tpep_pickup_datetime, tpep_dropoff_datetime,
--     passenger_count, trip_distance, RatecodeID, store_and_fwd_flag,
--     PULocationID, DOLocationID, payment_type, fare_amount, extra,
--     mta_tax, tip_amount, tolls_amount, total_amount,
--     congestion_surcharge, airport_fee, improvement_surcharge,
--     cbd_congestion_fee, VendorID
--   Publisher-engineered features: speed_mph, duration_min,
--     pickup_hour, day_of_week, is_weekend
-- =============================================================================

CREATE TABLE IF NOT EXISTS taxi_trips (

    -- -------------------------------------------------------------------------
    -- Surrogate primary key
    -- BIGINT covers up to 9.2 × 10^18 rows; safe for the ~3.9 M source rows
    -- and future reloads.  GENERATED ALWAYS prevents accidental overwrites.
    -- -------------------------------------------------------------------------
    trip_id                 BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- -------------------------------------------------------------------------
    -- Timestamps
    -- TIMESTAMP WITHOUT TIME ZONE because all TLC records are implicitly in
    -- America/New_York local time and storing the offset would require a
    -- policy decision not yet made by the team.  The range 2007-12-05 through
    -- 2025-12-01 is fully representable by standard TIMESTAMP.
    -- -------------------------------------------------------------------------
    tpep_pickup_datetime    TIMESTAMP       NOT NULL,
    tpep_dropoff_datetime   TIMESTAMP       NOT NULL,

    -- -------------------------------------------------------------------------
    -- Passenger and distance
    -- -------------------------------------------------------------------------
    passenger_count         SMALLINT        NOT NULL
                                CHECK (passenger_count BETWEEN 1 AND 6),

    trip_distance           NUMERIC(10, 4)  NOT NULL
                                CHECK (trip_distance > 0),

    -- -------------------------------------------------------------------------
    -- Rate code: valid TLC values are 1–6 and 99 (Unknown).
    -- SMALLINT is sufficient (max value 99).
    -- -------------------------------------------------------------------------
    "RatecodeID"            SMALLINT        NOT NULL
                                CHECK ("RatecodeID" IN (1, 2, 3, 4, 5, 6, 99)),

    -- -------------------------------------------------------------------------
    -- Store-and-forward flag: 'Y' or 'N' (CHAR(1)).
    -- -------------------------------------------------------------------------
    store_and_fwd_flag      CHAR(1)         NOT NULL
                                CHECK (store_and_fwd_flag IN ('Y', 'N')),

    -- -------------------------------------------------------------------------
    -- Location IDs: TLC zone range 1–265.
    -- SMALLINT covers the range (max 32767).
    -- -------------------------------------------------------------------------
    "PULocationID"          SMALLINT        NOT NULL
                                CHECK ("PULocationID" BETWEEN 1 AND 265),

    "DOLocationID"          SMALLINT        NOT NULL
                                CHECK ("DOLocationID" BETWEEN 1 AND 265),

    -- -------------------------------------------------------------------------
    -- Payment type: TLC codes 1–6.
    -- -------------------------------------------------------------------------
    payment_type            SMALLINT        NOT NULL
                                CHECK (payment_type BETWEEN 1 AND 6),

    -- -------------------------------------------------------------------------
    -- Financial columns
    -- NUMERIC(12, 2) stores values up to 9,999,999,999.99 — adequate for all
    -- validated trip amounts (max fare $10 k, max total $15 k).
    --
    -- fare_amount: validated ≥ 0 and ≤ $10,000 by Member 1.
    -- extra, mta_tax, cbd_congestion_fee: negative values are permitted
    --   because they represent legitimate correction records (documented in
    --   docs/dataset.md Section 9 and Section 10 item 4).
    -- tip_amount:  validated ≥ 0 by Member 1.
    -- tolls_amount, total_amount, congestion_surcharge,
    --   improvement_surcharge: expected non-negative in practice but no
    --   hard constraint applied at DB level to allow correction rows.
    -- airport_fee: NULLABLE — 19.19% of source rows have no airport fee
    --   because the trip did not involve an airport pickup.
    -- -------------------------------------------------------------------------
    fare_amount             NUMERIC(12, 2)  NOT NULL
                                CHECK (fare_amount >= 0),

    extra                   NUMERIC(12, 2)  NOT NULL,   -- can be negative (corrections)

    mta_tax                 NUMERIC(12, 2)  NOT NULL,   -- can be negative (corrections)

    tip_amount              NUMERIC(12, 2)  NOT NULL
                                CHECK (tip_amount >= 0),

    tolls_amount            NUMERIC(12, 2)  NOT NULL,

    improvement_surcharge   NUMERIC(12, 2)  NOT NULL,

    total_amount            NUMERIC(12, 2)  NOT NULL,

    congestion_surcharge    NUMERIC(12, 2)  NOT NULL,

    airport_fee             NUMERIC(12, 2)  NULL,       -- nullable: 19.19% missing (non-airport trips)

    cbd_congestion_fee      NUMERIC(12, 2)  NOT NULL,   -- can be negative (corrections)

    -- -------------------------------------------------------------------------
    -- Vendor ID: 1 = Creative Mobile Technologies, 2 = VeriFone.
    -- Included per dataset; SMALLINT sufficient.
    -- -------------------------------------------------------------------------
    "VendorID"              SMALLINT        NOT NULL,

    -- -------------------------------------------------------------------------
    -- Publisher-engineered features
    -- speed_mph: validated 0 < x ≤ 100 by Member 1; NUMERIC(8,4) stores
    --   values like 72.3456 without float rounding.
    -- duration_min: validated > 0 and ≤ 1440 (24 h); NUMERIC(10,4) handles
    --   sub-minute precision (e.g. 1.5033 minutes).
    -- pickup_hour: 0–23; SMALLINT.
    -- day_of_week: full day name string, max 9 chars ("Wednesday");
    --   VARCHAR(9) is exact.
    -- is_weekend: BOOLEAN.
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
-- Three targeted indexes justified by likely query patterns:
--
--  1. idx_taxi_trips_pickup_dt
--     Supports time-range queries (date filters, yearly groupings, min/max
--     lookups).  Verification query 6 (trip counts by year) uses this.
--     Pickup datetime is the most common filter in taxi analytics.
--
--  2. idx_taxi_trips_pu_location
--     Supports WHERE / GROUP BY on pickup zone — common in zone-level
--     aggregations that Member 3 or analysts will run.
--
--  3. idx_taxi_trips_do_location
--     Same reasoning as above for drop-off zone.
--
-- We deliberately skip indexes on payment_type, RatecodeID, and financial
-- columns at this stage: bulk loading 3.9 M rows with many indexes is
-- materially slower, and those columns are low-cardinality or rarely used
-- as selective filters.  Indexes can be added after initial load with
-- CREATE INDEX CONCURRENTLY if query profiling shows they help.
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_taxi_trips_pickup_dt
    ON taxi_trips (tpep_pickup_datetime);

CREATE INDEX IF NOT EXISTS idx_taxi_trips_pu_location
    ON taxi_trips ("PULocationID");

CREATE INDEX IF NOT EXISTS idx_taxi_trips_do_location
    ON taxi_trips ("DOLocationID");


-- =============================================================================
-- TABLE: rejected_taxi_trips
--
-- Audit table for rows that failed Member 1's validate_trips() checks.
-- Mirrors the raw source columns (pre-validation) plus a rejection_reason
-- column sourced directly from the rejected_df returned by transform_data().
--
-- Member 1 return contract (src/transform.py, transform_data docstring):
--   result["rejected_df"] contains all original columns plus
--   "rejection_reason" (str).
--
-- Design decisions:
--   - All source value columns are typed permissively (TEXT for string-like,
--     NUMERIC for numbers, TIMESTAMP for datetimes) to accept values that
--     failed validation — e.g. a fare of -999.99 cannot go into taxi_trips
--     but must be storable here for audit.
--   - rejection_reason is NOT NULL: every row in rejected_df has one.
--   - rejected_at records the server timestamp of insertion for traceability.
-- =============================================================================

CREATE TABLE IF NOT EXISTS rejected_taxi_trips (

    rejected_id             BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- Source timestamps stored as TEXT to preserve malformed values (e.g. a
    -- NaT that cannot be cast to TIMESTAMP is stored as the string 'NaT').
    tpep_pickup_datetime    TEXT,
    tpep_dropoff_datetime   TEXT,

    -- Numeric source columns stored as NUMERIC with generous precision.
    -- NULL is allowed because the rejection may have been caused by a missing
    -- value in one of these columns.
    "VendorID"              NUMERIC,
    passenger_count         NUMERIC,
    trip_distance           NUMERIC,
    "RatecodeID"            NUMERIC,
    store_and_fwd_flag      TEXT,
    "PULocationID"          NUMERIC,
    "DOLocationID"          NUMERIC,
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
    is_weekend              TEXT,           -- stored as text since source may be non-boolean

    -- Audit metadata
    rejection_reason        TEXT            NOT NULL,
    rejected_at             TIMESTAMP       NOT NULL DEFAULT NOW()

);

CREATE INDEX IF NOT EXISTS idx_rejected_reason
    ON rejected_taxi_trips (rejection_reason);
