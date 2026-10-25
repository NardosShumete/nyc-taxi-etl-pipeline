-- =============================================================================
-- verification_queries.sql
-- Member 2: Post-Load Verification Queries — NYC Taxi ETL Pipeline
--
-- Purpose: Confirm data integrity and completeness after Member 3's loader
--          has inserted rows into taxi_trips (and optionally
--          rejected_taxi_trips).
--
-- Usage:
--   psql -U <user> -d nyc_taxi -f sql/verification_queries.sql
--   or run individual queries in psql / DBeaver / pgAdmin.
--
-- These queries report actual database counts and statistics.
-- No expected values are hardcoded — the SQL tells you what is in the table.
-- =============================================================================


-- =============================================================================
-- SECTION 1 — Row counts
-- =============================================================================

-- 1a. Total rows in the accepted trips table
SELECT COUNT(*) AS total_loaded_trips
FROM   taxi_trips;


-- 1b. Does the loaded count meet the 2-million-row requirement?
--     Returns TRUE if ≥ 2,000,000 rows are present, FALSE otherwise.
SELECT COUNT(*) >= 2000000 AS meets_2_million_requirement,
       COUNT(*)             AS actual_row_count,
       2000000              AS required_minimum
FROM   taxi_trips;


-- 1c. Total rows in the audit/rejected table (only meaningful if Member 3
--     chose to load rejected rows; safe to run even if the table is empty)
SELECT COUNT(*) AS total_rejected_trips
FROM   rejected_taxi_trips;


-- =============================================================================
-- SECTION 2 — Timestamp range
-- =============================================================================

-- 2a. Pickup datetime range
SELECT MIN(tpep_pickup_datetime)  AS earliest_pickup,
       MAX(tpep_pickup_datetime)  AS latest_pickup
FROM   taxi_trips;


-- 2b. Drop-off datetime range
SELECT MIN(tpep_dropoff_datetime) AS earliest_dropoff,
       MAX(tpep_dropoff_datetime) AS latest_dropoff
FROM   taxi_trips;


-- 2c. Verify drop-off is always after pickup (should return 0 violations)
SELECT COUNT(*) AS dropoff_before_or_equal_pickup_violations
FROM   taxi_trips
WHERE  tpep_dropoff_datetime <= tpep_pickup_datetime;


-- =============================================================================
-- SECTION 3 — Nullable column audit
-- =============================================================================

-- 3a. Null counts for every nullable or potentially-null column.
--     airport_fee is the only officially nullable column; others should be 0.
SELECT
    COUNT(*) FILTER (WHERE tpep_pickup_datetime   IS NULL) AS null_pickup_dt,
    COUNT(*) FILTER (WHERE tpep_dropoff_datetime  IS NULL) AS null_dropoff_dt,
    COUNT(*) FILTER (WHERE passenger_count        IS NULL) AS null_passenger_count,
    COUNT(*) FILTER (WHERE trip_distance          IS NULL) AS null_trip_distance,
    COUNT(*) FILTER (WHERE "RatecodeID"           IS NULL) AS null_ratecode_id,
    COUNT(*) FILTER (WHERE store_and_fwd_flag     IS NULL) AS null_store_fwd_flag,
    COUNT(*) FILTER (WHERE "PULocationID"         IS NULL) AS null_pu_location_id,
    COUNT(*) FILTER (WHERE "DOLocationID"         IS NULL) AS null_do_location_id,
    COUNT(*) FILTER (WHERE payment_type           IS NULL) AS null_payment_type,
    COUNT(*) FILTER (WHERE fare_amount            IS NULL) AS null_fare_amount,
    COUNT(*) FILTER (WHERE extra                  IS NULL) AS null_extra,
    COUNT(*) FILTER (WHERE mta_tax                IS NULL) AS null_mta_tax,
    COUNT(*) FILTER (WHERE tip_amount             IS NULL) AS null_tip_amount,
    COUNT(*) FILTER (WHERE tolls_amount           IS NULL) AS null_tolls_amount,
    COUNT(*) FILTER (WHERE total_amount           IS NULL) AS null_total_amount,
    COUNT(*) FILTER (WHERE congestion_surcharge   IS NULL) AS null_congestion_surcharge,
    COUNT(*) FILTER (WHERE airport_fee            IS NULL) AS null_airport_fee,        -- expected ~19% non-zero
    COUNT(*) FILTER (WHERE cbd_congestion_fee     IS NULL) AS null_cbd_congestion_fee,
    COUNT(*) FILTER (WHERE improvement_surcharge  IS NULL) AS null_improvement_surcharge,
    COUNT(*) FILTER (WHERE "VendorID"             IS NULL) AS null_vendor_id,
    COUNT(*) FILTER (WHERE speed_mph              IS NULL) AS null_speed_mph,
    COUNT(*) FILTER (WHERE duration_min           IS NULL) AS null_duration_min,
    COUNT(*) FILTER (WHERE pickup_hour            IS NULL) AS null_pickup_hour,
    COUNT(*) FILTER (WHERE day_of_week            IS NULL) AS null_day_of_week,
    COUNT(*) FILTER (WHERE is_weekend             IS NULL) AS null_is_weekend
FROM taxi_trips;


-- 3b. Airport fee null rate (expected ~19.19% based on source inspection)
SELECT
    COUNT(*)                                              AS total_rows,
    COUNT(*) FILTER (WHERE airport_fee IS NULL)           AS null_airport_fee,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE airport_fee IS NULL)
        / NULLIF(COUNT(*), 0),
        2
    )                                                     AS null_airport_fee_pct
FROM taxi_trips;


-- =============================================================================
-- SECTION 4 — Financial summary statistics
-- =============================================================================

-- 4a. Fare amount statistics
SELECT
    MIN(fare_amount)    AS min_fare,
    MAX(fare_amount)    AS max_fare,
    ROUND(AVG(fare_amount)::NUMERIC, 2) AS avg_fare,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY fare_amount) AS median_fare
FROM taxi_trips;


-- 4b. Tip amount statistics
SELECT
    MIN(tip_amount)     AS min_tip,
    MAX(tip_amount)     AS max_tip,
    ROUND(AVG(tip_amount)::NUMERIC, 2) AS avg_tip
FROM taxi_trips;


-- 4c. Total amount statistics
SELECT
    MIN(total_amount)   AS min_total,
    MAX(total_amount)   AS max_total,
    ROUND(AVG(total_amount)::NUMERIC, 2) AS avg_total
FROM taxi_trips;


-- 4d. Negative-value counts (corrections — should be low but non-zero is acceptable)
SELECT
    COUNT(*) FILTER (WHERE extra             < 0) AS neg_extra_count,
    COUNT(*) FILTER (WHERE mta_tax           < 0) AS neg_mta_tax_count,
    COUNT(*) FILTER (WHERE cbd_congestion_fee< 0) AS neg_cbd_fee_count
FROM taxi_trips;


-- =============================================================================
-- SECTION 5 — Trip distance and duration summary
-- =============================================================================

SELECT
    MIN(trip_distance)  AS min_distance_miles,
    MAX(trip_distance)  AS max_distance_miles,
    ROUND(AVG(trip_distance)::NUMERIC, 4)    AS avg_distance_miles,

    MIN(duration_min)   AS min_duration_min,
    MAX(duration_min)   AS max_duration_min,
    ROUND(AVG(duration_min)::NUMERIC, 4)     AS avg_duration_min,

    MIN(speed_mph)      AS min_speed_mph,
    MAX(speed_mph)      AS max_speed_mph,
    ROUND(AVG(speed_mph)::NUMERIC, 4)        AS avg_speed_mph
FROM taxi_trips;


-- =============================================================================
-- SECTION 6 — Trip counts grouped by year
-- =============================================================================

-- 6a. Trips per year (ordered chronologically)
SELECT
    EXTRACT(YEAR FROM tpep_pickup_datetime)::INT AS pickup_year,
    COUNT(*)                                      AS trip_count
FROM   taxi_trips
GROUP  BY pickup_year
ORDER  BY pickup_year;


-- 6b. Trips per month (useful for spotting loading gaps or data anomalies)
SELECT
    DATE_TRUNC('month', tpep_pickup_datetime) AS pickup_month,
    COUNT(*)                                   AS trip_count
FROM   taxi_trips
GROUP  BY pickup_month
ORDER  BY pickup_month;


-- =============================================================================
-- SECTION 7 — Categorical integrity checks
-- =============================================================================

-- 7a. Distinct RatecodeID values present (expected: subset of {1,2,3,4,5,6,99})
SELECT "RatecodeID", COUNT(*) AS trip_count
FROM   taxi_trips
GROUP  BY "RatecodeID"
ORDER  BY "RatecodeID";


-- 7b. Any invalid RatecodeID values (should return 0 rows if schema CHECK held)
SELECT COUNT(*) AS invalid_ratecode_count
FROM   taxi_trips
WHERE  "RatecodeID" NOT IN (1, 2, 3, 4, 5, 6, 99);


-- 7c. Distinct payment_type values (expected: subset of {1,2,3,4,5,6})
SELECT payment_type, COUNT(*) AS trip_count
FROM   taxi_trips
GROUP  BY payment_type
ORDER  BY payment_type;


-- 7d. Distinct store_and_fwd_flag values (expected: 'Y', 'N')
SELECT store_and_fwd_flag, COUNT(*) AS trip_count
FROM   taxi_trips
GROUP  BY store_and_fwd_flag
ORDER  BY store_and_fwd_flag;


-- 7e. Distinct day_of_week values (expected: exactly 7 day names)
SELECT day_of_week, COUNT(*) AS trip_count
FROM   taxi_trips
GROUP  BY day_of_week
ORDER  BY trip_count DESC;


-- 7f. Weekday vs weekend split
SELECT
    is_weekend,
    COUNT(*) AS trip_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM   taxi_trips
GROUP  BY is_weekend
ORDER  BY is_weekend;


-- 7g. VendorID distribution (expected: 1 or 2)
SELECT "VendorID", COUNT(*) AS trip_count
FROM   taxi_trips
GROUP  BY "VendorID"
ORDER  BY "VendorID";


-- =============================================================================
-- SECTION 8 — Speed / duration cross-check
-- =============================================================================

-- 8a. Trips where recalculated speed deviates >20% from stored speed_mph.
--     Should return 0 rows if Member 1 validation ran correctly.
SELECT COUNT(*) AS speed_inconsistency_violations
FROM   taxi_trips
WHERE  ABS(speed_mph - (trip_distance / (duration_min / 60.0)))
           / GREATEST(trip_distance / (duration_min / 60.0), 0.000001)
       > 0.20;


-- 8b. Trips at boundary speeds (useful for confirming filter thresholds held)
SELECT
    COUNT(*) FILTER (WHERE speed_mph > 100) AS over_100mph,
    COUNT(*) FILTER (WHERE speed_mph = 0)   AS zero_mph,
    COUNT(*) FILTER (WHERE speed_mph < 0)   AS negative_mph
FROM taxi_trips;


-- =============================================================================
-- SECTION 9 — Pickup hour distribution
-- =============================================================================

SELECT
    pickup_hour,
    COUNT(*) AS trip_count
FROM   taxi_trips
GROUP  BY pickup_hour
ORDER  BY pickup_hour;


-- =============================================================================
-- SECTION 10 — Rejected-row audit (only if Member 3 loaded rejected rows)
-- =============================================================================

-- 10a. Total rejected rows
SELECT COUNT(*) AS total_rejected_rows
FROM   rejected_taxi_trips;


-- 10b. Rejection reason breakdown (sorted by frequency)
SELECT
    rejection_reason,
    COUNT(*)                                                      AS count,
    ROUND(100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0), 2) AS pct_of_rejected
FROM   rejected_taxi_trips
GROUP  BY rejection_reason
ORDER  BY count DESC;


-- 10c. Rejected rows over time (by year of pickup string, best-effort parse)
--      Uses TO_TIMESTAMP with a safe fallback via CASE so malformed values
--      return NULL rather than raising an error.
SELECT
    CASE
        WHEN tpep_pickup_datetime ~ '^\d{4}-\d{2}-\d{2}'
        THEN EXTRACT(YEAR FROM tpep_pickup_datetime::TIMESTAMP)::INT
        ELSE NULL
    END AS rejection_year,
    COUNT(*) AS rejected_count
FROM   rejected_taxi_trips
GROUP  BY rejection_year
ORDER  BY rejection_year;


-- =============================================================================
-- SECTION 11 — Combined load summary (single-shot overview)
-- =============================================================================

SELECT
    (SELECT COUNT(*) FROM taxi_trips)                        AS accepted_rows,
    (SELECT COUNT(*) FROM rejected_taxi_trips)               AS rejected_rows,
    (SELECT COUNT(*) FROM taxi_trips) +
    (SELECT COUNT(*) FROM rejected_taxi_trips)               AS total_processed,
    (SELECT COUNT(*) >= 2000000 FROM taxi_trips)             AS accepted_meets_2m_requirement,
    (SELECT MIN(tpep_pickup_datetime) FROM taxi_trips)       AS earliest_pickup,
    (SELECT MAX(tpep_pickup_datetime) FROM taxi_trips)       AS latest_pickup,
    (SELECT ROUND(AVG(fare_amount)::NUMERIC, 2)
     FROM   taxi_trips)                                      AS avg_fare_usd;
