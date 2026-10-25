-- =============================================================================
-- verification_queries.sql
-- Member 2: Post-Load Verification Queries — NYC Taxi ETL Pipeline
--
-- Run after Member 3 has loaded data into taxi_trips (and optionally
-- rejected_taxi_trips).
--
-- Usage:
--   psql -U <user> -d nyc_taxi -f sql/verification_queries.sql
--   or paste individual sections into psql / pgAdmin / DBeaver.
--
-- All queries report the ACTUAL state of the database.
-- No expected values are hardcoded; results are compared to thresholds
-- by the SQL itself (e.g. Section 1b returns a boolean).
--
-- Column naming note: all columns use lowercase snake_case.
-- DataFrame columns VendorID, RatecodeID, PULocationID, DOLocationID
-- are stored as vendor_id, ratecode_id, pu_location_id, do_location_id.
-- See docs/database.md Section 9 for the rename mapping Member 3 applies.
-- =============================================================================


-- =============================================================================
-- SECTION 1 — Row counts
-- =============================================================================

-- 1a. Total accepted rows
SELECT COUNT(*) AS total_loaded_trips
FROM   taxi_trips;


-- 1b. Does the loaded count meet the 2-million-row requirement?
--     Reports the actual count AND a boolean threshold check.
--     A TRUE result does NOT imply all source rows were accepted — it only
--     means the accepted rows in this table number at least 2,000,000.
--     The source dataset has 3,946,698 rows; some are expected to be rejected
--     by Member 1's validation rules.
SELECT COUNT(*) >= 2000000  AS meets_2_million_requirement,
       COUNT(*)              AS actual_accepted_row_count,
       2000000               AS required_minimum
FROM   taxi_trips;


-- 1c. Rejected row count (meaningful only if Member 3 loaded rejected rows)
SELECT COUNT(*) AS total_rejected_rows
FROM   rejected_taxi_trips;


-- =============================================================================
-- SECTION 2 — Timestamp range
-- =============================================================================

-- 2a. Pickup datetime range (source spans 2007-12-05 to 2025-11-30)
SELECT MIN(tpep_pickup_datetime)  AS earliest_pickup,
       MAX(tpep_pickup_datetime)  AS latest_pickup
FROM   taxi_trips;


-- 2b. Drop-off datetime range
SELECT MIN(tpep_dropoff_datetime) AS earliest_dropoff,
       MAX(tpep_dropoff_datetime) AS latest_dropoff
FROM   taxi_trips;


-- 2c. Integrity check: drop-off must be strictly after pickup.
--     Should return 0; any non-zero count is a validation gap.
SELECT COUNT(*) AS dropoff_not_after_pickup_violations
FROM   taxi_trips
WHERE  tpep_dropoff_datetime <= tpep_pickup_datetime;


-- =============================================================================
-- SECTION 3 — Null audit
-- =============================================================================

-- 3a. Null counts across all 25 data columns.
--     airport_fee is the only column expected to have nulls (~19.19%).
--     All other columns should show 0.
SELECT
    COUNT(*) FILTER (WHERE tpep_pickup_datetime  IS NULL) AS null_pickup_dt,
    COUNT(*) FILTER (WHERE tpep_dropoff_datetime IS NULL) AS null_dropoff_dt,
    COUNT(*) FILTER (WHERE passenger_count       IS NULL) AS null_passenger_count,
    COUNT(*) FILTER (WHERE trip_distance         IS NULL) AS null_trip_distance,
    COUNT(*) FILTER (WHERE ratecode_id           IS NULL) AS null_ratecode_id,
    COUNT(*) FILTER (WHERE store_and_fwd_flag    IS NULL) AS null_store_fwd_flag,
    COUNT(*) FILTER (WHERE pu_location_id        IS NULL) AS null_pu_location_id,
    COUNT(*) FILTER (WHERE do_location_id        IS NULL) AS null_do_location_id,
    COUNT(*) FILTER (WHERE payment_type          IS NULL) AS null_payment_type,
    COUNT(*) FILTER (WHERE fare_amount           IS NULL) AS null_fare_amount,
    COUNT(*) FILTER (WHERE extra                 IS NULL) AS null_extra,
    COUNT(*) FILTER (WHERE mta_tax               IS NULL) AS null_mta_tax,
    COUNT(*) FILTER (WHERE tip_amount            IS NULL) AS null_tip_amount,
    COUNT(*) FILTER (WHERE tolls_amount          IS NULL) AS null_tolls_amount,
    COUNT(*) FILTER (WHERE improvement_surcharge IS NULL) AS null_improvement_surcharge,
    COUNT(*) FILTER (WHERE total_amount          IS NULL) AS null_total_amount,
    COUNT(*) FILTER (WHERE congestion_surcharge  IS NULL) AS null_congestion_surcharge,
    COUNT(*) FILTER (WHERE airport_fee           IS NULL) AS null_airport_fee,
    COUNT(*) FILTER (WHERE cbd_congestion_fee    IS NULL) AS null_cbd_congestion_fee,
    COUNT(*) FILTER (WHERE vendor_id             IS NULL) AS null_vendor_id,
    COUNT(*) FILTER (WHERE speed_mph             IS NULL) AS null_speed_mph,
    COUNT(*) FILTER (WHERE duration_min          IS NULL) AS null_duration_min,
    COUNT(*) FILTER (WHERE pickup_hour           IS NULL) AS null_pickup_hour,
    COUNT(*) FILTER (WHERE day_of_week           IS NULL) AS null_day_of_week,
    COUNT(*) FILTER (WHERE is_weekend            IS NULL) AS null_is_weekend
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
-- SECTION 4 — Financial summary
-- =============================================================================

-- 4a. Fare amount statistics
SELECT
    MIN(fare_amount)                                          AS min_fare,
    MAX(fare_amount)                                          AS max_fare,
    ROUND(AVG(fare_amount)::NUMERIC, 2)                       AS avg_fare,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY fare_amount) AS median_fare
FROM taxi_trips;


-- 4b. Tip amount statistics
SELECT
    MIN(tip_amount)                         AS min_tip,
    MAX(tip_amount)                         AS max_tip,
    ROUND(AVG(tip_amount)::NUMERIC, 2)      AS avg_tip
FROM taxi_trips;


-- 4c. Total amount statistics
SELECT
    MIN(total_amount)                       AS min_total,
    MAX(total_amount)                       AS max_total,
    ROUND(AVG(total_amount)::NUMERIC, 2)    AS avg_total
FROM taxi_trips;


-- 4d. Negative-value counts.
--     extra, mta_tax, cbd_congestion_fee may legitimately be negative
--     (correction records).  Non-zero counts here are acceptable.
SELECT
    COUNT(*) FILTER (WHERE extra              < 0) AS neg_extra_count,
    COUNT(*) FILTER (WHERE mta_tax            < 0) AS neg_mta_tax_count,
    COUNT(*) FILTER (WHERE cbd_congestion_fee < 0) AS neg_cbd_fee_count
FROM taxi_trips;


-- =============================================================================
-- SECTION 5 — Trip distance, duration, and speed summary
-- =============================================================================

SELECT
    MIN(trip_distance)                              AS min_distance_miles,
    MAX(trip_distance)                              AS max_distance_miles,
    ROUND(AVG(trip_distance)::NUMERIC, 4)           AS avg_distance_miles,

    MIN(duration_min)                               AS min_duration_min,
    MAX(duration_min)                               AS max_duration_min,
    ROUND(AVG(duration_min)::NUMERIC, 4)            AS avg_duration_min,

    MIN(speed_mph)                                  AS min_speed_mph,
    MAX(speed_mph)                                  AS max_speed_mph,
    ROUND(AVG(speed_mph)::NUMERIC, 4)               AS avg_speed_mph
FROM taxi_trips;


-- =============================================================================
-- SECTION 6 — Trip counts by year
-- =============================================================================

-- 6a. Trips per year (source spans 2007–2025; expect multi-year distribution)
SELECT
    EXTRACT(YEAR FROM tpep_pickup_datetime)::INT AS pickup_year,
    COUNT(*)                                      AS trip_count
FROM   taxi_trips
GROUP  BY pickup_year
ORDER  BY pickup_year;


-- 6b. Trips per calendar month (useful for spotting load gaps)
SELECT
    DATE_TRUNC('month', tpep_pickup_datetime) AS pickup_month,
    COUNT(*)                                   AS trip_count
FROM   taxi_trips
GROUP  BY pickup_month
ORDER  BY pickup_month;


-- =============================================================================
-- SECTION 7 — Categorical integrity
-- =============================================================================

-- 7a. ratecode_id distribution (expected values: subset of {1,2,3,4,5,6,99})
SELECT ratecode_id, COUNT(*) AS trip_count
FROM   taxi_trips
GROUP  BY ratecode_id
ORDER  BY ratecode_id;


-- 7b. Invalid ratecode_id values — should return 0
SELECT COUNT(*) AS invalid_ratecode_count
FROM   taxi_trips
WHERE  ratecode_id NOT IN (1, 2, 3, 4, 5, 6, 99);


-- 7c. payment_type distribution (expected values: subset of {1,2,3,4,5,6})
SELECT payment_type, COUNT(*) AS trip_count
FROM   taxi_trips
GROUP  BY payment_type
ORDER  BY payment_type;


-- 7d. store_and_fwd_flag distribution (expected: 'Y' and/or 'N')
SELECT store_and_fwd_flag, COUNT(*) AS trip_count
FROM   taxi_trips
GROUP  BY store_and_fwd_flag
ORDER  BY store_and_fwd_flag;


-- 7e. day_of_week distribution (expected: all 7 day names)
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


-- 7g. vendor_id distribution (expected: 1 or 2)
SELECT vendor_id, COUNT(*) AS trip_count
FROM   taxi_trips
GROUP  BY vendor_id
ORDER  BY vendor_id;


-- =============================================================================
-- SECTION 8 — Speed / duration cross-check
-- =============================================================================

-- 8a. Rows where the recalculated speed deviates > 20% from stored speed_mph.
--     Member 1's rule 5 rejects such rows; this should return 0.
SELECT COUNT(*) AS speed_inconsistency_violations
FROM   taxi_trips
WHERE  ABS(speed_mph - (trip_distance / (duration_min / 60.0)))
           / GREATEST(trip_distance / (duration_min / 60.0), 0.000001)
       > 0.20;


-- 8b. Boundary speed checks (all should return 0 after validation)
SELECT
    COUNT(*) FILTER (WHERE speed_mph > 100) AS over_100mph,
    COUNT(*) FILTER (WHERE speed_mph <= 0)  AS zero_or_negative_mph
FROM taxi_trips;


-- =============================================================================
-- SECTION 9 — Pickup hour distribution
-- =============================================================================

SELECT pickup_hour,
       COUNT(*) AS trip_count
FROM   taxi_trips
GROUP  BY pickup_hour
ORDER  BY pickup_hour;


-- =============================================================================
-- SECTION 10 — Rejected-row audit
--              (Only meaningful if Member 3 loaded rejected rows)
-- =============================================================================

-- 10a. Total rejected rows
SELECT COUNT(*) AS total_rejected_rows
FROM   rejected_taxi_trips;


-- 10b. Rejection reason breakdown sorted by frequency
SELECT
    rejection_reason,
    COUNT(*)                                                       AS count,
    ROUND(100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0), 2) AS pct_of_rejected
FROM   rejected_taxi_trips
GROUP  BY rejection_reason
ORDER  BY count DESC;


-- 10c. Rejected row counts by year (best-effort; tpep_pickup_datetime is TEXT
--      in rejected_taxi_trips to tolerate malformed values like 'NaT')
SELECT
    CASE
        WHEN tpep_pickup_datetime ~ '^\d{4}-\d{2}-\d{2}'
        THEN EXTRACT(YEAR FROM tpep_pickup_datetime::TIMESTAMP)::INT
        ELSE NULL
    END         AS rejection_year,
    COUNT(*)    AS rejected_count
FROM   rejected_taxi_trips
GROUP  BY rejection_year
ORDER  BY rejection_year;


-- =============================================================================
-- SECTION 11 — Single-row load summary
-- =============================================================================

SELECT
    (SELECT COUNT(*)              FROM taxi_trips)           AS accepted_rows,
    (SELECT COUNT(*)              FROM rejected_taxi_trips)  AS rejected_rows,
    (SELECT COUNT(*) + (SELECT COUNT(*) FROM rejected_taxi_trips)
                                  FROM taxi_trips)           AS total_processed,
    (SELECT COUNT(*) >= 2000000   FROM taxi_trips)           AS accepted_meets_2m_requirement,
    (SELECT MIN(tpep_pickup_datetime) FROM taxi_trips)       AS earliest_pickup,
    (SELECT MAX(tpep_pickup_datetime) FROM taxi_trips)       AS latest_pickup,
    (SELECT ROUND(AVG(fare_amount)::NUMERIC, 2)
     FROM   taxi_trips)                                      AS avg_fare_usd;
