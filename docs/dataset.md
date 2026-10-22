# Dataset Documentation — NYC Yellow Taxi Trip Data (2025 Sample)

**Member 1 — Dataset Inspection, Data Quality Validation, and Pandas Transformation**

---

## 1. Dataset Title and Source

**Title:** NYC Yellow Taxi Trip Data — 2025 Sample (Cleaned and Enriched)

**Original publisher:** NYC Taxi and Limousine Commission (TLC)  
**TLC Trip Record Data page:** https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

> **Attribution note:** The dataset used in this project is a pre-cleaned 2025 sample derived from
> NYC TLC Yellow Taxi Trip Records published at the URL above.  The publisher applied additional
> cleaning and feature engineering before distribution (see Section 7).

> **Verification status:** Statistics in Sections 3–5 and 9 were verified by running
> `scripts/inspect_dataset.py` against the actual file on 2026-10-22.

---

## 2. Business Context and Relevance

New York City's yellow taxi network generates millions of trip records each month.  Each record
captures the full lifecycle of a single trip: when and where passengers were picked up and
dropped off, how far they travelled, how long it took, and what they paid.

For this ETL project the dataset serves as a realistic, large-scale tabular workload that
exercises every stage of a production data pipeline:

- **Inspection** (Member 1) — understand shape, quality, and completeness before loading.
- **Transformation** (Member 1) — validate business rules and standardise types.
- **Schema design** (Member 2) — define PostgreSQL tables that can absorb the validated records.
- **Loading and integration** (Member 3) — stream clean records into the database reliably.

The dataset is particularly suitable because it includes engineered features (`speed_mph`,
`duration_min`) that allow cross-column consistency checks, and because the TLC data dictionary
provides a clear specification of every field.

---

## 3. Actual Filename and Format

| Property      | Value                                        |
|---------------|----------------------------------------------|
| Filename      | `NYC_Taxi_Cleaned_Analysis_Ready.csv`        |
| Container     | `archive.zip` (168.7 MB compressed)          |
| Format        | CSV (UTF-8)                                  |
| Uncompressed  | 586 MB (614.47 MB as reported by stat)       |
| Location      | `data/raw/` (gitignored — not committed)     |

*Verified by `scripts/inspect_dataset.py` on 2026-10-22.*

---

## 4. Actual Row and Column Counts

| Metric             | Verified Value    |
|--------------------|-------------------|
| Total rows         | **3,946,698**     |
| Total columns      | **25**            |
| File size (stat)   | 614.47 MB         |

*Verified by `scripts/inspect_dataset.py` on 2026-10-22.*

---

## 5. Dataset Date Range

| Boundary               | Verified Value            |
|------------------------|---------------------------|
| Earliest pickup        | 2007-12-05 18:45:00       |
| Latest pickup          | 2025-11-30 23:59:59       |
| Earliest drop-off      | 2007-12-05 19:02:00       |
| Latest drop-off        | 2025-12-01 00:29:37       |
| Described coverage     | 2025 (sample)             |

> **Note:** The datetime range spans from 2007 to 2025, which is much wider than the "2025
> sample" description suggests.  This likely indicates the file is a multi-year aggregate
> rather than a single-year sample, or that some records have erroneous timestamps from older
> data included during aggregation.  Member 2 and Member 3 should be aware when designing
> date-partitioned schemas or loading strategies.

*Verified by `scripts/inspect_dataset.py` on 2026-10-22.*

---

## 6. Important Columns and Their Meanings

### Columns matching the assignment specification (19 of 19 present)

| Column                    | Actual dtype   | Description                                                                 |
|---------------------------|----------------|-----------------------------------------------------------------------------|
| `tpep_pickup_datetime`    | datetime64[ns] | Date and time the meter was engaged.                                        |
| `tpep_dropoff_datetime`   | datetime64[ns] | Date and time the meter was disengaged.                                     |
| `passenger_count`         | int64          | Number of passengers (driver-entered). Validated range: 1–6.                |
| `trip_distance`           | float64        | Elapsed trip distance in miles.                                             |
| `RatecodeID`              | int64          | Final rate code. Valid values: 1–6, 99.                                     |
| `store_and_fwd_flag`      | object         | Trip record stored in vehicle memory: "Y" or "N".                          |
| `PULocationID`            | int64          | TLC taxi zone for pickup. Range: 1–265.                                     |
| `DOLocationID`            | int64          | TLC taxi zone for drop-off. Range: 1–265.                                   |
| `payment_type`            | int64          | Payment method. Valid values: 1–6.                                          |
| `fare_amount`             | float64        | Metered fare (dollars).                                                     |
| `extra`                   | float64        | Miscellaneous extras and surcharges.                                        |
| `mta_tax`                 | float64        | $0.50 MTA tax.                                                              |
| `tip_amount`              | float64        | Tip (auto-populated for credit cards).                                      |
| `tolls_amount`            | float64        | Total tolls paid.                                                           |
| `total_amount`            | float64        | Total amount charged.                                                       |
| `congestion_surcharge`    | float64        | Congestion surcharge (up to $2.50).                                         |
| `airport_fee`             | float64        | Airport pick-up fee. **Note: actual column name in file is `Airport_fee`** (capital A). `normalize_columns()` renames it to `airport_fee`. |
| `speed_mph`               | float64        | **Engineered** — average speed in miles per hour. Range verified: 1–80 MPH. |
| `duration_min`            | float64        | **Engineered** — trip duration in minutes. Range verified: 1–179.97 min.   |

### Additional columns in actual file (beyond assignment spec)

These columns were not in the original assignment description but are present in the dataset.
Member 2 should decide whether to include them in the PostgreSQL schema.

| Column                 | Actual dtype | Description                                                     |
|------------------------|--------------|-----------------------------------------------------------------|
| `VendorID`             | int64        | TPEP provider ID (1 = Creative Mobile Technologies, 2 = VeriFone). |
| `improvement_surcharge`| float64      | $0.30 improvement surcharge.                                    |
| `cbd_congestion_fee`   | float64      | Central Business District congestion fee (newer surcharge).     |
| `pickup_hour`          | int64        | **Engineered** — hour of pickup (0–23).                         |
| `day_of_week`          | object       | **Engineered** — day name ("Monday", …, "Sunday").              |
| `is_weekend`           | bool         | **Engineered** — True if pickup is Saturday or Sunday.          |

### RatecodeID values
| Code | Meaning                        |
|------|--------------------------------|
| 1    | Standard rate                  |
| 2    | JFK                            |
| 3    | Newark                         |
| 4    | Nassau or Westchester          |
| 5    | Negotiated fare                |
| 6    | Group ride                     |
| 99   | Unknown                        |

### payment_type values
| Code | Meaning       |
|------|---------------|
| 1    | Credit card   |
| 2    | Cash          |
| 3    | No charge     |
| 4    | Dispute       |
| 5    | Unknown       |
| 6    | Voided trip   |

---

## 7. Publisher's Cleaning and Feature Engineering

The following steps are **described by the dataset's publisher** and documented here separately
from the validation our group performed (Section 8).

1. Zero-distance trips removed.
2. Trips with speeds exceeding 100 MPH removed.  *(Verified: max speed in file is 80 MPH.)*
3. Negative fare amounts removed.  *(Verified: min fare is $0.01.)*
4. Refunds and administrative errors removed.
5. Missing values in `airport_fee` standardised.  *(Actual: 757,387 nulls remain — 19.19%.)*
6. Missing values in `congestion_surcharge` standardised.  *(Verified: 0 nulls.)*
7. **Engineered feature:** `speed_mph` = `trip_distance` / (`duration_min` / 60).
8. **Engineered feature:** `duration_min` = drop-off minus pickup in minutes.
9. **Engineered feature:** `pickup_hour`, `day_of_week`, `is_weekend` (not described in original spec but present in file).

---

## 8. Validation Performed by Our Group

`src/transform.py` (`validate_trips` function) applies the following validation rules.
Every rule is documented in the module docstring.

| Rule | Column(s) | Action on failure |
|------|-----------|-------------------|
| Missing pickup timestamp | `tpep_pickup_datetime` | Reject |
| Missing drop-off timestamp | `tpep_dropoff_datetime` | Reject |
| Drop-off ≤ pickup | both datetime cols | Reject |
| Non-positive or missing distance | `trip_distance` | Reject |
| Distance > 500 miles | `trip_distance` | Reject |
| Speed > 100 MPH | `speed_mph` | Reject |
| Speed/duration inconsistency (> 20% relative error) | `speed_mph`, `duration_min`, `trip_distance` | Reject |
| Missing fare | `fare_amount` | Reject |
| Negative fare | `fare_amount` | Reject |
| Fare > $10,000 | `fare_amount` | Reject |
| Total amount > $15,000 | `total_amount` | Reject |
| Negative tip | `tip_amount` | Reject |
| Tip > $5,000 | `tip_amount` | Reject |
| Passenger count < 1 or > 6 | `passenger_count` | Reject |
| RatecodeID not in {1–6, 99} | `RatecodeID` | Reject |
| payment_type not in {1–6} | `payment_type` | Reject |
| Location ID outside 1–265 | `PULocationID`, `DOLocationID` | Reject |
| Non-positive duration | `duration_min` | Reject |
| Duration > 1,440 min (24 h) | `duration_min` | Reject |
| Exact duplicate rows | all columns | Reject all but first occurrence |

All rejected rows are preserved in a separate `rejected_df` with a `rejection_reason` column
for audit purposes.  No data is silently discarded.

---

## 9. Missing-Value and Duplicate Findings

*Verified by `scripts/inspect_dataset.py` on 2026-10-22.*

| Column                  | Null count  | % Null | Notes                                              |
|-------------------------|-------------|--------|----------------------------------------------------|
| All columns except one  | 0           | 0.00%  | No nulls in most columns                           |
| `Airport_fee`           | 757,387     | 19.19% | Publisher described as "standardised" — 19% remain null |

**Exact duplicate rows: 0**

### Key suspicious values found (from inspection)

| Finding | Detail |
|---------|--------|
| `passenger_count` range [0, 9] | Values 0 and 7–9 are outside TLC valid range of 1–6; these will be rejected by `validate_trips`. |
| `payment_type` min = 0 | Value 0 is not in TLC spec {1–6}; these rows will be rejected. |
| `mta_tax` min = -7.81 | Negative MTA tax records exist. Our validation does not currently reject negative `mta_tax` (correction records are possible). |
| `extra` min = -17.39 | Negative extra surcharges exist. Same reasoning — not rejected. |
| `cbd_congestion_fee` min = -0.75 | Small negative values; likely corrections. Not rejected. |
| `RatecodeID` mean = 2.71 | Anomalously high mean; bulk may be JFK/airport trips or the 99 ("Unknown") code is common. |

---

## 10. Known Limitations and Assumptions

1. **Date range anomaly:** Timestamps span 2007–2025, not only 2025 as described.  Downstream
   schema design (Member 2) and loading logic (Member 3) should handle multi-year data.

2. **`airport_fee` nulls:** 19.19% of rows have `Airport_fee` null despite the publisher
   stating they standardised this.  Our validation does not reject rows with a null
   `airport_fee` since it may legitimately be missing for non-airport trips.  Member 2
   should make this column nullable in the schema.

3. **Column name casing:** The file uses `Airport_fee` (capital A) instead of `airport_fee`.
   `normalize_columns()` in `src/transform.py` renames it transparently.

4. **Negative `extra`, `mta_tax`, `cbd_congestion_fee`:** These can represent legitimate
   corrections.  Our current validation accepts them.  If the group decides to reject
   them, coordinate with Member 2's schema and Member 3's loading logic.

5. **`passenger_count` = 0:** Present in the file despite publisher cleaning.  These rows
   are rejected by our validation as invalid.

6. **`payment_type` = 0:** Present in the file; not a valid TLC code.  Rejected by validation.

7. **Extra engineered columns:** `pickup_hour`, `day_of_week`, `is_weekend` are in the file
   but not in the original assignment spec.  They are not validated by `validate_trips` but
   are passed through to `clean_df` for Member 2 and Member 3 to decide on.

8. **Tip amounts for cash payments:** `tip_amount` is auto-populated only for credit card
   payments.  A $0 tip is legitimate for cash payments.

9. **Dataset not committed:** The raw file is gitignored.  All team members must obtain it
   independently and place it in `data/raw/` before running any pipeline stage.

---

## 11. Does the Dataset Meet the 2-Million-Row Requirement?

**✓ YES — verified.**

The dataset contains **3,946,698 rows**, which exceeds the 2,000,000-row requirement by
approximately 1.95 million rows.

*Verified by `scripts/inspect_dataset.py` on 2026-10-22.*

---

## Appendix: How to Reproduce This Documentation

```bash
# Place the dataset in data/raw/ then run:
python scripts/inspect_dataset.py --input "data/raw/NYC_Taxi_Cleaned_Analysis_Ready.csv"
```

The script processes the file in 500,000-row chunks and reports all quality metrics
without loading the entire 586 MB file into memory at once.
