"""
transform.py — Member 1: Data Quality Validation and Pandas Transformations
NYC Taxi ETL Pipeline

Responsibilities:
  - Normalize column names to a consistent snake_case format.
  - Convert raw DataFrame columns to their correct data types.
  - Validate business rules and data-quality constraints.
  - Apply the group's agreed transformation rules.
  - Report row-level accounting (input / valid / rejected + reasons).

This module is intentionally independent of PostgreSQL.
Member 2 owns the database schema; Member 3 owns loading and integration.

Return contract for transform_data()
-------------------------------------
Returns a dict:
{
    "clean_df"        : pd.DataFrame,   # rows that passed all validation rules
    "rejected_df"     : pd.DataFrame,   # rows that failed, with a 'rejection_reason' column
    "summary"         : dict,           # row counts and rejection-reason breakdown
}

The columns in clean_df are (in order):
    tpep_pickup_datetime, tpep_dropoff_datetime,
    passenger_count, trip_distance,
    RatecodeID, store_and_fwd_flag,
    PULocationID, DOLocationID,
    payment_type,
    fare_amount, extra, mta_tax, tip_amount, tolls_amount,
    total_amount, congestion_surcharge, airport_fee,
    speed_mph, duration_min

Member 3 integration note:
    Pass clean_df["clean_df"] directly to the loading layer.
    The rejected_df is for audit/logging only.
"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants — adjust thresholds in one place
# ---------------------------------------------------------------------------

# Speed: the original publisher already removed trips > 100 MPH.
# We re-apply the same cap as a safeguard; our own cap matches theirs.
MAX_SPEED_MPH: float = 100.0

# Trip distance bounds (miles)
MIN_TRIP_DISTANCE: float = 0.0       # exclusive — zero or negative = invalid
MAX_TRIP_DISTANCE: float = 500.0     # implausibly long for a NYC taxi trip

# Duration bounds (minutes)
MIN_DURATION_MIN: float = 0.0        # exclusive
MAX_DURATION_MIN: float = 1440.0     # 24 hours

# Passenger count
MIN_PASSENGER_COUNT: int = 1
MAX_PASSENGER_COUNT: int = 6         # NYC TLC max capacity for standard taxis

# Financial bounds
# Negative fares are possible (refunds / corrections) but extreme values
# are suspicious.  We flag, not silently drop, marginal cases.
MIN_FARE_AMOUNT: float = 0.0         # Negative fares treated as invalid here
MAX_FARE_AMOUNT: float = 10_000.0    # >$10k is implausible for a single trip
MAX_TIP_AMOUNT: float = 5_000.0
MAX_TOTAL_AMOUNT: float = 15_000.0

# Valid RatecodeID values (NYC TLC specification)
VALID_RATECODE_IDS = {1, 2, 3, 4, 5, 6, 99}

# Valid payment_type values (NYC TLC specification)
VALID_PAYMENT_TYPES = {1, 2, 3, 4, 5, 6}

# Location ID bounds (NYC TLC zone IDs 1–265)
MIN_LOCATION_ID: int = 1
MAX_LOCATION_ID: int = 265

# Speed / duration consistency tolerance
# When speed_mph and duration_min are present in the data, we verify
# that they are consistent with trip_distance.
# Tolerance accounts for rounding in the original feature engineering.
SPEED_DURATION_TOLERANCE: float = 0.20   # 20 % relative tolerance


# ---------------------------------------------------------------------------
# Step 1: Normalize column names
# ---------------------------------------------------------------------------

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a copy of *df* with column names normalised for consistent downstream use.

    Steps applied:
    1. Strip leading/trailing whitespace from all column names.
    2. Rename ``Airport_fee`` → ``airport_fee`` to match the TLC data dictionary
       and the expected schema (the actual file uses a capital 'A').

    The NYC TLC dataset uses mixed-case names (e.g. RatecodeID, PULocationID)
    which are preserved as-is to remain compatible with the published data
    dictionary and Member 2's schema design.

    All other columns found in the actual dataset but not in the original expected
    list (``VendorID``, ``improvement_surcharge``, ``cbd_congestion_fee``,
    ``pickup_hour``, ``day_of_week``, ``is_weekend``) are kept unchanged so that
    Member 3 can choose whether to load them into the database.
    """
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]

    # Normalise Airport_fee casing (discovered during inspection of actual file)
    if "Airport_fee" in df.columns and "airport_fee" not in df.columns:
        df = df.rename(columns={"Airport_fee": "airport_fee"})

    return df


# ---------------------------------------------------------------------------
# Step 2: Convert data types
# ---------------------------------------------------------------------------

def convert_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cast each column to its expected data type.

    Datetime columns are coerced with errors='coerce' so that malformed
    strings become NaT rather than raising an exception.  All other numeric
    columns are coerced similarly so that the downstream validator can detect
    and count invalid values.

    Returns a copy; does not mutate the input DataFrame.
    """
    df = df.copy()

    # --- Datetime columns ---------------------------------------------------
    for col in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
        if col in df.columns:
            if not pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = pd.to_datetime(df[col], errors="coerce")

    # --- Integer columns ----------------------------------------------------
    int_cols = [
        "passenger_count", "RatecodeID",
        "PULocationID", "DOLocationID", "payment_type",
    ]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            # Use nullable integer so NaN is preserved (not silently 0)
            df[col] = df[col].astype("Int64")

    # --- Float columns ------------------------------------------------------
    float_cols = [
        "trip_distance", "fare_amount", "extra", "mta_tax",
        "tip_amount", "tolls_amount", "total_amount",
        "congestion_surcharge", "airport_fee",
        "speed_mph", "duration_min",
    ]
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # --- String / categorical columns ---------------------------------------
    if "store_and_fwd_flag" in df.columns:
        df["store_and_fwd_flag"] = df["store_and_fwd_flag"].astype(str).str.strip().str.upper()
        # Map numeric encodings that may appear in some dataset versions
        df["store_and_fwd_flag"] = df["store_and_fwd_flag"].replace({"0": "N", "1": "Y"})

    return df


# ---------------------------------------------------------------------------
# Step 3: Validate trips — core business-rule checks
# ---------------------------------------------------------------------------

def validate_trips(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply data-quality rules to each row and return the DataFrame with a new
    column ``rejection_reason`` (str or None).

    Rules applied
    -------------
    1.  Missing pickup or drop-off timestamp (NaT after coercion).
    2.  Drop-off time ≤ pickup time (negative or zero duration).
    3.  trip_distance ≤ 0 or > MAX_TRIP_DISTANCE.
    4.  speed_mph > MAX_SPEED_MPH (when column present).
    5.  Inconsistency between speed_mph / duration_min and trip_distance
        (when both engineered features are present).
    6.  Missing or non-positive fare_amount.
    7.  fare_amount or total_amount outside plausible bounds.
    8.  Negative tip_amount (tips cannot be negative).
    9.  passenger_count outside [MIN, MAX] range.
    10. RatecodeID not in VALID_RATECODE_IDS.
    11. payment_type not in VALID_PAYMENT_TYPES.
    12. PULocationID or DOLocationID outside [1, 265].
    13. Exact duplicate rows (keeps the first occurrence).
    14. duration_min ≤ 0 or > MAX_DURATION_MIN (when column present).

    Publisher's pre-cleaning note
    ------------------------------
    The dataset is described as already cleaned by its publisher (zero-distance
    trips removed, speeds >100 MPH removed, negative fares removed).  We
    re-apply these checks as a verification layer.  Any remaining failures
    represent either data drift, partial cleaning, or records the publisher
    may have missed.

    Returns
    -------
    A copy of *df* with a ``rejection_reason`` column added.
    Rows that pass all checks have ``rejection_reason = None``.
    Only the *first* failing rule is recorded per row.
    """
    df = df.copy()
    df["rejection_reason"] = None

    def _mark(mask: pd.Series, reason: str) -> None:
        """Set rejection_reason for untagged rows matching *mask*."""
        untagged = df["rejection_reason"].isna()
        df.loc[mask & untagged, "rejection_reason"] = reason

    # 1. Missing timestamps
    if "tpep_pickup_datetime" in df.columns:
        _mark(df["tpep_pickup_datetime"].isna(), "missing_pickup_datetime")
    if "tpep_dropoff_datetime" in df.columns:
        _mark(df["tpep_dropoff_datetime"].isna(), "missing_dropoff_datetime")

    # 2. Drop-off not after pickup
    if {"tpep_pickup_datetime", "tpep_dropoff_datetime"}.issubset(df.columns):
        both_valid = (
            df["tpep_pickup_datetime"].notna()
            & df["tpep_dropoff_datetime"].notna()
        )
        _mark(
            both_valid & (df["tpep_dropoff_datetime"] <= df["tpep_pickup_datetime"]),
            "dropoff_not_after_pickup",
        )

    # 3. Trip distance
    if "trip_distance" in df.columns:
        _mark(df["trip_distance"].isna(), "missing_trip_distance")
        _mark(
            df["trip_distance"].notna() & (df["trip_distance"] <= MIN_TRIP_DISTANCE),
            "non_positive_trip_distance",
        )
        _mark(
            df["trip_distance"].notna() & (df["trip_distance"] > MAX_TRIP_DISTANCE),
            "implausible_trip_distance",
        )

    # 4. Speed cap
    if "speed_mph" in df.columns:
        _mark(
            df["speed_mph"].notna() & (df["speed_mph"] > MAX_SPEED_MPH),
            "speed_exceeds_100mph",
        )

    # 5. Speed / duration consistency check
    # Expected speed = trip_distance / (duration_min / 60)
    # We only check rows where all three values are present and positive.
    if {"speed_mph", "duration_min", "trip_distance"}.issubset(df.columns):
        have_all = (
            df["speed_mph"].notna()
            & df["duration_min"].notna()
            & df["trip_distance"].notna()
            & (df["duration_min"] > 0)
            & (df["trip_distance"] > 0)
        )
        expected_speed = df["trip_distance"] / (df["duration_min"] / 60.0)
        relative_error = (df["speed_mph"] - expected_speed).abs() / expected_speed.clip(lower=1e-6)
        _mark(
            have_all & (relative_error > SPEED_DURATION_TOLERANCE),
            "speed_duration_inconsistency",
        )

    # 6. Fare amount
    if "fare_amount" in df.columns:
        _mark(df["fare_amount"].isna(), "missing_fare_amount")
        _mark(
            df["fare_amount"].notna() & (df["fare_amount"] < MIN_FARE_AMOUNT),
            "negative_fare_amount",
        )
        _mark(
            df["fare_amount"].notna() & (df["fare_amount"] > MAX_FARE_AMOUNT),
            "implausible_fare_amount",
        )

    # 7. Total amount bound
    if "total_amount" in df.columns:
        _mark(
            df["total_amount"].notna() & (df["total_amount"] > MAX_TOTAL_AMOUNT),
            "implausible_total_amount",
        )

    # 8. Tip amount
    if "tip_amount" in df.columns:
        _mark(
            df["tip_amount"].notna() & (df["tip_amount"] < 0),
            "negative_tip_amount",
        )
        _mark(
            df["tip_amount"].notna() & (df["tip_amount"] > MAX_TIP_AMOUNT),
            "implausible_tip_amount",
        )

    # 9. Passenger count
    if "passenger_count" in df.columns:
        _mark(df["passenger_count"].isna(), "missing_passenger_count")
        _mark(
            df["passenger_count"].notna()
            & (
                (df["passenger_count"] < MIN_PASSENGER_COUNT)
                | (df["passenger_count"] > MAX_PASSENGER_COUNT)
            ),
            "invalid_passenger_count",
        )

    # 10. RatecodeID
    if "RatecodeID" in df.columns:
        _mark(df["RatecodeID"].isna(), "missing_RatecodeID")
        _mark(
            df["RatecodeID"].notna()
            & ~df["RatecodeID"].isin(VALID_RATECODE_IDS),
            "invalid_RatecodeID",
        )

    # 11. Payment type
    if "payment_type" in df.columns:
        _mark(df["payment_type"].isna(), "missing_payment_type")
        _mark(
            df["payment_type"].notna()
            & ~df["payment_type"].isin(VALID_PAYMENT_TYPES),
            "invalid_payment_type",
        )

    # 12. Location IDs
    for loc_col, label in [("PULocationID", "pickup"), ("DOLocationID", "dropoff")]:
        if loc_col in df.columns:
            _mark(df[loc_col].isna(), f"missing_{label}_location_id")
            _mark(
                df[loc_col].notna()
                & (
                    (df[loc_col] < MIN_LOCATION_ID)
                    | (df[loc_col] > MAX_LOCATION_ID)
                ),
                f"invalid_{label}_location_id",
            )

    # 13. Exact duplicates — flag all but the first occurrence
    dupe_cols = [c for c in df.columns if c != "rejection_reason"]
    _mark(df.duplicated(subset=dupe_cols, keep="first"), "exact_duplicate_row")

    # 14. Duration bounds
    if "duration_min" in df.columns:
        _mark(df["duration_min"].isna(), "missing_duration_min")
        _mark(
            df["duration_min"].notna() & (df["duration_min"] <= MIN_DURATION_MIN),
            "non_positive_duration_min",
        )
        _mark(
            df["duration_min"].notna() & (df["duration_min"] > MAX_DURATION_MIN),
            "implausible_duration_min",
        )

    return df


# ---------------------------------------------------------------------------
# Step 4: Orchestrate — main entry point
# ---------------------------------------------------------------------------

def transform_data(df: pd.DataFrame) -> dict:
    """
    Full Member 1 transformation pipeline.

    Steps
    -----
    1. normalize_columns  — strip whitespace from column names
    2. convert_data_types — cast to correct types, coerce errors to NaN/NaT
    3. validate_trips     — tag each row with a rejection_reason or None

    Parameters
    ----------
    df : pd.DataFrame
        Raw (or publisher-cleaned) taxi trip records.

    Returns
    -------
    dict with keys:
        "clean_df"   : pd.DataFrame  — rows that passed all validation rules
        "rejected_df": pd.DataFrame  — rows that failed, with rejection_reason
        "summary"    : dict          — row-count accounting + reason breakdown

    See module docstring for the agreed column list in clean_df.

    Integration note for Member 3
    --------------------------------
    Feed result["clean_df"] to the loading layer.
    result["rejected_df"] may be persisted to a separate audit table if desired.
    """
    if df is None or not isinstance(df, pd.DataFrame):
        raise TypeError("transform_data expects a pandas DataFrame.")

    n_input = len(df)
    logger.info("transform_data: received %d rows", n_input)

    # Step 1: normalize
    df = normalize_columns(df)

    # Step 2: type conversion
    df = convert_data_types(df)

    # Step 3: validate
    df = validate_trips(df)

    # Split into clean / rejected
    is_rejected = df["rejection_reason"].notna()
    clean_df = df[~is_rejected].drop(columns=["rejection_reason"]).reset_index(drop=True)
    rejected_df = df[is_rejected].reset_index(drop=True)

    n_clean = len(clean_df)
    n_rejected = len(rejected_df)

    # Rejection breakdown
    reason_counts: dict = {}
    if n_rejected > 0:
        reason_counts = (
            rejected_df["rejection_reason"]
            .value_counts()
            .to_dict()
        )

    summary = {
        "n_input": n_input,
        "n_clean": n_clean,
        "n_rejected": n_rejected,
        "rejection_rate_pct": round(100.0 * n_rejected / n_input, 2) if n_input > 0 else 0.0,
        "rejection_reasons": reason_counts,
    }

    logger.info(
        "transform_data complete: %d clean, %d rejected (%.2f%%)",
        n_clean,
        n_rejected,
        summary["rejection_rate_pct"],
    )

    return {
        "clean_df": clean_df,
        "rejected_df": rejected_df,
        "summary": summary,
    }


# ---------------------------------------------------------------------------
# Helper: build a clean summary string (useful in scripts / notebooks)
# ---------------------------------------------------------------------------

def format_summary(summary: dict) -> str:
    """Return a human-readable multi-line summary string from transform_data()."""
    lines = [
        "=== Transform Summary ===",
        f"  Input rows   : {summary['n_input']:,}",
        f"  Clean rows   : {summary['n_clean']:,}",
        f"  Rejected rows: {summary['n_rejected']:,}  ({summary['rejection_rate_pct']:.2f}%)",
    ]
    if summary["rejection_reasons"]:
        lines.append("  Rejection breakdown:")
        for reason, count in sorted(
            summary["rejection_reasons"].items(), key=lambda x: -x[1]
        ):
            lines.append(f"    {reason:<45} {count:>10,}")
    return "\n".join(lines)
