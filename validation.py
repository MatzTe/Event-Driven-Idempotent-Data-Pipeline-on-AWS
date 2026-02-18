import re
import pandas as pd

# -------------------------------------------------------------------
# Schema contract: expected structure of incoming dataset.
# Using a set allows fast difference operations for schema validation.
# -------------------------------------------------------------------
EXPECTED_COLUMNS = {
    "user_id",
    "name",
    "email",
    "signup_date",
    "country",
    "age",
    "subscription_tier"
}

# -------------------------------------------------------------------
# Business rule: allowed subscription tiers.
# Set lookup is O(1), more efficient than list membership checks.
# -------------------------------------------------------------------
VALID_TIERS = {"free", "basic", "premium", "enterprise"}

# -------------------------------------------------------------------
# Basic email pattern validation.
# Note: This is a simplified regex and does not cover all RFC cases.
# -------------------------------------------------------------------
EMAIL_REGEX = r"^[\w\.-]+@[\w\.-]+\.\w+$"


def validate_dataframe(df: pd.DataFrame):
    """
    Validate a DataFrame against schema and row-level business rules.

    Validation occurs in two phases:
        1. Schema validation (structural integrity)
        2. Row-level validation (business logic rules)

    Args:
        df (pd.DataFrame): Input dataset.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]:
            - valid_df: rows passing all validation checks
            - invalid_df: rows failing at least one validation rule

    Raises:
        ValueError: If required columns are missing.
    """

    # 1. Schema validation (fail fast)
    _validate_schema(df)

    # 2. Row-level validation
    # apply(..., axis=1) evaluates each row independently
    valid_mask = df.apply(_is_valid_row, axis=1)

    # Split dataset using boolean mask
    valid_df = df[valid_mask].copy()
    invalid_df = df[~valid_mask].copy()

    return valid_df, invalid_df


def _validate_schema(df: pd.DataFrame):
    """
    Ensure all required columns exist in the dataset.

    Args:
        df (pd.DataFrame)

    Raises:
        ValueError: If any expected column is missing.

    Notes:
        - Extra columns are allowed (forward compatibility).
        - Only missing required fields trigger failure.
    """

    missing_columns = EXPECTED_COLUMNS - set(df.columns)

    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")


def _is_valid_row(row):
    """
    Validate a single row against business rules.

    Rules:
        - user_id must not be null
        - email must match basic email pattern
        - signup_date must not be null
        - age must be positive integer
        - subscription_tier must be allowed

    Args:
        row (pd.Series)

    Returns:
        bool: True if row is valid, False otherwise.
    """

    if pd.isnull(row["user_id"]):
        return False

    if not _valid_email(row["email"]):
        return False

    if pd.isnull(row["signup_date"]):
        return False

    if not _valid_age(row["age"]):
        return False

    if row["subscription_tier"] not in VALID_TIERS:
        return False

    return True


def _valid_email(email):
    """
    Validate email format using regex.

    Args:
        email (Any)

    Returns:
        bool
    """
    if pd.isnull(email):
        return False
    # Convert to string to avoid type issues
    return re.match(EMAIL_REGEX, str(email)) is not None


def _valid_age(age):
    """
    Validate that age is a positive integer.

    Args:
        age (Any)

    Returns:
        bool
    """
    try:
        return int(age) > 0
    except:
        return False
