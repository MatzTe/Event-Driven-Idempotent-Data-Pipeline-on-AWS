import pandas as pd


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize and standardize raw input dataset.

    This function prepares the dataset for validation by:
        1. Standardizing column names
        2. Normalizing string fields
        3. Converting data types

    The goal is to reduce formatting noise before applying
    strict validation rules.

    Args:
        df (pd.DataFrame): Raw input dataset.

    Returns:
        pd.DataFrame: Cleaned dataset.
    """
    # Work on a copy to avoid mutating original reference
    df = df.copy()

    # Step 1: Normalize column naming conventions
    df = _normalize_columns(df)

    # Step 2: Normalize string-based fields
    df = _normalize_strings(df)

    # Step 3: Convert relevant fields to appropriate types
    df = _convert_types(df)

    return df


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize column names for consistency.

    Operations:
        - Remove leading/trailing spaces
        - Convert to lowercase
        - Replace spaces with underscores

    Example:
        " User ID " → "user_id"

    This prevents schema mismatches caused by formatting differences.
    """
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    return df


def _normalize_strings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize string-based columns.

    Operations:
        - Strip surrounding whitespace
        - Lowercase emails
        - Capitalize country names
        - Lowercase subscription tiers

    Notes:
        - Only applies transformations if the column exists
          (defensive programming).
        - Converting to string prevents failures when values
          are mixed types.
    """
    string_columns = ["name", "email", "country", "subscription_tier"]

    # Trim whitespace across relevant string columns
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Field-specific normalization rules
    if "email" in df.columns:
        df["email"] = df["email"].str.lower()

    if "country" in df.columns:
        df["country"] = df["country"].str.capitalize()

    if "subscription_tier" in df.columns:
        df["subscription_tier"] = df["subscription_tier"].str.lower()

    return df


def _convert_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert selected columns to appropriate data types.

    Conversions:
        - user_id → numeric
        - age → numeric
        - signup_date → datetime

    Uses errors="coerce" so invalid parsing results in NaN/NaT.
    These invalid values will later be handled during validation.

    This separation keeps cleaning responsible only for formatting,
    not enforcing business rules.
    """
    
    if "user_id" in df.columns:
        df["user_id"] = pd.to_numeric(df["user_id"], errors="coerce")

    if "age" in df.columns:
        df["age"] = pd.to_numeric(df["age"], errors="coerce")

    if "signup_date" in df.columns:
        df["signup_date"] = pd.to_datetime(
            df["signup_date"],
            errors="coerce"
        )

    return df