"""
AWS Lambda entrypoint for the Event-Driven Serverless Data Pipeline.

This module orchestrates the full data processing workflow triggered by
an S3 ObjectCreated event. The pipeline performs:

1. Event parsing
2. File download from S3
3. Idempotency check via content hashing
4. Data normalization
5. Schema validation
6. Separation of valid and invalid records
7. Upload of processed results
8. Structured logging of execution metrics

Design Principles:
- Event-driven architecture
- Idempotent processing (MD5-based)
- Clear separation of concerns (cleaning, validation, storage)
- Fail-fast schema enforcement
"""

import time
import logging
import io
import pandas as pd

from storage import download_file, upload_file, file_exists
from cleaning import normalize_dataframe
from validation import validate_dataframe
from utils import calculate_hash

# Configure structured logging for observability in CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def extract_s3_event(event):

    """
    Extract bucket name and object key from an S3 event.

    Assumes the Lambda is triggered by an ObjectCreated event.

    Args:
        event (dict): Raw AWS Lambda event payload.

    Returns:
        tuple: (bucket_name, object_key)
    """
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]
    return bucket, key


def load_into_dataframe(file_bytes: bytes) -> pd.DataFrame:
    """
    Load raw CSV bytes into a Pandas DataFrame.

    Uses in-memory buffering to avoid temporary disk writes,
    keeping the Lambda execution fully ephemeral.

    Args:
        file_bytes (bytes): Raw file content from S3.

    Returns:
        pd.DataFrame: Parsed CSV dataset.
    """
    return pd.read_csv(io.BytesIO(file_bytes))


def lambda_handler(event, context):
    """
    Main Lambda entrypoint.

    Workflow:
        - Parse S3 event
        - Download file
        - Compute hash (idempotency)
        - Normalize and validate data
        - Separate valid and invalid records
        - Persist results to S3
        - Log execution metrics

    Idempotency Strategy:
        The file content is hashed (MD5). If a processed file with the same
        hash already exists, execution is skipped. This prevents duplicate
        processing even if S3 triggers multiple events.

    Error Handling:
        - Schema validation failures result in full file being sent to error/
        - Execution metrics are logged regardless of outcome
    """

    start_time = time.time()

    # 1. Extract event data from S3 trigger
    bucket, key = extract_s3_event(event)
    logger.info(f"Processing file: {key}")

    # 2. Download file from S3
    file_bytes = download_file(bucket, key)

    # 3. Compute content hash for idempotent processing
    file_hash = calculate_hash(file_bytes)

    processed_key = f"processed/users_cleaned_{file_hash}.csv"
    error_key = f"error/invalid_rows_{file_hash}.csv"

    # 4. Idempotency check: skip if already processed
    if file_exists(bucket, processed_key):
        logger.info("File already processed. Skipping.")
        return

    # 5. Load raw CSV into DataFrame
    df = load_into_dataframe(file_bytes)
    total_rows = len(df)

    # 6. Apply normalization rules (column formatting, string cleaning, type casting)
    df = normalize_dataframe(df)

    # 7. Validate schema and row-level constraints
    try:
        valid_df, invalid_df = validate_dataframe(df)
    except ValueError as e:
        logger.error(f"Schema validation failed: {str(e)}")
        upload_file(bucket, error_key, df)
        return

    # 8. Deduplicate valid records by primary identifier
    valid_df = valid_df.drop_duplicates(subset=["user_id"])

    valid_rows = len(valid_df)
    invalid_rows = len(invalid_df)

    # 9. Persist clean dataset
    if valid_rows > 0:
        upload_file(bucket, processed_key, valid_df)

    # 10. Persist invalid rows for traceability
    if invalid_rows > 0:
        upload_file(bucket, error_key, invalid_df)

    # 11. Log structured execution metrics
    processing_time = round(time.time() - start_time, 2)

    logger.info({
        "file": key,
        "hash": file_hash,
        "total_rows": total_rows,
        "valid_rows": valid_rows,
        "invalid_rows": invalid_rows,
        "processing_time_seconds": processing_time
    })
