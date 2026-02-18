import boto3
import botocore
import pandas as pd
import io

# Create a reusable S3 client.
# In AWS Lambda, this is instantiated outside the handler
# so connections can be reused across invocations (performance optimization).
s3_client = boto3.client("s3")


def download_file(bucket: str, key: str) -> bytes:
    """
    Download a file from S3 and return its raw bytes.

    This function retrieves the object from S3 using the provided
    bucket name and key, then reads the response body fully into memory.

    Args:
        bucket (str): S3 bucket name.
        key (str): Object key (path inside the bucket).

    Returns:
        bytes: Raw file content.

    Raises:
        botocore.exceptions.ClientError:
            If the object does not exist or access is denied.
    """
    response = s3_client.get_object(Bucket=bucket, Key=key)

    return response["Body"].read()


def upload_file(bucket: str, key: str, df: pd.DataFrame):
    """
    Upload a Pandas DataFrame to S3 as a CSV file.

    The DataFrame is first serialized in-memory using StringIO
    to avoid writing temporary files to disk (Lambda is ephemeral).

    Args:
        bucket (str): Destination S3 bucket.
        key (str): Target object key.
        df (pd.DataFrame): DataFrame to upload.

    Returns:
        None

    Notes:
        - CSV is written without index to keep schema clean.
        - This method loads the full CSV into memory before upload,
          which is suitable for small-to-medium datasets.
    """
    # In-memory text buffer
    csv_buffer = io.StringIO()

    # Serialize DataFrame to CSV
    df.to_csv(csv_buffer, index=False)

    # Upload the CSV string as object body
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_buffer.getvalue()
    )


def file_exists(bucket: str, key: str) -> bool:
    """
    Check whether an object exists in S3.

    This function performs a lightweight metadata request (HEAD)
    instead of downloading the full object.

    Args:
        bucket (str): S3 bucket name.
        key (str): Object key.

    Returns:
        bool:
            True if the object exists.
            False if it does not exist.

    Implementation Details:
        - Uses head_object() because it only retrieves metadata,
          making it more efficient than get_object().
        - If the object does not exist, AWS raises a ClientError.
    """
    try:
        # HEAD request retrieves metadata only (no file download)
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except botocore.exceptions.ClientError:
        # If the error is a 404, the file does not exist.
        # Other errors (e.g., permissions) could also land here.
        return False
