import hashlib


def calculate_hash(file_bytes: bytes) -> str:
    """
    Generate a deterministic content hash for a file.

    This function computes the MD5 hash of the raw file bytes
    and returns its hexadecimal representation.

    The hash acts as a content fingerprint and is used to:
        - Enable idempotent processing
        - Detect duplicate uploads
        - Generate unique output filenames

    Args:
        file_bytes (bytes): Raw file content.

    Returns:
        str: 32-character hexadecimal MD5 digest.

    Notes:
        - MD5 is not used here for cryptographic security.
        - It is sufficient for content identity detection.
        - Any change in file content (even 1 byte)
          produces a completely different hash.
    """
    return hashlib.md5(file_bytes).hexdigest()