import logging
import pandas as pd
from typing import List, Dict, Any
from etl.utils import (
    MINIO_BUCKET_NAME,
    MINIO_ROOT_USER,
    MINIO_ROOT_PASSWARD,
    endpoint_url,
)

logger = logging.getLogger(__name__)


def validate_file(file: str) -> Dict[str, Any]:
    """
    Validate a CSV file stored in MinIO by checking required schema.

    Args:
        file (str): Path to the file inside the bucket (relative key).

    Returns:
        dict: Result containing:
            - "file" (str): File name.
            - "is_valid" (bool): Whether validation passed.
            - "error" (str, optional): Error message if validation failed.
    """
    try:
        df = pd.read_csv(
            f"s3://{MINIO_BUCKET_NAME}/{file}",
            storage_options={
                "key": MINIO_ROOT_USER,
                "secret": MINIO_ROOT_PASSWARD,
                "client_kwargs": {"endpoint_url": endpoint_url},
            },
        )

        required_columns = {
            "video_id", "description", "channel_id",
            "total_likes", "total_dislikes", "date_created",
        }
        if not required_columns.issubset(df.columns):
            missing = required_columns - set(df.columns)
            logger.warning("File %s missing columns: %s", file, missing)
            return {"file": file, "is_valid": False, "error": f"Missing columns: {missing}"}

        logger.info("File %s passed validation", file)
        return {"file": file, "is_valid": True}

    except Exception as e:
        logger.error("Validation failed for %s: %s", file, e, exc_info=True)
        return {"file": file, "is_valid": False, "error": str(e)}


def aggregate_results(results: List[Dict[str, Any]]) -> Dict[str, List]:
    """
    Aggregate validation results into valid and invalid file lists.

    Args:
        results (list[dict]): List of validation results.

    Returns:
        dict: Dictionary containing:
            - "valid_files": List of valid file paths.
            - "invalid_files": List of invalid file result dicts.
    """
    valid = [r["file"] for r in results if r.get("is_valid")]
    invalid = [r for r in results if not r.get("is_valid")]
    logger.info("Validation summary: %d valid, %d invalid", len(valid), len(invalid))
    return {"valid_files": valid, "invalid_files": invalid}
