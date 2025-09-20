import os
import logging
from datetime import datetime
from etl.utils import connect_minio, MINIO_BUCKET_NAME

logger = logging.getLogger(__name__)


def archive_file(source_data: dict) -> list[str]:
    """
    Archive valid files from MinIO by moving them to an `archive/` folder.

    Args:
        source_data (dict): Dictionary containing keys:
            - "valid_files": list of files to archive.

    Returns:
        list[str]: List of archived file paths.
    """
    from minio.commonconfig import CopySource

    files_to_archive = source_data.get("valid_files", [])
    if not files_to_archive:
        logger.info("No files to archive.")
        return []

    archived_files = []
    date_prefix = datetime.now().strftime("%Y-%m-%d")
    minio_client = connect_minio()

    for file_key in files_to_archive:
        try:
            file_name = os.path.basename(file_key)
            archive_key = f"archive/{date_prefix}/{file_name}"

            # Copy to archive
            minio_client.copy_object(
                MINIO_BUCKET_NAME,
                archive_key,
                CopySource(MINIO_BUCKET_NAME, file_key),
            )

            # Delete original
            minio_client.remove_object(MINIO_BUCKET_NAME, file_key)

            archived_files.append(archive_key)
            logger.info("Archived %s -> %s", file_key, archive_key)

        except Exception as e:
            logger.error("Archive failed for %s: %s", file_key, e, exc_info=True)

    return archived_files


def quarantine_file(source_data: dict) -> list[str]:
    """
    Quarantine invalid files from MinIO by moving them to a `quarantine/` folder.

    Args:
        source_data (dict): Dictionary containing keys:
            - "invalid_files": list of files to quarantine.

    Returns:
        list[str]: List of quarantined file paths.
    """
    files_to_quarantine = source_data.get("invalid_files", [])
    if not files_to_quarantine:
        logger.info("No files to quarantine.")
        return []

    quarantine_files = []
    date_prefix = datetime.now().strftime("%Y-%m-%d")
    minio_client = connect_minio()

    for file_key in files_to_quarantine:
        try:
            file_name = os.path.basename(file_key)
            quarantine_key = f"quarantine/{date_prefix}/{file_name}"

            # Copy to quarantine
            minio_client.copy_object(
                MINIO_BUCKET_NAME,
                quarantine_key,
                f"/{MINIO_BUCKET_NAME}/{file_key}",
            )

            # Delete original
            minio_client.remove_object(MINIO_BUCKET_NAME, file_key)

            quarantine_files.append(quarantine_key)
            logger.info("Quarantined %s -> %s", file_key, quarantine_key)

        except Exception as e:
            logger.error("Quarantine failed for %s: %s", file_key, e, exc_info=True)

    return quarantine_files
