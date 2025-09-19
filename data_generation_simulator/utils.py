from typing import List
from models import YouTubeVideo
import datetime
import csv
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
import logging
import os
import io


# Load environment variables (assuming dotenv already loaded elsewhere)
load_dotenv()
MINIO_ROOT_USER = os.getenv('MINIO_ROOT_USER')
MINIO_ROOT_PASSWARD = os.getenv('MINIO_ROOT_PASSWARD')
MINIO_BUCKET_NAME = os.getenv('MINIO_BUCKET_NAME')
MINIO_LOCALHOST = os.getenv('MINIO_LOCALHOST')
MINIO_PORT = os.getenv('MINIO_PORT')


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def write_videos_to_csv(videos: List[YouTubeVideo], minio_client: Minio) -> None:
    """
    Writes a list of YouTubeVideo objects to a CSV file and uploads it to MinIO.

    Args:
        videos (List[YouTubeVideo]): A list of YouTubeVideo objects containing video details.
        minio_client (Minio): An initialized MinIO client for object storage operations.
    Returns:
        None
    """
    object_name = f"raw/videos_data__{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    csv_buffer = io.StringIO()

    try:
        # Create CSV writer and write header
        writer = csv.writer(csv_buffer)
        writer.writerow([
            "video_id", "channel_id", "channel_name", "channel_subscribers",
            "description", "total_views", "total_likes", "total_dislikes",
            "date_created", "date_updated"
        ])

        # Write each video's details
        for v in videos:
            writer.writerow([
                v.video_id, v.channel_id, v.channel_name, v.channel_subscribers,
                v.description, v.total_views, v.total_likes, v.total_dislikes,
                v.date_created, v.date_updated
            ])

        csv_buffer.seek(0)

        # Upload CSV to MinIO
        minio_client.put_object(
            bucket_name=MINIO_BUCKET_NAME,
            object_name=object_name,
            data=io.BytesIO(csv_buffer.getvalue().encode("utf-8")),
            length=len(csv_buffer.getvalue().encode("utf-8")),
            content_type="application/csv",
        )
        logging.info(f"Uploaded {object_name} to bucket {MINIO_BUCKET_NAME}")

    except Exception as e:
        logging.error(f"Failed to write or upload videos to CSV: {e}", exc_info=True)


def connect_minio() -> Minio | None:
    """
    Connects to the MinIO server using credentials from environment variables.
    Returns:
        Minio | None: An initialized MinIO client if connection is successful,
                      otherwise None.
    """
    try:
        # Initialize MinIO client
        client = Minio(
            endpoint=f"{MINIO_LOCALHOST}:{MINIO_PORT}",
            access_key=MINIO_ROOT_USER,
            secret_key=MINIO_ROOT_PASSWARD,
            secure=False
        )

        # Check if bucket exists
        if not client.bucket_exists(MINIO_BUCKET_NAME):
            logging.warning(f"Bucket '{MINIO_BUCKET_NAME}' does not exist. Please create it manually.")
        else:
            logging.info(f"Bucket '{MINIO_BUCKET_NAME}' already exists.")

        return client

    except S3Error as e:
        logging.error(f"MinIO S3Error occurred: {e}", exc_info=True)
        return None
    except Exception as e:
        logging.error(f"Unexpected error while connecting to MinIO: {e}", exc_info=True)
        return None
