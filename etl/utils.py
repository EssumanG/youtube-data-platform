import os
import logging
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv


load_dotenv()
logger = logging.getLogger(__name__)

MINIO_BUCKET_NAME = os.getenv('MINIO_BUCKET_NAME')
MINIO_ROOT_USER = os.getenv('MINIO_ROOT_USER')
MINIO_ROOT_PASSWARD = os.getenv('MINIO_ROOT_PASSWARD')
MINIO_DOCKER_NAME = os.getenv('MINIO_DOCKER_NAME')
MINIO_DOCKER_PORT = os.getenv('MINIO_DOCKER_PORT')

endpoint_url = f"http://{MINIO_DOCKER_NAME}:{MINIO_DOCKER_PORT}"

def connect_minio() -> Minio:
    """Initialize and return a MinIO client."""
    try:
        client = Minio(
            endpoint=f"{MINIO_DOCKER_NAME}:{MINIO_DOCKER_PORT}",
            access_key=MINIO_ROOT_USER,
            secret_key=MINIO_ROOT_PASSWARD,
            secure=False,
        )
        logger.info("MinIO client initialized")
        return client
    except S3Error as e:
        logger.error(f"Failed to connect to MinIO: {e}")
        raise
