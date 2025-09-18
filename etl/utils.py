from minio import Minio
import os
from dotenv import load_dotenv


load_dotenv()

MINIO_ROOT_USER = os.getenv('MINIO_ROOT_USER')
MINIO_ROOT_PASSWARD = os.getenv('MINIO_ROOT_PASSWARD')
MINIO_DOCKER_NAME = os.getenv('MINIO_DOCKER_NAME')
MINIO_DOCKER_PORT = os.getenv('MINIO_DOCKER_PORT')

def connect_minio():
    minio_client = Minio(
        endpoint=f"{MINIO_DOCKER_NAME}:{MINIO_DOCKER_PORT}",
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWARD,
        secure=False,
    )
    return minio_client
