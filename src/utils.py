from typing import List
from models import YouTubeVideo
import datetime
import csv
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
import os
import io

load_dotenv()
MINIO_ROOT_USER = os.getenv('MINIO_ROOT_USER')
MINIO_ROOT_PASSWARD = os.getenv('MINIO_ROOT_PASSWARD')
MINIO_BUCKET_NAME = os.getenv('MINIO_BUCKET_NAME')
MINIO_LOCALHOST = os.getenv('MINIO_LOCALHOST')
MINIO_PORT = os.getenv('MINIO_PORT')


def write_videos_to_csv(videos: List[YouTubeVideo], minio_client:Minio):
    object_name = f"vidoes_data__{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"


    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(["video_id", "channel_id", "description", "total_views", "total_likes", "total_dislikes", "date_created"])
    
    for v in videos:
        writer.writerow([v.video_id, v.channel_id, v.description, v.total_views, v.total_likes, v.total_dislikes, v.date_created])

    csv_buffer.seek(0)

    minio_client.put_object(
        bucket_name=MINIO_BUCKET_NAME,
        object_name=object_name,
        data=io.BytesIO(csv_buffer.getvalue().encode("utf-8")),
        length=len(csv_buffer.getvalue().encode("utf-8")),
        content_type="application/csv",
    )
    print(f"✅ Uploaded {object_name} to bucket {MINIO_BUCKET_NAME}")

def connect_minio():
    print(MINIO_LOCALHOST, MINIO_PORT)
    try:
        # Initialize client
        client = Minio(
            endpoint=f"{MINIO_LOCALHOST}:{MINIO_PORT}" ,  # MinIO endpoint (host:port)
            access_key=MINIO_ROOT_USER, # Replace with your access key
            secret_key=MINIO_ROOT_PASSWARD, # Replace with your secret key
            secure=False
        )

        # Check if bucket exists
        bucket_name = "datalake"
        if not client.bucket_exists(bucket_name):
            # client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created")
        else:
            print(f"Bucket '{bucket_name}' already exists")

        print(client)
        return client 
    except S3Error as e:
        print("Error occurred:", e)
        return None
