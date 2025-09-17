from typing import List
from models import YouTubeVideo
import datetime
import csv
from minio import Minio
from minio.error import S3Error


def write_videos_to_csv(videos: List[YouTubeVideo]):
    filename = f"vidoes_data__{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    with open(filename, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["video_id", "channel_id", "description", "total_views", "total_likes", "total_dislikes", "date_created"])
        for v in videos:
            writer.writerow([v.video_id, v.channel_id, v.description, v.total_views, v.total_likes, v.total_dislikes, v.date_created])


def connect_minio():
    try:
        # Initialize client
        client = Minio(
            endpoint="localhost:9000",   # MinIO endpoint (host:port)
            access_key="admin", # Replace with your access key
            secret_key="password1234", # Replace with your secret key
            secure=False
        )

        # Check if bucket exists
        bucket_name = "datalake"
        if not client.bucket_exists(bucket_name):
            # client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created")
        else:
            print(f"Bucket '{bucket_name}' already exists")

        print("File downloaded successfully")
        print(client)

    except S3Error as e:
        print("Error occurred:", e)
