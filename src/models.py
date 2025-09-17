import csv
import os
from minio import Minio
from minio.error import S3Error
from dataclasses import dataclass



@dataclass
class YoutubeChannel:
    channel_id: str
    name: str
    subscribers: int
    created_at: str


@dataclass
class YouTubeVideo:
    video_id: str
    channel_id: str
    description: str
    total_views: int
    total_likes: int
    total_dislikes: int
    date_created: str

