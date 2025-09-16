import csv
import os
from minio import Minio
from minio.error import S3Error
from dataclasses import dataclass
from typing import List
from faker import Faker
import random
import uuid
import datetime
import time

fake = Faker()


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


class Initiator:
    number_of_channels = 4
    max_number_videos = 3
    videos: List[YouTubeVideo] = []
    channels: List[YoutubeChannel] = []

    def __init__(self):
        print("Initializing simulation")

    @classmethod
    def setup(cls):
        print("Setting up channels...")
        for _ in range(cls.number_of_channels):
            cls.channels.append(cls.create_channel())

        time.sleep(1)

        print("Setting up videos...")
        for channel in cls.channels:
            for _ in range(random.randint(1, cls.max_number_videos)):
                cls.videos.append(cls.create_video(channel.channel_id))

    @classmethod
    def create_channel(cls) -> YoutubeChannel:
        channel = YoutubeChannel(
            channel_id=str(uuid.uuid4()),
            name=fake.company(),
            subscribers=random.randint(2, 50),  # new channels start small
            created_at=fake.date_this_decade().isoformat()
        )
        return channel

    @classmethod
    def create_video(cls, channel_id: str) -> YouTubeVideo:
        video = YouTubeVideo(
            video_id=str(uuid.uuid4()),
            channel_id=channel_id,
            description=fake.text(max_nb_chars=100),
            total_views=random.randint(2, 50),  # small start
            total_likes=random.randint(0, 10),
            total_dislikes=random.randint(0, 2),
            date_created=fake.date_this_year().isoformat()
        )
        return video

    @classmethod
    def update_video_stats(cls):
        """Incrementally update views/likes/dislikes for existing videos"""
        updated_videos = []
        for v in cls.videos:
            v.total_views += random.randint(1, 200)
            v.total_likes += random.randint(0, 20)
            v.total_dislikes += random.randint(0, 5)
            updated_videos.append(v)
        return updated_videos
        

    @classmethod
    def maybe_add_channel_or_video(cls):
        """Randomly add a new channel or a new video"""
        if random.random() < 0.45:  # 30% chance to create something new
            if random.random() < 0.3:
                # new channel
                new_ch = cls.create_channel()
                cls.channels.append(new_ch)
                print(f"New channel created: {new_ch.name}")
                return []
            else:
                # new video under existing channel
                new_videos = []
                channel = random.choice(cls.channels)
                new_vid = cls.create_video(channel.channel_id)
                cls.videos.append(new_vid)
                new_videos.append(new_vid)
                print(f"New video created under {channel.name}")
                return new_videos
        return []


def write_videos_to_csv(videos: List[YouTubeVideo]):
    filename = f"vidoes_data__{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
    with open(filename, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["video_id", "channel_id", "description", "total_views", "total_likes", "total_dislikes", "date_created"])
        for v in videos:
            writer.writerow([v.video_id, v.channel_id, v.description, v.total_views, v.total_likes, v.total_dislikes, v.date_created])


if __name__ == "__main__":
    initiator = Initiator()
    initiator.setup()

    while True:
        # update stats
        new_updates = initiator.update_video_stats()

        # maybe create new channels/videos
        new_vidoes = initiator.maybe_add_channel_or_video()
   
        # write snapshot of all videos to CSV
        write_videos_to_csv(new_updates + new_vidoes)

        print(f"Snapshot saved with {len(initiator.videos)} videos, {len(initiator.channels)} channels")

        time.sleep(10)  # wait before next update
