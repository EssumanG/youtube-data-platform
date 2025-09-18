from models import YoutubeChannel, YouTubeVideo
from typing import List
import random
import uuid
import time
from faker import Faker
import datetime

fake = Faker()

class Simulator:
    number_of_channels = 2
    max_number_videos = 2
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
                cls.videos.append(cls.create_video(channel))

    @classmethod
    def create_channel(cls) -> YoutubeChannel:
        channel = YoutubeChannel(
            channel_id=str(uuid.uuid4()),
            name=fake.company(),
            subscribers=random.randint(2, 10),  # new channels start small
            created_at=fake.date_this_decade().isoformat()
        )
        return channel

    @classmethod
    def create_video(cls, channel: YoutubeChannel) -> YouTubeVideo:
        date = datetime.datetime.now()
        video = YouTubeVideo(
            video_id=str(uuid.uuid4()),
            channel_id=channel.channel_id,
            channel_name=channel.name,
            channel_subscribers=channel.subscribers,
            description=fake.text(max_nb_chars=100),
            total_views=random.randint(2, 50),  # small start
            total_likes=random.randint(0, 10),
            total_dislikes=random.randint(0, 2),
            date_created=date,
            date_updated=date
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
            v.date_updated = datetime.datetime.now()
            updated_videos.append(v)
            print(v.date_updated)
        return updated_videos
    
    @classmethod
    def update_channel_stats(cls):
        """Incrementally update views/likes/dislikes for existing videos"""
        channel = random.choice(cls.channels)
        channel.subscribers += random.randint(0, 10)
        
    
        

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
                new_vid = cls.create_video(channel)
                cls.videos.append(new_vid)
                new_videos.append(new_vid)
                print(f"New video created under {channel.name}")
                return new_videos
        return []


