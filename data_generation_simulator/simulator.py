import random
import uuid
import time
import datetime
import logging
from typing import List
from faker import Faker
from models import YoutubeChannel, YouTubeVideo

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

fake = Faker()


class Simulator:
    """
    A simulator for generating and updating YouTube channels and videos
    with randomized statistics for testing or prototyping.
    """

    number_of_channels = 2
    max_number_videos = 2
    videos: List[YouTubeVideo] = []
    channels: List[YoutubeChannel] = []

    def __init__(self):
        logging.info("Initializing simulation")

    @classmethod
    def setup(cls) -> None:
        """
        Set up the simulation by creating initial channels and videos.
        """
        try:
            logging.info("Setting up channels...")
            for _ in range(cls.number_of_channels):
                cls.channels.append(cls.create_channel())

            time.sleep(1)  # simulate time delay

            logging.info("Setting up videos...")
            for channel in cls.channels:
                for _ in range(random.randint(1, cls.max_number_videos)):
                    cls.videos.append(cls.create_video(channel))

        except Exception as e:
            logging.error(f"Error during setup: {e}", exc_info=True)

    @classmethod
    def create_channel(cls) -> YoutubeChannel:
        """
        Create a new YouTube channel with fake data.
        Returns:
            YoutubeChannel: A new channel instance.
        """
        try:
            channel = YoutubeChannel(
                channel_id=str(uuid.uuid4()),
                name=fake.company(),
                subscribers=random.randint(2, 10),  # new channels start small
                created_at=fake.date_this_decade().isoformat()
            )
            return channel
        except Exception as e:
            logging.error(f"Error creating channel: {e}", exc_info=True)
            raise

    @classmethod
    def create_video(cls, channel: YoutubeChannel) -> YouTubeVideo:
        """
        Create a new YouTube video under a given channel.
        Args:
            channel (YoutubeChannel): The channel to associate the video with.
        Returns:
            YouTubeVideo: A new video instance.
        """
        try:
            date = datetime.datetime.now()
            video = YouTubeVideo(
                video_id=str(uuid.uuid4()),
                channel_id=channel.channel_id,
                channel_name=channel.name,
                channel_subscribers=channel.subscribers,
                description=fake.text(max_nb_chars=100),
                total_views=random.randint(2, 50),
                total_likes=random.randint(0, 10),
                total_dislikes=random.randint(0, 2),
                date_created=date,
                date_updated=date
            )
            return video
        except Exception as e:
            logging.error(f"Error creating video: {e}", exc_info=True)
            raise

    @classmethod
    def update_video_stats(cls) -> List[YouTubeVideo]:
        """
        Incrementally update views, likes, and dislikes for existing videos.
        Returns:
            List[YouTubeVideo]: The updated list of videos.
        """
        updated_videos = []
        try:
            for v in cls.videos:
                v.total_views += random.randint(1, 200)
                v.total_likes += random.randint(0, 20)
                v.total_dislikes += random.randint(0, 5)
                v.date_updated = datetime.datetime.now()
                updated_videos.append(v)
                logging.info(f"Updated video {v.video_id} at {v.date_updated}")
            return updated_videos
        except Exception as e:
            logging.error(f"Error updating video stats: {e}", exc_info=True)
            return updated_videos

    @classmethod
    def update_channel_stats(cls) -> None:
        """
        Incrementally update subscribers for a random channel.
        """
        try:
            if cls.channels:
                channel = random.choice(cls.channels)
                channel.subscribers += random.randint(0, 10)
                logging.info(f"Updated subscribers for channel {channel.name} to {channel.subscribers}")
            else:
                logging.warning("No channels available to update.")
        except Exception as e:
            logging.error(f"Error updating channel stats: {e}", exc_info=True)

    @classmethod
    def maybe_add_channel_or_video(cls) -> List[YouTubeVideo]:
        """
        Randomly add a new channel or a new video.
        Returns:
            List[YouTubeVideo]: A list of newly created videos (if any).
        """
        try:
            if random.random() < 0.45:  # 45% chance to create something new
                if random.random() < 0.3:
                    # Create a new channel
                    new_ch = cls.create_channel()
                    cls.channels.append(new_ch)
                    logging.info(f"New channel created: {new_ch.name}")
                    return []
                else:
                    # Create a new video under an existing channel
                    new_videos = []
                    channel = random.choice(cls.channels)
                    new_vid = cls.create_video(channel)
                    cls.videos.append(new_vid)
                    new_videos.append(new_vid)
                    logging.info(f"New video created under channel {channel.name}")
                    return new_videos
            return []
        except Exception as e:
            logging.error(f"Error in maybe_add_channel_or_video: {e}", exc_info=True)
            return []
