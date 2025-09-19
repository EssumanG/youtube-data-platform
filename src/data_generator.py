import random
import uuid
import time
import datetime
import copy
import logging
from simulator import Simulator
from utils import write_videos_to_csv, connect_minio
from models import YouTubeVideo
from typing import List
from dotenv import load_dotenv
import os


load_dotenv()
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def main():
    """
    Run the YouTube simulation:
    - Continuously update video/channel stats.
    - Periodically flush updates to MinIO as CSV files.
    """
    try:
        simulator = Simulator()
        simulator.setup()

        buffer: List[YouTubeVideo] = []
        last_flush = time.time()

        # Batch flush interval (600s = 10 minutes)
        BATCH_INTERVAL = os.getenv("BATCH_INTERVAL")
        minio_client = connect_minio()

        while True:
            try:
                # Update existing video stats
                new_updates = simulator.update_video_stats()
                snapshot_updates = [copy.deepcopy(v) for v in new_updates]

                # Maybe add new channels/videos
                new_videos = simulator.maybe_add_channel_or_video()
                snapshot_new_videos = [copy.deepcopy(v) for v in new_videos]

                # Add snapshots to buffer
                buffer.extend(snapshot_updates + snapshot_new_videos)
                logging.info(
                    f"Buffered {len(buffer)} updates so far "
                    f"(total: {len(simulator.videos)} videos, {len(simulator.channels)} channels)"
                )

                # Check if flush interval has passed
                if time.time() - last_flush >= BATCH_INTERVAL:
                    if buffer:
                        write_videos_to_csv(buffer, minio_client=minio_client)
                        logging.info(
                            f"Flushed {len(buffer)} updates to CSV at {datetime.datetime.now()}"
                        )
                        buffer.clear()
                    last_flush = time.time()  # reset timer

                # Random sleep to simulate "real-time" updates
                time.sleep(random.randint(10, 30))

            except Exception as e:
                logging.error(f"Error in simulation loop: {e}", exc_info=True)
                time.sleep(5)  # pause briefly before retrying

    except KeyboardInterrupt:
        logging.warning("Simulation stopped manually (Ctrl+C).")
    except Exception as e:
        logging.critical(f"Fatal error in main: {e}", exc_info=True)


if __name__ == "__main__":
    main()
