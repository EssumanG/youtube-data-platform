from dataclasses import dataclass
from faker import Faker
import random
import uuid
import time
from simulator import Simulator
from utils import write_videos_to_csv, connect_minio
import datetime
from models import YouTubeVideo
from typing import List
import copy









if __name__ == "__main__":
    simulator = Simulator()
    simulator.setup()
    buffer = []
    last_flush = time.time()
    BATCH_INTERVAL = 100  #10 minutes
    minio_client = connect_minio()


    while True:
        # update stats
            # update stats
        new_updates = simulator.update_video_stats()

        # snapshot each updated video so later mutations don't overwrite them
        snapshot_updates = [copy.deepcopy(v) for v in new_updates]

        # maybe create new channels/videos
        new_videos = simulator.maybe_add_channel_or_video()
        snapshot_new_videos = [copy.deepcopy(v) for v in new_videos]

        # add snapshots to buffer
        buffer += (snapshot_updates + snapshot_new_videos)
        print("buffer", buffer)
   
        # check if 10 minutes have passed
        if time.time() - last_flush >= BATCH_INTERVAL:
            if buffer:
                write_videos_to_csv(buffer, minio_client=minio_client)
                print(f"flushed {len(buffer)} updates to CSV at {datetime.datetime.now()}")
                buffer.clear()
            last_flush = time.time()  # reset timer

        print(f"Buffered {len(buffer)} updates so far ",
              f"(total: {len(simulator.videos)} videos, {len(simulator.channels)} channels)")

        time.sleep(random.randint(10, 30))  # wait before next update