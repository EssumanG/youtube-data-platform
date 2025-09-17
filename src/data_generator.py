from dataclasses import dataclass
from faker import Faker
import random
import uuid
import time
from simulator import Simulator
from utils import write_videos_to_csv
import datetime









if __name__ == "__main__":
    simulator = Simulator()
    simulator.setup()
    buffer = []
    last_flush = time.time()
    BATCH_INTERVAL = 300  #10 minutes


    while True:
        # update stats
        new_updates = simulator.update_video_stats()

        # maybe create new channels/videos
        new_vidoes = simulator.maybe_add_channel_or_video()
        buffer.extend(new_updates + new_vidoes)
   
        # check if 10 minutes have passed
        if time.time() - last_flush >= BATCH_INTERVAL:
            if buffer:
                write_videos_to_csv(buffer)
                print(f"flushed {len(buffer)} updates to CSV at {datetime.datetime.now()}")
                buffer.clear()
            last_flush = time.time()  # reset timer

        print(f"Buffered {len(buffer)} updates so far "
              f"(total: {len(simulator.videos)} videos, {len(simulator.channels)} channels)")

        time.sleep(random.randint(10, 30))  # wait before next update