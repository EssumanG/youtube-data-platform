# Mini Data Platform
This project sets up a basic data platform using Astronomer Airflow

The platform demonstrating the four core layer of a data pipeline:
1. Collect Data
2. Process Data 
3. Store Data
4. Visualize data


## Services Used
- **Airflow (Astro CLI)**: Orchestrates data ingestion and processing
- **PostgreSQL**: Database to stores processed data
- **MinIO**:  - file storage (like AWS S3)
- **Metabase**: creates charts and reports

## Services Overview
1. ### **Data Generator**
    - Simulates YouTube video datasets (channels, videos, stats).
    - Periodically generates new CSV snapshots of updated video statistics.
    - Uploads CSVs to MinIO (acting as raw storage).

2. ### **Minio**
    - Acts as the object store (S3-compatible).
    - Holds:
        - Raw data (CSV files from the generator)
        - Archived data (after processing)
        - Quarantined files (invalid schema files)
        - Transformed datasets (Staging for downstream load and analytics)

3. ### **Airflow (via Astro CLI)** 
    - Orchestrates the end-to-end pipeline.
    - DAG workflow (triggered when new files arrive in MinIO – trigger mechanism pending):
        1. **Extract**: Batch raw CSV files from MinIO.
        2. **Validate**: Check schema/columns.
            - Valid files → transformation
            - Invalid files → quarantine folder in MinIO
        3. **Transform:** Build derived datasets:
            - Trend 30s – Rolling metrics sampled every 30 seconds
            - Channel Stats – Aggregated statistics per channel
        4. **Load**: Insert transformed datasets into PostgreSQL tables.
        5. **Archive**: Move processed raw files into an archive/ folder in MinIO.

4. ### **PostgresSQL**
    - Stores transformed datasets for downstream analytics.
    - Two key tables (from current design):
        - `video_metrics` (trend data at 30s intervals)
        - `channel_stats` (aggregated per channel)

5. ### **Metabase (planned)**
    - Will be used to visualize the data stored in PostgreSQL.
    - Planned dashboards:
        - Trending videos over time
        - Channel growth insights
        - Engagement (likes/dislikes vs views)

## Setup
1. Install Dependancies
    - Docker
    - Astronemer

2. Clone this repo
```bash
git clone https://github.com/your-username/mini-data-platform.git
cd mini-data-platform
```
3. Set envronment variables.
Create a .env file in the project root directory and copy the evnironment in the the **`example.env`** and paste in the .env file

4. Start the platform
```
astro dev
```
This will spin up the services below:
    - Airflow (scheduler, webserver, workers)
    - PostgreSQL
    - MinIO
    - (Metabase container can be added later)
- 

---

### Project Sructure

```
├── dags/                        # Airflow DAG definitions
├── etl/                         # Extract, validate, transform, load helpers
│   ├── extract.py
│   ├── validate.py
│   ├── transform.py
│   ├── load.py
│   ├── move_files.py
│   └── archive.py
├── simulator/                   # Data generator scripts
├── docker-compose.override.yml  # Extra service configs (Postgres, MinIO, etc.)
├── .env                         # Environment variables
└── README.md
```

## Dataset Schema

- ### **Raw Data (CSV uploaded by simulator)**:
Each generated file represents a batch snapshot of YouTube video stats.

| Column                | Type     | Description                              |
| --------------------- | -------- | ---------------------------------------- |
| `video_id`            | string   | Unique identifier for a video            |
| `channel_id`          | string   | Unique identifier for a channel          |
| `channel_name`        | string   | Channel’s name                           |
| `channel_subscribers` | int      | Subscriber count at the time of snapshot |
| `description`         | string   | Video description text                   |
| `total_views`         | int      | Total view count                         |
| `total_likes`         | int      | Total like count                         |
| `total_dislikes`      | int      | Total dislike count                      |
| `date_created`        | datetime | When video was first created             |
| `date_updated`        | datetime | When snapshot was taken                  |

- ### **Transformed Datasets**
Trend 30s (`video_metrics` table)
Rolling metrics sampled at 30-second intervals.

| Column           | Type     | Description                       |
| ---------------- | -------- | --------------------------------- |
| `date_updated`   | datetime | Time window (30s interval)        |
| `description`    | string   | Video description                 |
| `channel_name`   | string   | Channel name                      |
| `total_views`    | int      | Max views observed in interval    |
| `total_likes`    | int      | Max likes observed in interval    |
| `total_dislikes` | int      | Max dislikes observed in interval |

- ### **Channel Stats(`channel_stats` table)**:
Aggregated statistics per channel.

| Column                | Type   | Description                      |
| --------------------- | ------ | -------------------------------- |
| `channel_id`          | string | Channel ID                       |
| `channel_name`        | string | Channel name                     |
| `channel_subscribers` | int    | Latest subscriber count          |
| `total_videos`        | int    | Number of videos in dataset      |
| `total_views`         | int    | Max total views across videos    |
| `total_likes`         | int    | Max total likes across videos    |
| `total_dislikes`      | int    | Max total dislikes across videos |


## Next Steps
- Containerize the data generator
- Implement event-based DAG triggering on MinIO upload
- Add Metabase service and dashboards
- Automate CI/CD for Airflow DAGs and ETL scripts