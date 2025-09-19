import pandas as pd
import datetime
from etl.utils import (
    MINIO_BUCKET_NAME, MINIO_ROOT_USER, MINIO_ROOT_PASSWARD, endpoint_url
)


def transform_data(files: dict):
    """
    Transform raw CSVs into trend_30s and channel_stats datasets.
    """
    valid_files = files.get("valid_files", [])
    if not valid_files:
        print("No valid files for transformation")
        return []

    # Load and concat
    dfs = [
        pd.read_csv(
            f"s3://{MINIO_BUCKET_NAME}/{f}",
            storage_options={
                "key": MINIO_ROOT_USER,
                "secret": MINIO_ROOT_PASSWARD,
                "client_kwargs": {"endpoint_url": endpoint_url},
            },
        )
        for f in valid_files
    ]
    df = pd.concat(dfs, ignore_index=True)

    # Ensure datetime
    for col in ["date_created", "date_updated"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    df = df.sort_values("date_updated").set_index("date_updated")

    # Trend every 30s
    trend_30s = (
        df.groupby(["description", "channel_name"])
        .resample("30S")
        .agg({"total_views": "max", "total_likes": "max", "total_dislikes": "max"})
        .reset_index()
    )

    # Channel-level stats
    channel_stats = (
        df.groupby(["channel_id", "channel_name", "channel_subscribers"])
        .agg(
            total_videos=("video_id", "count"),
            total_views=("total_views", "max"),
            total_likes=("total_likes", "max"),
            total_dislikes=("total_dislikes", "max"),
        )
        .reset_index()
    )

    # Output paths
    stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    trend_path = f"transformed/trend_30s__{stamp}.csv"
    stats_path = f"transformed/channel_stats__{stamp}.csv"

    # Save back to MinIO
    trend_30s.to_csv(
        f"s3://{MINIO_BUCKET_NAME}/{trend_path}",
        index=False,
        storage_options={
            "key": MINIO_ROOT_USER,
            "secret": MINIO_ROOT_PASSWARD,
            "client_kwargs": {"endpoint_url": endpoint_url},
        },
    )
    channel_stats.to_csv(
        f"s3://{MINIO_BUCKET_NAME}/{stats_path}",
        index=False,
        storage_options={
            "key": MINIO_ROOT_USER,
            "secret": MINIO_ROOT_PASSWARD,
            "client_kwargs": {"endpoint_url": endpoint_url},
        },
    )

    print(f"Generated {trend_path} and {stats_path}")
    return [("trend_30s", trend_path), ("channel_stats", stats_path)]
