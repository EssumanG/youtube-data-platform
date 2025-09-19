from airflow.decorators import task
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from etl.utils import (
    MINIO_BUCKET_NAME, MINIO_ROOT_USER, MINIO_ROOT_PASSWARD, endpoint_url
)


def load_to_postgres(file_info: tuple):
    """
    Load transformed CSVs from MinIO into Postgres tables.
    """
    file_type, file_path = file_info
    hook = PostgresHook(postgres_conn_id="psql_conn", schema="public")

    # Map file_type to table
    table_map = {
        "trend_30s": "public.video_metrics",
        "channel_stats": "public.channel_stats"
    }
    table_name = table_map.get(file_type)
    if not table_name:
        raise ValueError(f"Unknown file_type: {file_type}")

    # Load CSV from MinIO
    df = pd.read_csv(
        f"s3://{MINIO_BUCKET_NAME}/{file_path}",
        storage_options={
            "key": MINIO_ROOT_USER,
            "secret": MINIO_ROOT_PASSWARD,
            "client_kwargs": {"endpoint_url": endpoint_url}
        },
    )

    # Insert rows
    hook.insert_rows(
        table=table_name,
        rows=df.values.tolist(),
        target_fields=list(df.columns),
        replace=True,  # only deduplicates rows in-memory
    )
    return f"Loaded {len(df)} rows into {table_name}"


