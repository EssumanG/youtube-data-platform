from airflow.decorators import dag, task
from airflow.datasets import Dataset
from datetime import datetime
from minio import Minio
from etl.utils import connect_minio
import os

# Configure MinIO client (adjust endpoint & creds in Airflow Connections instead of hardcoding)

MINIO_BUCKET_NAME = os.getenv('MINIO_BUCKET_NAME')
MINIO_ROOT_USER = os.getenv('MINIO_ROOT_USER')
MINIO_ROOT_PASSWARD = os.getenv('MINIO_ROOT_PASSWARD')
MINIO_DOCKER_NAME = os.getenv('MINIO_DOCKER_NAME')
MINIO_DOCKER_PORT = os.getenv('MINIO_DOCKER_PORT')
endpoint_url = f"http://{MINIO_DOCKER_NAME}:{MINIO_DOCKER_PORT}" 
raw_bucker_dir=f"{MINIO_BUCKET_NAME}/raw"

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Essuman", "retries": 3},
    tags=["youtube"],
)
def youtube_pipeline():

    @task()
    def get_batch_data():
        """
        Check for new batch data in MinIO bucket.
        (In real case: replace with S3 sensor or a polling mechanism)
        """
        minio_client = connect_minio()
        objects = list(minio_client.list_objects(MINIO_BUCKET_NAME, prefix="raw", recursive=True))
        print(f"Found {len(objects)} files in {raw_bucker_dir}")
        return [obj.object_name for obj in objects]

    @task()
    def validate_task(file: str):
        """
        Print info of the files: name, size, and last modified
        """
        import pandas as pd
        s3_object_path = f"s3://{MINIO_BUCKET_NAME}/{file}"
        print(f"Validating file: s3://{s3_object_path}")
        try:
            df = pd.read_csv(
                s3_object_path,
                storage_options={
                    "key": MINIO_ROOT_USER,
                    "secret": MINIO_ROOT_PASSWARD,
                    "client_kwargs": {"endpoint_url": endpoint_url}
                })

            required_columns = {"video_id", "description", "channel_id", "total_likes", "total_dislikes", "date_created"}
            if not required_columns.issubset(set(df.columns)):
                print(f"Missing required columns in {s3_object_path}")
                return {"file": file, "is_valid":False}

           
            print("validation passed")
            return {"file": file, "is_valid":True}
        except Exception as e:
            print(f"Validation failed for {file} in bucket {raw_bucker_dir}: {e}")
            return {"file": file, "is_valid":False}

    @task()
    def aggregate_results(results: list[dict]):
        """Split validated results into valid and invalid lists"""
        valid = [r["file"] for r in results if r["is_valid"]]
        invalid = [r for r in results if not r["is_valid"]]
        return {"valid_files": valid, "invalid_files": invalid}
    

    @task()
    def transform_task(files: dict):
        import pandas as pd
        if len(files["valid_files"]) > 0:
            print("Transforming:", files["valid_files"])
        
            dfs = [pd.read_csv(
                    f"s3://{MINIO_BUCKET_NAME}/{f}",
                    storage_options={
                        "key": MINIO_ROOT_USER,
                        "secret": MINIO_ROOT_PASSWARD,
                        "client_kwargs": {"endpoint_url": endpoint_url}
                    }) for f in files["valid_files"]]
            
            df = pd.concat(dfs, ignore_index=True)

            df["date_created"] = pd.to_datetime(df["date_created"])
            df["date_updated"] = pd.to_datetime(df["date_updated"])
            df = df.set_index("date_updated")

            trend_30s = (
                df.groupby(["description", "channel_name"]).resample("30S")
                .agg({
                    "total_views": "max",          # total views
                    "total_likes": "max",
                    "total_dislikes": "max",
                })
                .reset_index()
            )
            
            channel_stats = (
                df.groupby(["channel_id", "channel_name", "channel_subscribers"]) \
                    .agg(
                    total_videos=("video_id", "count"),
                    total_views=("total_views", "max"),
                    total_likes=("total_likes", "max"),
                    total_dislikes=("total_dislikes", "max")
                )
            ).reset_index()
            # Define output paths in MinIO
            trend_path = "transformed/trend_30s.csv"
            stats_path = "transformed/channel_stats.csv"

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

            # Return only paths for downstream tasks
            return [
            ("trend_30s", trend_path),
            ("channel_stats", stats_path),
            ]


    @task()
    def quarantine_task(source_data: dict):
        print("Quarantining:", source_data["invalid_files"])
        files_to_quarantine = source_data.get("in_valid", [])
        if not files_to_quarantine:
            print("No files to quarantine")
            return []


        quarantine_files = []
        date_prefix = datetime.now().strftime("%Y-%m-%d")

        for file_key in files_to_quarantine:
            try:
                file_name = os.path.basename(file_key)
                quarantine_key = f"quarantine/{date_prefix}/{file_name}"

                # Copy to quarantine
                minio_client = connect_minio()
                minio_client.copy_object(
                    MINIO_BUCKET_NAME,
                    quarantine_key,
                    f"/{MINIO_BUCKET_NAME}/{file_key}"
                )

                # Delete original
                minio_client.remove_object(MINIO_BUCKET_NAME, file_key)

                quarantine_files.append(quarantine_key)
                print(f"quarantined {file_key} -> {quarantine_key}")

            except Exception as e:
                print(f"qQarantine failed for {file_key}: {e}")

        return quarantine_files

    @task()
    def load_to_postgres(file_info: tuple):
        import pandas as pd
        from airflow.providers.postgres.hooks.postgres import PostgresHook

         
        file_type, file_path = file_info
        # Hook to Postgres
        hook = PostgresHook(postgres_conn_id="psql_conn", schema="public")

        if file_type == "trend_30s":
            table_name = "public.video_metrics"
        elif file_type == "channel_stats":
            table_name = "public.channel_stats"
        storage_opts = {
            "key": MINIO_ROOT_USER,
            "secret": MINIO_ROOT_PASSWARD,
            "client_kwargs": {"endpoint_url": endpoint_url},
        }

        df = pd.read_csv(f"s3://{MINIO_BUCKET_NAME}/{file_path}", storage_options=storage_opts)

        # Load into Postgres
        hook.insert_rows(
            table=table_name,
            rows=df.values.tolist(),
            target_fields=list(df.columns),
            replace=True  # overwrite for idempotency, optional
        )
    

    @task
    def archive_task(source_data: dict):
        from minio.commonconfig import CopySource
        files_to_archive = source_data.get("valid_files", [])
        if not files_to_archive:
            print("No files to archive")
            return []


        archived_files = []
        date_prefix = datetime.now().strftime("%Y-%m-%d")

        for file_key in files_to_archive:
            try:
                file_name = os.path.basename(file_key)
                archive_key = f"archive/{date_prefix}/{file_name}"
                print(archive_key, "archiving_key-")

                # Copy to archive
                minio_client = connect_minio()
                print("connected")
                minio_client.copy_object(
                    MINIO_BUCKET_NAME,
                    f"{archive_key}",
                    CopySource(MINIO_BUCKET_NAME, file_key),
                )

                # Delete original
                minio_client.remove_object(MINIO_BUCKET_NAME, file_key)

                archived_files.append(archive_key)
                print(f"Archived {file_key} -> {archive_key}")

            except Exception as e:
                print(f"Archive failed for {file_key}: {e}")

        return archived_files
    # def load_task():
    #     pass

    

    file_list = get_batch_data()
    validated_data = validate_task.expand(file=file_list)
    aggregated = aggregate_results(validated_data)

    transform_result = transform_task(aggregated)
    quarantine_task(aggregated)
    archive_data = archive_task(aggregated)
    load_to_postgres.expand(file_info=transform_result)

    transform_result  >> archive_data

dag = youtube_pipeline()