from airflow.decorators import dag, task
from airflow.datasets import Dataset
from pendulum import datetime
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
        objects = list(minio_client.list_objects(MINIO_BUCKET_NAME, recursive=True))
        print(f"Found {len(objects)} files in {MINIO_BUCKET_NAME}")
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
            print(f"Validation failed for {file} in bucket {MINIO_BUCKET_NAME}: {e}")
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
        # df = df[["video_id", "description", "channel_name", "total_views", "total_likes", "total_dislikes"]]

        trend_30s = (
            df.groupby(["description", "channel_name"]).resample("30S")
            .agg({
                "video_id": "count",           # number of videos uploaded
                "total_views": "max",          # total views
                "total_likes": "max",
                "total_dislikes": "max",
            })
            .rename(columns={"video_id": "video_count"}).reset_index()
        )
        
        channel_stats = (
            df.groupby("channel_name") \
                .agg(
                total_videos=("video_id", "count"),
                total_views=("total_views", "sum"),
                avg_views=("total_views", "mean"),
                avg_likes=("total_likes", "mean"),
                avg_dislikes=("total_dislikes", "mean")
            )
        )

        return trend_30s, channel_stats



    @task()
    def quarantine_task(files: dict):
        print("Quarantining:", files["invalid_files"])

    # @task()
    # def load_task():
    #     pass

    

    file_list = get_batch_data()
    validated_data = validate_task.expand(file=file_list)
    aggregated = aggregate_results(validated_data)

    transform_task(aggregated)
    quarantine_task(aggregated)

dag = youtube_pipeline()