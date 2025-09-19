from airflow.decorators import dag, task
from datetime import datetime
from etl.extract import get_batch_data
from etl.validate import validate_file, aggregate_results
from etl.transform import transform_data
from etl.move_files import archive_file, quarantine_file
from etl.load import load_to_postgres
from etl.utils import MINIO_BUCKET_NAME
from etl.utils import (
    MINIO_BUCKET_NAME, MINIO_ROOT_USER, MINIO_ROOT_PASSWARD, endpoint_url
)


# Configure MinIO client (adjust endpoint & creds in Airflow Connections instead of hardcoding)


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
    def extract_task():
       return get_batch_data()

    @task()
    def validate_task(file: str):
      return validate_file(file)
    

    @task()
    def aggregate_result_tasks(results: list[dict]):
        return aggregate_results
    

    @task()
    def transform_task(files: dict):
       return transform_data(files)
    

    @task()
    def quarantine_task(source_data: dict):
        return quarantine_file(source_data=source_data)

    @task()
    def load_to_postgres_task(file_info: tuple):
        return load_to_postgres(file_info)
    

    @task
    def archive_task(source_data: dict):
        archive_file(source_data)
    # def load_task():
    #     pass

    

    file_list = extract_task()
    validated_data = validate_task.expand(file=file_list)
    aggregated = aggregate_result_tasks(validated_data)

    transform_result = transform_task(aggregated)
    quarantine_task(aggregated)
    archive_data = archive_task(aggregated)
    load_to_postgres_task.expand(file_info=transform_result)

    transform_result  >> archive_data

dag = youtube_pipeline()