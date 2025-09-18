from airflow.decorators import dag, task
from airflow.datasets import Dataset
from pendulum import datetime
from minio import Minio

# Configure MinIO client (adjust endpoint & creds in Airflow Connections instead of hardcoding)
minio_client = Minio(
    endpoint="minio:9000",
    access_key="admin",
    secret_key="password1234",
    secure=False,
)

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
        bucket_name = "datalake"
        objects = list(minio_client.list_objects(bucket_name, recursive=True))
        print(f"Found {len(objects)} files in {bucket_name}")
        return [obj.object_name for obj in objects]

    @task()
    def print_file_info(files: list):
        """
        Print info of the files: name, size, and last modified
        """
        bucket_name = "datalake"
        for file in files:
            obj = minio_client.stat_object(bucket_name, file)
            print(
                f"File: {file}, Size: {obj.size}, LastModified: {obj.last_modified}"
            )

    file_list = get_batch_data()
    print_file_info(file_list)


dag = youtube_pipeline()