from etl.utils import connect_minio, MINIO_BUCKET_NAME

def get_batch_data():
    """Extract list of raw files from MinIO bucket."""
    minio_client = connect_minio()
    objects = list(minio_client.list_objects(MINIO_BUCKET_NAME, prefix="raw", recursive=True))
    # print(f"Found {len(objects)} files in {raw_bucker_dir}")
    return [obj.object_name for obj in objects]