import os

from minio import Minio

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio.minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "root")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "root123!")

# MinIO 
def init_minio_client() -> Minio:
    return Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)