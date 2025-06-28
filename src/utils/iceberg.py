import os

from pyiceberg.catalog.hive import HiveCatalog

HIVE_CATALOG_URI = os.getenv("HIVE_CATALOG_URI", "thrift://hive-metastore.hive-metastore:9083")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio.minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "root")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "root123!")

def get_iceberg_catalog() -> HiveCatalog:
    return HiveCatalog(
        "default",
        **{
            "uri": HIVE_CATALOG_URI,
            "s3.endpoint": f"http://{MINIO_ENDPOINT}",
            "s3.access-key-id": MINIO_ACCESS_KEY,
            "s3.secret-access-key": MINIO_SECRET_KEY,
            "hive.hive2-compatible": True
        }
    )