import os

from pyspark.sql import SparkSession

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio.minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "root")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "root123!")

def init_spark_session() -> SparkSession:
    spark = SparkSession.builder \
        .appName("WeatherSouthKoreaDailyAverageParquet") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()
    return spark