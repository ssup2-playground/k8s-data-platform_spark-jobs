import os

from pyspark.sql import SparkSession

HIVE_METASTORE_URI = os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore.hive-metastore:9083")
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
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    return spark

def init_spark_session_with_iceberg() -> SparkSession:
    spark = SparkSession.builder \
        .appName("WeatherSouthKoreaDailyAverageIceberg") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.hive.metastore.uris", HIVE_METASTORE_URI) \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hive") \
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://iceberg-warehouse") \
        .getOrCreate()
    return spark