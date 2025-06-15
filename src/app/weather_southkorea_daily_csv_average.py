import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

from utils.minio import init_minio_client
from utils.spark import init_spark_session

## Constants
MINIO_BUCKET = "weather"
MINIO_DIRECTORY_SOUTHKOREA_DAILY_CSV = "southkorea/daily_average_csv"

## Functions
def get_daily_average_csv_object_name(date: str) -> str:
    '''Get daily average csv object name'''
    return (
        f"{MINIO_DIRECTORY_SOUTHKOREA_DAILY_CSV}/"
        f"year={int(date[0:4])}/"
        f"month={int(date[4:6])}/"
        f"day={int(date[6:8])}/"
        f"data.csv"
    )

## Main
def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Weather South Korea Daily Average Analysis')
    parser.add_argument('--date', required=True, help='Target date (YYYY-MM-DD)')
    args = parser.parse_args()
    
    # Validate date format
    try:
        target_date = datetime.strptime(args.date, '%Y-%m-%d')
        year = target_date.year
        month = target_date.month
        day = target_date.day
    except ValueError:
        print("Error: Date format should be YYYY-MM-DD")
        return

    # Check if data exists in MinIO
    minio_client = init_minio_client()
    object_average_csv_name = get_daily_average_csv_object_name(args.date)
    try:
        minio_client.stat_object(MINIO_BUCKET, object_average_csv_name)
        print("data already exists in minio")
        return 0
    except Exception as e:
        if "NoSuchKey" not in str(e):
            print("Unexpected error : {0}".format(e))
            return 1
    
    # Create Spark Session and process data
    spark = init_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    query = f"""
    SELECT 
        AVG(temp) as avg_temp,
        AVG(rain) as avg_rain,
        AVG(snow) as avg_snow,
        AVG(cloud_cover_total) as avg_cloud_cover_total,
        AVG(cloud_cover_lowmiddle) as avg_cloud_cover_lowmiddle,
        AVG(cloud_lowest) as avg_cloud_lowest,
        AVG(humidity) as avg_humidity,
        AVG(wind_speed) as avg_wind_speed,
        AVG(pressure_local) as avg_pressure_local,
        AVG(pressure_sea) as avg_pressure_sea,
        AVG(pressure_vaper) as avg_pressure_vaper,
        AVG(dew_point) as avg_dew_point,
        COUNT(*) as total_records
    FROM hive.weather.southkorea_daily_parquet
    WHERE year = {year} AND month = {month} AND day = {day}
    """
    
    result_df = spark.sql(query)
    
    # Add partition columns to the result
    result_df_with_partitions = result_df \
        .withColumn("year", lit(year)) \
        .withColumn("month", lit(month)) \
        .withColumn("day", lit(day))
    
    # Display results
    print(f"=== Weather Daily Average Results for {args.date} ===")
    result_df_with_partitions.show(truncate=False)
    
    # Save results to MinIO
    result_df_with_partitions.write \
        .format("csv") \
        .mode("overwrite") \
        .save(f"s3a://{MINIO_BUCKET}/{object_average_csv_name}")

if __name__ == "__main__":
    main()