import argparse
from datetime import datetime

from pyiceberg.table import Table

from utils.spark import init_spark_session_with_iceberg     
from utils.iceberg import get_iceberg_catalog

## Constants
MINIO_BUCKET = "weather"
ICEBERG_DAILY_AVERAGE_PYICEBERG_TABLE = "weather.southkorea_daily_average_iceberg_parquet"
ICEBERG_DAILY_AVERAGE_SPARK_TABLE = "iceberg.weather.southkorea_daily_average_iceberg_parquet"
ICEBERG_DAILY_SPARK_TABLE = "iceberg.weather.southkorea_daily_iceberg_parquet"

## Functions
def check_partition_exists_by_date(iceberg_table: Table, year: int, month: int, day: int) -> bool:
    '''Check if a specific partition exists using inspect.partitions()'''
    date_list = iceberg_table.inspect.partitions()["partition"].to_pylist()
    date_set = set(tuple(date.values()) for date in date_list)
    return (year, month, day) in date_set

## Main
def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Weather South Korea Daily Average Analysis')
    parser.add_argument('--date', required=True, help='Target date (YYYY-MM-DD)')
    args = parser.parse_args()
    
    # Validate date format
    try:
        target_date = datetime.strptime(args.date, '%Y%m%d')
        year = target_date.year
        month = target_date.month
        day = target_date.day
    except ValueError:
        print("Error: Date format should be YYYYMMDD")
        return

    # Get daily average table
    catalog = get_iceberg_catalog()
    daily_average_table = catalog.load_table(ICEBERG_DAILY_AVERAGE_PYICEBERG_TABLE)

    # Check if data exists in Iceberg table for the target date
    if check_partition_exists_by_date(daily_average_table, year, month, day):
        print(f"Data already exists in Iceberg table for date {args.date}")
        return 0

    # Create spark session
    spark = init_spark_session_with_iceberg()
    spark.sparkContext.setLogLevel("INFO")

    # Calculate average
    avg_query = f"""
    SELECT
        branch_name,

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

        year,
        month,
        day
    FROM {ICEBERG_DAILY_SPARK_TABLE}
    WHERE year = {year} AND month = {month} AND day = {day}
    GROUP BY branch_name, year, month, day
    """
    
    result_df = spark.sql(avg_query)
    
    # Display results
    result_df.show(truncate=False)
    
    # Save results to MinIO
    result_df.write \
        .format("iceberg") \
        .mode("append") \
        .partitionBy("year", "month", "day") \
        .saveAsTable(ICEBERG_DAILY_AVERAGE_SPARK_TABLE)

if __name__ == "__main__":
    main()