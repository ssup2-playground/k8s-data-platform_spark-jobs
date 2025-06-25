import argparse
from datetime import datetime

from utils.spark import init_spark_session_with_iceberg

## Constants
ICEBERG_TABLE = "weather.southkorea_hourly_iceberg_parquet"
ICEBERG_AVG_TABLE = "weather.southkorea_hourly_iceberg_avg_parquet"

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

    # Create spark session
    spark = init_spark_session_with_iceberg()
    spark.sparkContext.setLogLevel("INFO")

    # Check if data exists in Iceberg table for the target date
    check_query = f"""
    SELECT COUNT(*) as record_count
    FROM {ICEBERG_AVG_TABLE}
    WHERE year = {year} AND month = {month} AND day = {day}
    """
    
    result = spark.sql(check_query).collect()
    record_count = result[0]['record_count']
    if record_count == 0:
        print(f"No data found in Iceberg table for date {args.date}")
        return 0
    
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
        COUNT(*) as total_records
    FROM {ICEBERG_AVG_TABLE}
    WHERE year = {year} AND month = {month} AND day = {day}
    GROUP BY branch_name
    """
    
    result_df = spark.sql(avg_query)
    
    # Display results
    result_df.show(truncate=False)
    
    # Save results to MinIO
    result_df.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable(ICEBERG_AVG_TABLE)

if __name__ == "__main__":
    main()