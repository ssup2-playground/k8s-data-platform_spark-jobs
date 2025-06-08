from pyspark.sql import SparkSession

def create_spark_session():
    return SparkSession.builder \
        .appName("SouthKorea Weather Average Calculator") \
        .config("spark.sql.warehouse.dir", "s3a://weather/warehouse") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

def calculate_averages():
    spark = create_spark_session()
    
    # SQL query to calculate averages
    query = """
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
        AVG(dew_point) as avg_dew_point
    FROM hive.weather.southkorea_hourly_parquet
    """
    
    # Execute query and show results
    spark.sql(query).show(truncate=False)
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    calculate_averages() 