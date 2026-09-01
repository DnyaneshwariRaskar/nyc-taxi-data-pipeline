import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp,round

file_path = "data/raw/yellow_tripdata_2025-01.parquet"

if os.path.exists(file_path):
    print("Source file found:", file_path)

    spark = SparkSession.builder \
        .appName("NYCTaxiIngestion") \
        .getOrCreate()

    df = spark.read.parquet(file_path)

    print("Data successfully loaded.")

    df.printSchema()

    df.select(
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "fare_amount",
        "tip_amount",
        "total_amount"
    ).show(5)


    df = df.withColumn(
    "trip_duration_minutes",
    round(
        (
            unix_timestamp(df.tpep_dropoff_datetime)
            - unix_timestamp(df.tpep_pickup_datetime)
        ) / 60,
        2
    )
)
    print("Trip duration calculated:")

    df.select(
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_duration_minutes"
    ).show(5)

    spark.stop()

else:
    print("Source file not found:", file_path)