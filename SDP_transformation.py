from pyspark import pipelines as dp
from pyspark.sql import functions as F
SOURCE_PATH = "/Volumes/energy_dev/smart_meter/landing/readings/"
SOURCE_PATH2 = "/Volumes/energy_dev/smart_meter/landing/new_read/"

@dp.table(name="bronze_meter_reading_new")
def bronze_meter_readings_new():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.partitionColumns", "")
        .option("pathGlobFilter", "*.json")
        .schema("""
            reading_id STRING,
            meter_id STRING,
            reading_time TIMESTAMP,
            interval_kwh DOUBLE
        """)
        .load(SOURCE_PATH2)
        .withColumn("ingested_at", F.current_timestamp())
    )

@dp.table(name="bronze_meter_readings")
def bronze_meter_readings():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema("""
            reading_id STRING,
            meter_id STRING,
            reading_time TIMESTAMP,
            interval_kwh DOUBLE
        """)
        .load(SOURCE_PATH)
        .withColumn("ingested_at", F.current_timestamp())
    )

@dp.table(name="silver_meter_readings")
@dp.expect_or_drop("valid_reading_id", "reading_id IS NOT NULL")
@dp.expect_or_drop("valid_meter_id", "meter_id IS NOT NULL")
@dp.expect_or_drop("valid_time", "reading_time IS NOT NULL")
@dp.expect_or_drop(
    "valid_consumption",
    "interval_kwh IS NOT NULL AND interval_kwh >= 0"
)
def silver_meter_readings():
    return spark.readStream.table("bronze_meter_readings")

@dp.table(name="silver_meter_readings_new")
@dp.expect_or_drop("valid_reading_id", "reading_id IS NOT NULL")
@dp.expect_or_drop("valid_meter_id", "meter_id IS NOT NULL")
@dp.expect_or_drop("valid_time", "reading_time IS NOT NULL")
@dp.expect_or_drop(
    "valid_consumption",
    "interval_kwh IS NOT NULL AND interval_kwh >= 0"
)
def silver_meter_readings_new():
    return spark.readStream.table("bronze_meter_reading_new")


@dp.materialized_view(name="gold_daily_consumption")
def gold_daily_consumption():
    return (
        spark.read.table("silver_meter_readings")
        .withColumn("reading_date", F.to_date("reading_time"))
        .groupBy("meter_id", "reading_date")
        .agg(
            F.round(F.sum("interval_kwh"), 2).alias("daily_kwh"),
            F.count("*").alias("reading_count")
        )
    )


@dp.materialized_view(name="gold_daily_consumption_new")
def gold_daily_consumption_new():
    return (
        spark.read.table("silver_meter_readings_new")
        .withColumn("reading_date", F.to_date("reading_time"))
        .groupBy("meter_id", "reading_date")
        .agg(
            F.round(F.sum("interval_kwh"), 2).alias("daily_kwh"),
            F.count("*").alias("reading_count")
        )
    )