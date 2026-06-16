#!/usr/bin/env python3
"""
TFL Spark Full Load Batch Job
Reads ALL historical TFL arrival data from Kafka topic tfl_arrivals
(earliest → latest), deduplicates on id, partitions by line and
arrival date, and writes Parquet to HDFS.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)

KAFKA_BROKER = 'ip-172-31-6-42.eu-west-2.compute.internal:9092'
KAFKA_TOPIC  = 'tfl_arrivals'
HDFS_OUTPUT  = '/tmp/gokul_batch/tfl_full_load/output'

SCHEMA = StructType([
    StructField("id",              StringType(),  True),
    StructField("vehicleId",       StringType(),  True),
    StructField("stationName",     StringType(),  True),
    StructField("lineName",        StringType(),  True),
    StructField("platformName",    StringType(),  True),
    StructField("expectedArrival", StringType(),  True),
    StructField("timeToStation",   IntegerType(), True),
    StructField("currentLocation", StringType(),  True),
    StructField("direction",       StringType(),  True),
    StructField("destinationName", StringType(),  True),
    StructField("timestamp",       StringType(),  True),
])


def main():
    spark = SparkSession.builder \
        .appName("TFL_Full_Load") \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Batch read — consume all available messages
    raw = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets",   "latest") \
        .load()

    total_raw = raw.count()
    print(f"Total raw Kafka records: {total_raw}")

    parsed = raw.select(
        from_json(col("value").cast("string"), SCHEMA).alias("d")
    ).select("d.*") \
     .filter(col("id").isNotNull()) \
     .withColumn("ingested_at",  current_timestamp()) \
     .withColumn("arrival_date", to_date(col("expectedArrival")))

    total_parsed = parsed.count()
    print(f"Parsed records (JSON valid): {total_parsed}")

    deduped = parsed.dropDuplicates(["id"])
    total_deduped = deduped.count()
    print(f"After deduplication: {total_deduped} "
          f"({total_parsed - total_deduped} duplicates removed)")

    deduped.write \
        .mode("overwrite") \
        .partitionBy("lineName", "arrival_date") \
        .parquet(HDFS_OUTPUT)

    print("\n=== Records per Line ===")
    deduped.groupBy("lineName") \
           .count() \
           .orderBy("count", ascending=False) \
           .show(10)

    print("\n=== Records per Date ===")
    deduped.groupBy("arrival_date") \
           .count() \
           .orderBy("arrival_date") \
           .show(10)

    print(f"\nFull load complete → {HDFS_OUTPUT}")
    spark.stop()


if __name__ == '__main__':
    main()
