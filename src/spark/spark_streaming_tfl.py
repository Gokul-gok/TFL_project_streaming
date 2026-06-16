#!/usr/bin/env python3
"""
TFL Spark Structured Streaming: Kafka → HDFS Parquet
Reads TFL arrival messages from the tfl_arrivals Kafka topic,
parses JSON, and writes micro-batches as Parquet to HDFS.
Runs for 1 hour then exits cleanly.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)

KAFKA_BROKER = 'ip-172-31-6-42.eu-west-2.compute.internal:9092'
KAFKA_TOPIC  = 'tfl_arrivals'
HDFS_OUTPUT  = '/tmp/gokul/tfl_spark_streaming/output'
CHECKPOINT   = '/tmp/gokul/tfl_spark_streaming/checkpoint'
RUN_SECS     = 3600  # 1 hour — matches producer duration

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
        .appName("TFL_Spark_Streaming") \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    parsed = raw.select(
        from_json(col("value").cast("string"), SCHEMA).alias("d")
    ).select("d.*") \
     .withColumn("ingested_at", current_timestamp())

    query = parsed.writeStream \
        .format("parquet") \
        .option("path", HDFS_OUTPUT) \
        .option("checkpointLocation", CHECKPOINT) \
        .trigger(processingTime="60 seconds") \
        .start()

    print(f"Spark Streaming started  →  reading: {KAFKA_TOPIC}  writing: {HDFS_OUTPUT}")

    query.awaitTermination(RUN_SECS)
    query.stop()
    spark.stop()

    print("Spark Streaming completed successfully.")


if __name__ == '__main__':
    main()
