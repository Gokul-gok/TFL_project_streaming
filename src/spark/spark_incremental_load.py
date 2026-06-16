#!/usr/bin/env python3
"""
TFL Spark Incremental Load: Kafka → HDFS Parquet (new messages only)

Reads the Kafka offset watermark from HDFS, consumes only messages
published since the last run, appends them to the incremental output
directory, then updates the watermark with the new latest offsets.
"""

import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

KAFKA_BROKER    = 'ip-172-31-6-42.eu-west-2.compute.internal:9092'
KAFKA_TOPIC     = 'tfl_arrivals'
HDFS_OUTPUT     = '/tmp/gokul_batch/tfl_incremental/kafka_output'
WATERMARK_FILE  = '/tmp/gokul_batch/watermark/kafka_offsets.json'

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


def _hdfs_read(spark, path):
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    p  = spark._jvm.org.apache.hadoop.fs.Path(path)
    if not fs.exists(p):
        return None
    stream = fs.open(p)
    reader = spark._jvm.java.io.BufferedReader(
                 spark._jvm.java.io.InputStreamReader(stream))
    lines = []
    line = reader.readLine()
    while line is not None:
        lines.append(line)
        line = reader.readLine()
    reader.close()
    return "\n".join(lines)


def _hdfs_write(spark, path, content):
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    p  = spark._jvm.org.apache.hadoop.fs.Path(path)
    out    = fs.create(p, True)
    writer = spark._jvm.java.io.PrintWriter(out)
    writer.print(content)
    writer.close()


def main():
    spark = SparkSession.builder \
        .appName("TFL_Incremental_Load") \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # ── Read watermark ────────────────────────────────────────────────────────
    raw_wm = _hdfs_read(spark, WATERMARK_FILE)
    if raw_wm:
        starting_offsets = raw_wm.strip()
        print(f"Resuming from watermark: {starting_offsets}")
    else:
        starting_offsets = "earliest"
        print("No watermark found — reading from earliest")

    # ── Read new Kafka messages ───────────────────────────────────────────────
    raw = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", starting_offsets) \
        .option("endingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    total = raw.count()
    print(f"New messages since last run: {total}")

    if total == 0:
        print("Nothing to process — pipeline is up to date.")
        spark.stop()
        return

    # ── Parse and write ───────────────────────────────────────────────────────
    parsed = raw.select(
        from_json(col("value").cast("string"), SCHEMA).alias("d"),
        col("partition"),
        col("offset"),
    ).select("d.*", "partition", "offset") \
     .withColumn("ingested_at", current_timestamp())

    parsed.drop("partition", "offset").write \
        .mode("append") \
        .partitionBy("lineName") \
        .parquet(HDFS_OUTPUT)

    print(f"Written {total} records to {HDFS_OUTPUT}")

    # ── Update watermark: next run starts after these offsets ─────────────────
    rows = raw.groupBy("partition").agg({"offset": "max"}).collect()
    new_offsets = {KAFKA_TOPIC: {str(r["partition"]): r["max(offset)"] + 1 for r in rows}}
    _hdfs_write(spark, WATERMARK_FILE, json.dumps(new_offsets))
    print(f"Watermark updated: {new_offsets}")

    spark.stop()
    print("Incremental Spark load complete.")


if __name__ == '__main__':
    main()
