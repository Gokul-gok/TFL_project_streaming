-- Hive external table over Spark Structured Streaming output (Parquet)

CREATE DATABASE IF NOT EXISTS gokul_tfl_proj;

DROP TABLE IF EXISTS gokul_tfl_proj.tfl_spark_arrivals;

CREATE EXTERNAL TABLE gokul_tfl_proj.tfl_spark_arrivals (
    id              STRING,
    vehicleId       STRING,
    stationName     STRING,
    lineName        STRING,
    platformName    STRING,
    expectedArrival STRING,
    timeToStation   INT,
    currentLocation STRING,
    direction       STRING,
    destinationName STRING,
    timestamp       STRING,
    ingested_at     TIMESTAMP
)
STORED AS PARQUET
LOCATION '/tmp/gokul/tfl_spark_streaming/output';

SELECT COUNT(*) AS total_streaming_records
FROM gokul_tfl_proj.tfl_spark_arrivals;
