-- Hive external partitioned table over Spark Full Load output (Parquet)

CREATE DATABASE IF NOT EXISTS gokul_tfl_proj;

DROP TABLE IF EXISTS gokul_tfl_proj.tfl_full_load;

CREATE EXTERNAL TABLE gokul_tfl_proj.tfl_full_load (
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
PARTITIONED BY (lineName_part STRING, arrival_date STRING)
STORED AS PARQUET
LOCATION '/tmp/gokul_batch/tfl_full_load/output';

-- Auto-register partitions from HDFS
MSCK REPAIR TABLE gokul_tfl_proj.tfl_full_load;

-- Validation queries
SELECT COUNT(*) AS total_records FROM gokul_tfl_proj.tfl_full_load;

SELECT lineName_part,
       COUNT(*) AS record_count
FROM   gokul_tfl_proj.tfl_full_load
GROUP  BY lineName_part
ORDER  BY record_count DESC;
