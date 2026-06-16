-- TFL Star Schema: external Hive tables pointing at Sqoop-imported HDFS data
-- Database: gokul_tfl_proj

CREATE DATABASE IF NOT EXISTS gokul_tfl_proj;

-- ── Dimension: Date ──────────────────────────────────────────────────────────
DROP TABLE IF EXISTS gokul_tfl_proj.dim_date;
CREATE EXTERNAL TABLE gokul_tfl_proj.dim_date (
    date_id    INT,
    year       INT,
    quarter    INT,
    month      INT,
    period     STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/gokul/tfl_project1/dim_date'
TBLPROPERTIES ('skip.header.line.count'='1');

-- ── Dimension: Lines ─────────────────────────────────────────────────────────
DROP TABLE IF EXISTS gokul_tfl_proj.dim_lines;
CREATE EXTERNAL TABLE gokul_tfl_proj.dim_lines (
    line_id       INT,
    line_name     STRING,
    is_night_tube TINYINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/gokul/tfl_project1/dim_lines'
TBLPROPERTIES ('skip.header.line.count'='1');

-- ── Dimension: Networks ──────────────────────────────────────────────────────
DROP TABLE IF EXISTS gokul_tfl_proj.dim_networks;
CREATE EXTERNAL TABLE gokul_tfl_proj.dim_networks (
    network_id   INT,
    network_name STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/gokul/tfl_project1/dim_networks'
TBLPROPERTIES ('skip.header.line.count'='1');

-- ── Dimension: Stations ──────────────────────────────────────────────────────
DROP TABLE IF EXISTS gokul_tfl_proj.dim_stations;
CREATE EXTERNAL TABLE gokul_tfl_proj.dim_stations (
    station_id   INT,
    station_name STRING,
    network_id   INT,
    service_type STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/gokul/tfl_project1/dim_stations'
TBLPROPERTIES ('skip.header.line.count'='1');

-- ── Fact: Station Lines ──────────────────────────────────────────────────────
DROP TABLE IF EXISTS gokul_tfl_proj.fact_station_lines;
CREATE EXTERNAL TABLE gokul_tfl_proj.fact_station_lines (
    station_id     INT,
    line_id        INT,
    is_interchange INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/gokul/tfl_project1/fact_station_lines'
TBLPROPERTIES ('skip.header.line.count'='1');

-- ── Fact: Passenger Entry / Exit ─────────────────────────────────────────────
DROP TABLE IF EXISTS gokul_tfl_proj.fact_passenger_entry_exit;
CREATE EXTERNAL TABLE gokul_tfl_proj.fact_passenger_entry_exit (
    entry_exit_id BIGINT,
    station_id    INT,
    date_id       INT,
    entry_count   BIGINT,
    exit_count    BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/gokul/tfl_project1/fact_passenger_entry_exit'
TBLPROPERTIES ('skip.header.line.count'='1');

-- ── Incremental watermark (tracks last processed year) ───────────────────────
DROP TABLE IF EXISTS gokul_tfl_proj.incremental_watermark;
CREATE TABLE gokul_tfl_proj.incremental_watermark (
    last_processed_year INT
)
STORED AS PARQUET
LOCATION '/tmp/gokul/tfl_project1/watermark';

-- ── Quick validation ─────────────────────────────────────────────────────────
SELECT 'dim_date',                COUNT(*) FROM gokul_tfl_proj.dim_date
UNION ALL
SELECT 'dim_lines',               COUNT(*) FROM gokul_tfl_proj.dim_lines
UNION ALL
SELECT 'dim_networks',            COUNT(*) FROM gokul_tfl_proj.dim_networks
UNION ALL
SELECT 'dim_stations',            COUNT(*) FROM gokul_tfl_proj.dim_stations
UNION ALL
SELECT 'fact_station_lines',      COUNT(*) FROM gokul_tfl_proj.fact_station_lines
UNION ALL
SELECT 'fact_passenger_entry_exit', COUNT(*) FROM gokul_tfl_proj.fact_passenger_entry_exit;
