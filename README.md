# TFL Data Engineering Project — Streaming & Batch Pipelines

End-to-end data pipeline ingesting **Transport for London (TFL) real-time bus/tube arrival data** using Kafka, Spark, Sqoop, Hive, and HBase — orchestrated by Jenkins on a Cloudera CDH cluster.

---

## Architecture Overview

### Streaming Pipeline

```mermaid
flowchart LR
    API["TFL REST API\n(External)"]
    PROD["Python Kafka Producer\nnohup · background"]
    KAFKA[("Kafka\ntopic: tfl_arrivals")]
    SPARK["Spark Structured Streaming\nspark_streaming_tfl.py"]
    HBASE[("HBase\ntable: tfl_arrivals")]
    OUT[("HDFS Output\nParquet")]
    CHKPT[("HDFS Checkpoint")]
    HIVE_S[("Hive\ntfl_spark_arrivals")]

    API -->|"HTTP poll"| PROD
    PROD -->|"JSON messages"| KAFKA
    KAFKA -->|"write_kafka_to_hbase.py"| HBASE
    KAFKA -->|"micro-batch"| SPARK
    SPARK -->|"Parquet append"| OUT
    SPARK -.->|"offset state"| CHKPT
    OUT -->|"external table"| HIVE_S
```

### Batch Pipeline — Full Load

```mermaid
flowchart TD
    PG[("PostgreSQL\n13.42.152.118:5432")]
    KAFKA[("Kafka\ntopic: tfl_arrivals")]

    PG -->|"sqoop_import.sh\n6 tables"| STAG[("HDFS Staging\ntfl_project1/")]
    STAG -->|"spark_gold_layer.py\n7 aggregations"| GOLD[("HDFS Gold Layer\ntfl_project1/gold/")]
    KAFKA -->|"spark_full_load_tfl.py\nearliest → latest"| FOUT[("HDFS Full Load\ntfl_full_load/output")]
    STAG -->|"hive_table.sql"| HIVE[("Hive\ngokul_tfl_proj")]
    GOLD -->|"hive_table.sql"| HIVE
    FOUT -->|"hive_full_load_table.sql"| HIVE
```

### Batch Pipeline — Incremental Load

```mermaid
flowchart TD
    PG[("PostgreSQL")]
    KAFKA[("Kafka\ntfl_arrivals")]
    STAG[("HDFS Staging\nfact_passenger_entry_exit")]

    STAG -->|"save_watermark.sh"| WM[("HDFS Watermark\nlast_entry_exit_id\nkafka_offsets.json")]
    WM -.->|"read last_id"| ISQ
    PG -->|"sqoop_incremental.sh\nWHERE entry_exit_id > last_id"| ISQ[("HDFS Incremental\nfact_passenger_entry_exit")]
    WM -.->|"read saved offsets"| IKF
    KAFKA -->|"spark_incremental_load.py\nsaved_offset → latest"| IKF[("HDFS Incremental\nkafka_output")]
    ISQ -->|"update watermark"| WM
    IKF -->|"update kafka_offsets"| WM
```

---

## Pipelines

### Streaming Pipeline (`jenkins_streaming`) — 13 Stages

| Stage | Name | Action |
|---|---|---|
| 1 | Checkout | Clone repo from GitHub |
| 2 | Prepare Remote Directory | Create `/home/ec2-user/gokul_tfl/{kafka,spark,hive}` |
| 3 | Copy Scripts to Cloudera | SCP producer, consumer, streaming, Hive SQL |
| 4 | Set Permissions | `chmod +x` on all Python scripts |
| 5 | Kill Previous Jobs | `pkill` any lingering producer/consumer/Spark processes |
| 6 | Prepare HDFS Directories | Clean and recreate output + checkpoint dirs |
| 7 | Start Kafka Producer | `nohup` TFL API → Kafka topic `tfl_arrivals` |
| 8 | Start HBase Consumer | `nohup` Kafka → HBase table `tfl_arrivals` |
| 9 | Start Spark Streaming | `nohup spark-submit` Kafka → HDFS Parquet (micro-batch) |
| 10 | Verify Kafka Messages | Print message count in topic |
| 11 | Verify HBase Records | `count` on HBase table |
| 12 | Create Hive Table | `beeline` DDL for `tfl_spark_arrivals` external table |
| 13 | Analyse Logs | Tail producer, HBase, Spark logs |

### Batch Pipeline (`Jenkinsfile`) — 16 Stages

| Stage | Name | Action |
|---|---|---|
| 1 | Checkout | Clone repo from GitHub |
| 2 | Prepare Remote Directory | Create `{sqoop,hive,spark}` subdirs |
| 3 | Copy Scripts to Cloudera | SCP all shell scripts, SQL files, Python scripts |
| 4 | Set Permissions | `chmod +x` on all scripts |
| 5 | Prepare Staging Directory | `hdfs dfs -mkdir -p` for all output paths |
| 6 | Clean HDFS | Remove previous staging/gold/full-load data |
| 7 | Run Sqoop Import | Full import of 6 PostgreSQL tables → HDFS CSV |
| 8 | Create Hive Tables | Star schema DDL via beeline |
| 9 | Run Spark Gold Layer | 7 aggregation queries → HDFS Parquet |
| 10 | Run Spark Full Load | Kafka earliest→latest → HDFS Parquet |
| 11 | Create Hive Full Load Table | DDL for `tfl_full_load` external table |
| 12 | Verify Results | Print HDFS listings and Hive row counts |
| 13 | Save Watermark | Save `max(entry_exit_id)` and Kafka offsets to HDFS |
| 14 | Run Incremental Sqoop | Import only new fact rows (`WHERE entry_exit_id > last_id`) |
| 15 | Run Spark Incremental Load | Kafka from saved offset → latest → HDFS Parquet |
| 16 | Verify Incremental Results | Print new row counts and current watermark values |

---

## Infrastructure

| Component | Value |
|---|---|
| Jenkins | `51.24.13.205:8081` |
| Cloudera EC2 | `13.41.167.97` (ec2-user) |
| PostgreSQL | `13.42.152.118:5432 / testdb` |
| Kafka Broker | `ip-172-31-6-42.eu-west-2.compute.internal:9092` |
| Kafka Topic | `tfl_arrivals` |
| HBase Table | `tfl_arrivals` |
| Hive Database | `gokul_tfl_proj` |

---

## File Structure

```
TFL_project_streaming/
├── jenkins_streaming        # Streaming pipeline (13 stages)
├── Jenkinsfile              # Batch pipeline (16 stages)
└── src/
    ├── kafka/
    │   ├── send_data_to_kafka.py       # TFL API → Kafka producer
    │   └── write_kafka_to_hbase.py     # Kafka → HBase consumer
    ├── spark/
    │   ├── spark_streaming_tfl.py      # Spark Structured Streaming
    │   ├── spark_gold_layer.py         # Batch gold layer aggregations
    │   ├── spark_full_load_tfl.py      # Kafka full load → HDFS
    │   ├── spark_incremental_load.py   # Kafka incremental load (offset watermark)
    │   ├── hive_spark_table.sql        # Hive DDL: tfl_spark_arrivals
    │   └── hive_full_load_table.sql    # Hive DDL: tfl_full_load
    ├── sqoop_import.sh                 # Full Sqoop import (6 tables, retry logic)
    ├── sqoop_incremental.sh            # Incremental Sqoop (WHERE id > watermark)
    ├── save_watermark.sh               # Save max entry_exit_id + Kafka offsets
    └── hive_table.sql                  # Hive DDL: star schema tables
```

---

## HDFS Paths

| Pipeline | Path | Contents |
|---|---|---|
| Streaming | `/tmp/gokul/tfl_spark_streaming/output` | Streaming Parquet output |
| Streaming | `/tmp/gokul/tfl_spark_streaming/checkpoint` | Spark checkpoint |
| Batch | `/tmp/gokul_batch/tfl_project1/dim_date` | Sqoop: dim_date |
| Batch | `/tmp/gokul_batch/tfl_project1/dim_stations` | Sqoop: dim_stations |
| Batch | `/tmp/gokul_batch/tfl_project1/dim_networks` | Sqoop: dim_networks |
| Batch | `/tmp/gokul_batch/tfl_project1/dim_lines` | Sqoop: dim_lines |
| Batch | `/tmp/gokul_batch/tfl_project1/fact_station_lines` | Sqoop: fact_station_lines |
| Batch | `/tmp/gokul_batch/tfl_project1/fact_passenger_entry_exit` | Sqoop: fact table |
| Batch | `/tmp/gokul_batch/tfl_project1/gold/` | Gold layer (7 Parquet aggregations) |
| Batch | `/tmp/gokul_batch/tfl_full_load/output` | Kafka full load Parquet |
| Batch | `/tmp/gokul_batch/watermark/last_entry_exit_id` | Sqoop watermark |
| Batch | `/tmp/gokul_batch/watermark/kafka_offsets.json` | Kafka offset watermark |
| Batch | `/tmp/gokul_batch/tfl_incremental/fact_passenger_entry_exit` | Incremental Sqoop output |
| Batch | `/tmp/gokul_batch/tfl_incremental/kafka_output` | Incremental Spark output |

---

## Hive Tables (`gokul_tfl_proj`)

| Table | Pipeline | Description |
|---|---|---|
| `dim_date` | Batch | Date dimension |
| `dim_stations` | Batch | Station dimension (12 cols) |
| `dim_networks` | Batch | Network dimension |
| `dim_lines` | Batch | Line dimension |
| `fact_station_lines` | Batch | Station–line bridge table |
| `fact_passenger_entry_exit` | Batch | Passenger entry/exit fact table |
| `tfl_full_load` | Batch | Kafka full load (partitioned by lineName, date) |
| `tfl_spark_arrivals` | Streaming | Spark structured streaming output |

---

## Log Paths

| Location | Path | Contents |
|---|---|---|
| EC2 host | `/tmp/gokul_producer.log` | Kafka producer output |
| EC2 host | `/tmp/gokul_hbase.log` | HBase consumer output |
| EC2 host | `/tmp/gokul_spark_streaming.log` | Spark streaming job output |
| Jenkins UI | `http://51.24.13.205:8081/job/streaming_gokul/<build>/console` | Streaming build log |
| Jenkins UI | `http://51.24.13.205:8081/job/batch_gokul/<build>/console` | Batch build log |
| Jenkins server | `/var/lib/jenkins/jobs/streaming_gokul/builds/<N>/log` | Raw streaming log file |
| Jenkins server | `/var/lib/jenkins/jobs/batch_gokul/builds/<N>/log` | Raw batch log file |
| HDFS | `hdfs:///user/spark/applicationHistory/` | Spark application event history |
| HDFS | `hdfs:///user/spark/driverLogs/` | Spark driver logs |
