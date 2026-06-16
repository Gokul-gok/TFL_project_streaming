#!/bin/bash
# Save watermarks for subsequent incremental runs.
# Must be called after the full Sqoop import and Spark full load complete.

HDFS_STAGING="/tmp/gokul_batch/tfl_project1"
WATERMARK_DIR="/tmp/gokul_batch/watermark"
KAFKA_BROKER="ip-172-31-6-42.eu-west-2.compute.internal:9092"
KAFKA_TOPIC="tfl_arrivals"

hdfs dfs -mkdir -p "${WATERMARK_DIR}"

# ── 1. Sqoop watermark: max entry_exit_id from the imported CSV ───────────────
echo "=== Saving Sqoop watermark ==="
hdfs dfs -cat "${HDFS_STAGING}/fact_passenger_entry_exit/part-"* | \
    python3 -c "
import sys
max_id = 0
for line in sys.stdin:
    parts = line.strip().split(',')
    if parts and parts[0].strip().isdigit():
        max_id = max(max_id, int(parts[0].strip()))
print(max_id)
" | hdfs dfs -put -f - "${WATERMARK_DIR}/last_entry_exit_id"

echo "Sqoop watermark (last entry_exit_id):"
hdfs dfs -cat "${WATERMARK_DIR}/last_entry_exit_id"

# ── 2. Kafka watermark: current latest offsets per partition ──────────────────
echo "=== Saving Kafka offset watermark ==="
kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list "${KAFKA_BROKER}" \
    --topic "${KAFKA_TOPIC}" \
    --time -1 2>/dev/null | \
    python3 -c "
import sys, json
offsets = {}
for line in sys.stdin:
    line = line.strip()
    if line.count(':') == 2:
        parts = line.split(':')
        offsets[parts[1]] = int(parts[2])
print(json.dumps({'tfl_arrivals': offsets}))
" | hdfs dfs -put -f - "${WATERMARK_DIR}/kafka_offsets.json"

echo "Kafka watermark (latest offsets):"
hdfs dfs -cat "${WATERMARK_DIR}/kafka_offsets.json"
echo ""
echo "Watermark saved to ${WATERMARK_DIR}"
