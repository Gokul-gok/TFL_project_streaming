#!/bin/bash
# Incremental Sqoop import: only new rows in fact_passenger_entry_exit
# Reads last processed entry_exit_id from HDFS watermark, imports rows
# with entry_exit_id greater than that value, then updates the watermark.
# Dimension tables (dim_*) are not re-imported — they are stable reference data.

PG_HOST="13.42.152.118"
PG_PORT="5432"
PG_DB="testdb"
PG_USER="admin"
PG_PASS="admin123"
JDBC="jdbc:postgresql://${PG_HOST}:${PG_PORT}/${PG_DB}"
HDFS_INCREMENTAL="/tmp/gokul_batch/tfl_incremental"
WATERMARK_FILE="/tmp/gokul_batch/watermark/last_entry_exit_id"
MAX_RETRIES=3
RETRY_WAIT=15

# ── Read watermark ────────────────────────────────────────────────────────────
if hdfs dfs -test -f "${WATERMARK_FILE}" 2>/dev/null; then
    LAST_ID=$(hdfs dfs -cat "${WATERMARK_FILE}" 2>/dev/null | tr -d '[:space:]')
    LAST_ID=${LAST_ID:-0}
else
    echo ">>> No watermark found — run the full load first"
    LAST_ID=0
fi

echo ">>> Incremental Sqoop: fact_passenger_entry_exit WHERE entry_exit_id > ${LAST_ID}"

# ── Clean previous incremental staging ───────────────────────────────────────
hdfs dfs -rm -r -f "${HDFS_INCREMENTAL}/fact_passenger_entry_exit" || true
hdfs dfs -mkdir -p "${HDFS_INCREMENTAL}/fact_passenger_entry_exit"

# ── Incremental import with retry ─────────────────────────────────────────────
ATTEMPT=0
SUCCESS=false

while [ $ATTEMPT -lt $MAX_RETRIES ]; do
    ATTEMPT=$((ATTEMPT + 1))
    echo "    Attempt ${ATTEMPT}/${MAX_RETRIES} ..."

    if sqoop import \
        --connect "${JDBC}" \
        --username "${PG_USER}" \
        --password "${PG_PASS}" \
        --query "SELECT * FROM fact_passenger_entry_exit WHERE entry_exit_id > ${LAST_ID} AND \$CONDITIONS" \
        --target-dir "${HDFS_INCREMENTAL}/fact_passenger_entry_exit" \
        --delete-target-dir \
        --num-mappers 1 \
        --fields-terminated-by ',' \
        --lines-terminated-by '\n' \
        -m 1; then
        SUCCESS=true
        break
    else
        if [ $ATTEMPT -lt $MAX_RETRIES ]; then
            echo "    Attempt ${ATTEMPT} failed. Waiting ${RETRY_WAIT}s before retry..."
            sleep ${RETRY_WAIT}
        fi
    fi
done

if [ "${SUCCESS}" != "true" ]; then
    echo ">>> ERROR: Incremental Sqoop failed after ${MAX_RETRIES} attempts" >&2
    exit 1
fi

# ── Count new records imported ────────────────────────────────────────────────
NEW_COUNT=$(hdfs dfs -cat "${HDFS_INCREMENTAL}/fact_passenger_entry_exit/part-"* 2>/dev/null | wc -l)
echo ">>> New records imported: ${NEW_COUNT}"

# ── Update watermark to the new max entry_exit_id ────────────────────────────
if [ "${NEW_COUNT}" -gt 0 ]; then
    NEW_MAX=$(hdfs dfs -cat "${HDFS_INCREMENTAL}/fact_passenger_entry_exit/part-"* 2>/dev/null | \
        python3 -c "
import sys
max_id = 0
for line in sys.stdin:
    parts = line.strip().split(',')
    if parts and parts[0].strip().isdigit():
        max_id = max(max_id, int(parts[0].strip()))
print(max_id)
")
    if [ -n "${NEW_MAX}" ] && [ "${NEW_MAX}" -gt 0 ] 2>/dev/null; then
        echo "${NEW_MAX}" | hdfs dfs -put -f - "${WATERMARK_FILE}"
        echo ">>> Watermark updated: last_entry_exit_id = ${NEW_MAX}"
    fi
else
    echo ">>> No new records — watermark unchanged at ${LAST_ID}"
fi

echo ">>> Incremental Sqoop complete"
