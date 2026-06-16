#!/bin/bash
# Sqoop import: PostgreSQL → HDFS (6 tables for TFL star schema)

PG_HOST="13.42.152.118"
PG_PORT="5432"
PG_DB="testdb"
PG_USER="admin"
PG_PASS="admin123"
HDFS_BASE="/tmp/gokul_batch/tfl_project1"
JDBC="jdbc:postgresql://${PG_HOST}:${PG_PORT}/${PG_DB}"
MAX_RETRIES=3
RETRY_WAIT=15

TABLES=(
    "dim_date"
    "dim_stations"
    "dim_networks"
    "dim_lines"
    "fact_station_lines"
    "fact_passenger_entry_exit"
)

import_table() {
    local TABLE=$1
    sqoop import \
        --connect "${JDBC}" \
        --username "${PG_USER}" \
        --password "${PG_PASS}" \
        --table "${TABLE}" \
        --target-dir "${HDFS_BASE}/${TABLE}" \
        --delete-target-dir \
        --num-mappers 1 \
        --fields-terminated-by ',' \
        --lines-terminated-by '\n' \
        -m 1
}

for TABLE in "${TABLES[@]}"; do
    echo ">>> Importing ${TABLE} ..."
    ATTEMPT=0
    SUCCESS=false

    while [ $ATTEMPT -lt $MAX_RETRIES ]; do
        ATTEMPT=$((ATTEMPT + 1))
        echo "    Attempt ${ATTEMPT}/${MAX_RETRIES} ..."
        if import_table "${TABLE}"; then
            echo ">>> ${TABLE} imported successfully"
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
        echo ">>> ERROR: Failed to import ${TABLE} after ${MAX_RETRIES} attempts" >&2
        exit 1
    fi
done

echo "All ${#TABLES[@]} tables imported to ${HDFS_BASE}"
