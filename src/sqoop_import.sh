#!/bin/bash
# Sqoop import: PostgreSQL → HDFS (6 tables for TFL star schema)

PG_HOST="13.42.152.118"
PG_PORT="5432"
PG_DB="testdb"
PG_USER="admin"
PG_PASS="admin123"
HDFS_BASE="/tmp/gokul/tfl_project1"
JDBC="jdbc:postgresql://${PG_HOST}:${PG_PORT}/${PG_DB}"

TABLES=(
    "dim_date"
    "dim_stations"
    "dim_networks"
    "dim_lines"
    "fact_station_lines"
    "fact_passenger_entry_exit"
)

for TABLE in "${TABLES[@]}"; do
    echo ">>> Importing ${TABLE} ..."
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

    if [ $? -eq 0 ]; then
        echo ">>> ${TABLE} imported successfully"
    else
        echo ">>> ERROR: Failed to import ${TABLE}" >&2
        exit 1
    fi
done

echo "All ${#TABLES[@]} tables imported to ${HDFS_BASE}"
