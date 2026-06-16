pipeline {
    agent any

    environment {
        // ── Remote EC2 / Cloudera cluster ────────────────────────────────────
        REMOTE_HOST    = '13.41.167.97'
        REMOTE_USER    = 'ec2-user'
        REMOTE_DIR     = '/home/ec2-user/gokul_tfl'
        PEM_KEY        = '/var/lib/jenkins/.ssh/gokul_key.pem'

        // ── PostgreSQL (source) ──────────────────────────────────────────────
        PG_HOST        = '13.42.152.118'
        PG_PORT        = '5432'
        PG_DB          = 'testdb'
        PG_USER        = 'admin'
        PG_PASS        = 'admin123'

        // ── HDFS paths ───────────────────────────────────────────────────────
        HDFS_STAGING   = '/tmp/gokul_batch/tfl_project1'
        HDFS_GOLD      = '/tmp/gokul_batch/tfl_project1/gold'
        HDFS_FULL_LOAD = '/tmp/gokul_batch/tfl_full_load/output'

        // ── Hive ─────────────────────────────────────────────────────────────
        HIVE_DB        = 'gokul_tfl_proj'

        // ── Kafka ────────────────────────────────────────────────────────────
        KAFKA_BROKER   = 'ip-172-31-6-42.eu-west-2.compute.internal:9092'
        KAFKA_TOPIC    = 'tfl_arrivals'
    }

    stages {

        // ── 1. Checkout source code ──────────────────────────────────────────
        stage('Checkout') {
            steps {
                git branch: 'main',
                    url: 'https://github.com/Gokul-gok/TFL_project_streaming.git'
            }
        }

        // ── 2. Create remote working directories ─────────────────────────────
        stage('Prepare Remote Directory') {
            steps {
                sh """
                    ssh -i "${PEM_KEY}" -o StrictHostKeyChecking=no ${REMOTE_USER}@${REMOTE_HOST} '
                        mkdir -p ${REMOTE_DIR}/sqoop
                        mkdir -p ${REMOTE_DIR}/hive
                        mkdir -p ${REMOTE_DIR}/spark
                    '
                """
            }
        }

        // ── 3. Copy scripts to Cloudera ──────────────────────────────────────
        stage('Copy Scripts to Cloudera') {
            steps {
                sh """
                    scp -i "${PEM_KEY}" -o StrictHostKeyChecking=no \
                        src/sqoop_import.sh \
                        src/sqoop_incremental.sh \
                        src/save_watermark.sh \
                        ${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_DIR}/sqoop/

                    scp -i "${PEM_KEY}" -o StrictHostKeyChecking=no \
                        src/hive_table.sql \
                        ${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_DIR}/hive/

                    scp -i "${PEM_KEY}" -o StrictHostKeyChecking=no \
                        src/spark/spark_gold_layer.py \
                        src/spark/spark_full_load_tfl.py \
                        src/spark/spark_incremental_load.py \
                        ${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_DIR}/spark/

                    scp -i "${PEM_KEY}" -o StrictHostKeyChecking=no \
                        src/spark/hive_full_load_table.sql \
                        ${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_DIR}/hive/
                """
            }
        }

        // ── 4. Make scripts executable ───────────────────────────────────────
        stage('Set Permissions') {
            steps {
                sh """
                    ssh -i "${PEM_KEY}" -o StrictHostKeyChecking=no ${REMOTE_USER}@${REMOTE_HOST} '
                        chmod +x ${REMOTE_DIR}/sqoop/*.sh
                        chmod +x ${REMOTE_DIR}/spark/*.py
                    '
                """
            }
        }

        // ── 5. Prepare HDFS staging directories ─────────────────────────────
        stage('Prepare Staging Directory') {
            steps {
                sh """
                    ssh -i "${PEM_KEY}" -o StrictHostKeyChecking=no ${REMOTE_USER}@${REMOTE_HOST} '
                        hdfs dfs -mkdir -p ${HDFS_STAGING}
                        hdfs dfs -mkdir -p ${HDFS_GOLD}
                        hdfs dfs -mkdir -p ${HDFS_FULL_LOAD}
                        hdfs dfs -chmod -R 777 /tmp/gokul_batch || true
                    '
                """
            }
        }

        // ── 6. Clean HDFS staging data ───────────────────────────────────────
        stage('Clean HDFS') {
            steps {
                sh """
                    ssh -i "${PEM_KEY}" -o StrictHostKeyChecking=no ${REMOTE_USER}@${REMOTE_HOST} '
                        for TABLE in dim_date dim_stations dim_networks dim_lines fact_station_lines fact_passenger_entry_exit; do
                            hdfs dfs -rm -r -f ${HDFS_STAGING}/\${TABLE} || true
                        done
                        hdfs dfs -rm -r -f ${HDFS_GOLD}       || true
                        hdfs dfs -rm -r -f ${HDFS_FULL_LOAD}  || true

                        hdfs dfs -mkdir -p ${HDFS_GOLD}
                        hdfs dfs -mkdir -p ${HDFS_FULL_LOAD}
                        hdfs dfs -chmod -R 777 /tmp/gokul_batch || true
                        echo "HDFS cleaned and ready"
                    '
                """
            }
        }

        // ── 7. Sqoop import (6 tables from PostgreSQL → HDFS) ─────────────────
        stage('Run Sqoop Import') {
            steps {
                sh """
                    ssh -i "${PEM_KEY}" -o StrictHostKeyChecking=no ${REMOTE_USER}@${REMOTE_HOST} '
                        bash ${REMOTE_DIR}/sqoop/sqoop_import.sh
                    '
                """
            }
        }

        // ── 8. Create Hive external tables (star schema) ─────────────────────
        stage('Create Hive Tables') {
            steps {
                sh """
                    ssh -i "${PEM_KEY}" \
                        -o StrictHostKeyChecking=no \
                        -o ServerAliveInterval=10 \
                        -o ServerAliveCountMax=6 \
                        ${REMOTE_USER}@${REMOTE_HOST} '
                        beeline -u "jdbc:hive2://localhost:10000" \
                                -f ${REMOTE_DIR}/hive/hive_table.sql \
                                2>/dev/null || true
                    '
                """
            }
        }

        // ── 9. Run Spark Gold Layer ───────────────────────────────────────────
        stage('Run Spark Gold Layer') {
            steps {
                sh """
                    ssh -i "${PEM_KEY}" \
                        -o StrictHostKeyChecking=no \
                        -o ServerAliveInterval=10 \
                        -o ServerAliveCountMax=30 \
                        ${REMOTE_USER}@${REMOTE_HOST} '
                        spark-submit \
                            --master local[*] \
                            --conf spark.sql.parquet.writeLegacyFormat=true \
                            --conf spark.sql.shuffle.partitions=4 \
                            ${REMOTE_DIR}/spark/spark_gold_layer.py
                    '
                """
            }
        }

        // ── 10. Run Spark Full Load (Kafka → HDFS Parquet) ────────────────────
        stage('Run Spark Full Load') {
            steps {
                sh """
                    ssh -i "${PEM_KEY}" \
                        -o StrictHostKeyChecking=no \
                        -o ServerAliveInterval=10 \
                        -o ServerAliveCountMax=30 \
                        ${REMOTE_USER}@${REMOTE_HOST} '
                        spark-submit \
                            --master local[*] \
                            --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7 \
                            --conf spark.sql.parquet.writeLegacyFormat=true \
                            --conf spark.sql.shuffle.partitions=4 \
                            ${REMOTE_DIR}/spark/spark_full_load_tfl.py
                    '
                """
            }
        }

        // ── 11. Create Hive table over full-load output ───────────────────────
        stage('Create Hive Full Load Table') {
            steps {
                sh """
                    ssh -i "${PEM_KEY}" \
                        -o StrictHostKeyChecking=no \
                        -o ServerAliveInterval=10 \
                        -o ServerAliveCountMax=6 \
                        ${REMOTE_USER}@${REMOTE_HOST} '
                        beeline -u "jdbc:hive2://localhost:10000" \
                                -f ${REMOTE_DIR}/hive/hive_full_load_table.sql \
                                2>/dev/null || true
                    '
                """
            }
        }

        // ── 12. Verify results ────────────────────────────────────────────────
        stage('Verify Results') {
            steps {
                sh """
                    ssh -i "${PEM_KEY}" -o StrictHostKeyChecking=no ${REMOTE_USER}@${REMOTE_HOST} '
                        echo "=== Staging data ==="
                        hdfs dfs -ls ${HDFS_STAGING}

                        echo "=== Gold layer tables ==="
                        hdfs dfs -ls ${HDFS_GOLD}

                        echo "=== Full load output ==="
                        hdfs dfs -ls ${HDFS_FULL_LOAD}

                        echo "=== Hive table counts ==="
                        beeline -u "jdbc:hive2://localhost:10000" \
                            -e "SELECT COUNT(*) FROM ${HIVE_DB}.tfl_full_load;" \
                            2>/dev/null | tail -5 || true
                    '
                """
            }
        }

        // ── 13. Save watermarks for next incremental run ──────────────────────
        stage('Save Watermark') {
            steps {
                sh """
                    ssh -i "${PEM_KEY}" \
                        -o StrictHostKeyChecking=no \
                        -o ServerAliveInterval=10 \
                        -o ServerAliveCountMax=6 \
                        ${REMOTE_USER}@${REMOTE_HOST} '
                        bash ${REMOTE_DIR}/sqoop/save_watermark.sh
                    '
                """
            }
        }

        // ── 14. Incremental Sqoop (new fact rows only) ────────────────────────
        stage('Run Incremental Sqoop') {
            steps {
                sh """
                    ssh -i "${PEM_KEY}" \
                        -o StrictHostKeyChecking=no \
                        -o ServerAliveInterval=10 \
                        -o ServerAliveCountMax=12 \
                        ${REMOTE_USER}@${REMOTE_HOST} '
                        bash ${REMOTE_DIR}/sqoop/sqoop_incremental.sh
                    '
                """
            }
        }

        // ── 15. Incremental Spark load (new Kafka messages only) ──────────────
        stage('Run Spark Incremental Load') {
            steps {
                sh """
                    ssh -i "${PEM_KEY}" \
                        -o StrictHostKeyChecking=no \
                        -o ServerAliveInterval=10 \
                        -o ServerAliveCountMax=30 \
                        ${REMOTE_USER}@${REMOTE_HOST} '
                        spark-submit \
                            --master local[*] \
                            --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7 \
                            --conf spark.sql.parquet.writeLegacyFormat=true \
                            --conf spark.sql.shuffle.partitions=4 \
                            ${REMOTE_DIR}/spark/spark_incremental_load.py
                    '
                """
            }
        }

        // ── 16. Verify incremental results ────────────────────────────────────
        stage('Verify Incremental Results') {
            steps {
                sh """
                    ssh -i "${PEM_KEY}" -o StrictHostKeyChecking=no ${REMOTE_USER}@${REMOTE_HOST} '
                        echo "=== Incremental Sqoop output ==="
                        hdfs dfs -ls /tmp/gokul_batch/tfl_incremental/fact_passenger_entry_exit || echo "No incremental Sqoop data"
                        SQOOP_COUNT=\$(hdfs dfs -cat /tmp/gokul_batch/tfl_incremental/fact_passenger_entry_exit/part-* 2>/dev/null | wc -l)
                        echo "New fact rows: \${SQOOP_COUNT}"

                        echo "=== Incremental Spark output ==="
                        hdfs dfs -ls /tmp/gokul_batch/tfl_incremental/kafka_output || echo "No incremental Spark data"

                        echo "=== Current watermarks ==="
                        echo -n "last_entry_exit_id: "
                        hdfs dfs -cat /tmp/gokul_batch/watermark/last_entry_exit_id 2>/dev/null || echo "not set"
                        echo -n "kafka_offsets: "
                        hdfs dfs -cat /tmp/gokul_batch/watermark/kafka_offsets.json 2>/dev/null || echo "not set"
                    '
                """
            }
        }
    }

    post {
        success {
            echo """
            ╔══════════════════════════════════════════════════════╗
            ║         TFL Batch Pipeline  ─  SUCCESS               ║
            ╠══════════════════════════════════════════════════════╣
            ║  Cluster        : ${REMOTE_HOST}                     ║
            ║  HDFS Staging   : ${HDFS_STAGING}                    ║
            ║  HDFS Gold      : ${HDFS_GOLD}                       ║
            ║  HDFS Full Load : ${HDFS_FULL_LOAD}                  ║
            ║  Hive DB        : ${HIVE_DB}                         ║
            ╚══════════════════════════════════════════════════════╝
            """
        }
        failure {
            echo """
            ╔══════════════════════════════════════════════════════╗
            ║         TFL Batch Pipeline  ─  FAILED                ║
            ╠══════════════════════════════════════════════════════╣
            ║  Check logs on the Cloudera host: ${REMOTE_HOST}     ║
            ╚══════════════════════════════════════════════════════╝
            """
        }
    }
}
