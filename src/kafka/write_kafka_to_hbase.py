#!/usr/bin/env python3
"""
TFL Kafka → HBase Consumer
Consumes TFL arrival messages from Kafka and writes them to HBase
in configurable batches using the HBase shell subprocess.
"""

import json
import re
import logging
import subprocess
from kafka import KafkaConsumer
from datetime import datetime

logging.basicConfig(
    filename='/tmp/hbase_consumer.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_BROKER = 'ip-172-31-6-42.eu-west-2.compute.internal:9092'
KAFKA_TOPIC  = 'tfl_arrivals'
HBASE_TABLE  = 'tfl_arrivals'
BATCH_SIZE   = 20


def sanitise(value):
    """Remove characters that break HBase shell put commands."""
    return re.sub(r"['\"\\\r\n\t]", '', str(value))


def build_row_key(record):
    station = sanitise(record.get('stationName', 'unknown'))
    vehicle = sanitise(record.get('vehicleId',   'unknown'))
    ts      = sanitise(record.get('timestamp',   datetime.utcnow().isoformat()))
    return f"{station}_{vehicle}_{ts}"


def write_batch_to_hbase(batch):
    commands = []
    for record in batch:
        row_key = build_row_key(record)
        fields = {
            'cf:stationName':     record.get('stationName', ''),
            'cf:vehicleId':       record.get('vehicleId', ''),
            'cf:lineName':        record.get('lineName', ''),
            'cf:platformName':    record.get('platformName', ''),
            'cf:expectedArrival': record.get('expectedArrival', ''),
            'cf:timeToStation':   record.get('timeToStation', ''),
        }
        for col, val in fields.items():
            commands.append(
                f"put '{HBASE_TABLE}', '{row_key}', '{col}', '{sanitise(val)}'"
            )

    hbase_input = '\n'.join(commands) + '\nexit\n'
    try:
        result = subprocess.run(
            ['hbase', 'shell', '-n'],
            input=hbase_input,
            capture_output=True,
            text=True,
            timeout=60
        )
        if result.returncode != 0:
            logger.warning("HBase shell stderr: %s", result.stderr[:300])
        else:
            logger.info("Wrote batch of %d records to HBase", len(batch))
    except subprocess.TimeoutExpired:
        logger.error("HBase shell timed out for batch of %d", len(batch))
    except Exception as exc:
        logger.error("HBase write failed: %s", exc)


def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='gokul_tfl_hbase_consumer',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    logger.info("HBase consumer started. Topic: %s → HBase: %s", KAFKA_TOPIC, HBASE_TABLE)
    print(f"Consuming from {KAFKA_TOPIC} → writing to HBase:{HBASE_TABLE}")

    batch = []
    total = 0

    try:
        for msg in consumer:
            batch.append(msg.value)
            if len(batch) >= BATCH_SIZE:
                write_batch_to_hbase(batch)
                total += len(batch)
                print(f"[{datetime.utcnow().isoformat()}] Written {total} records to HBase")
                batch = []
    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")
    finally:
        if batch:
            write_batch_to_hbase(batch)
            total += len(batch)
        consumer.close()
        logger.info("Consumer finished. Total records written: %d", total)
        print(f"HBase consumer done. Total: {total}")


if __name__ == '__main__':
    main()
