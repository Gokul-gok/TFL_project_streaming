#!/usr/bin/env python3
"""
TFL Kafka Producer
Fetches live train arrival predictions from the TFL Unified API
for 5 London Underground lines and publishes them to a Kafka topic.
Runs for 1 hour (3600 s), polling every 30 seconds.
"""

import json
import time
import logging
import requests
from kafka import KafkaProducer
from datetime import datetime

logging.basicConfig(
    filename='/tmp/producer_success.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_BROKER  = 'ip-172-31-6-42.eu-west-2.compute.internal:9092'
KAFKA_TOPIC   = 'tfl_arrivals'
TFL_API_BASE  = 'https://api.tfl.gov.uk/Line/{line}/Arrivals'
LINES         = ['victoria', 'central', 'jubilee', 'northern', 'piccadilly']
POLL_INTERVAL = 30    # seconds between API polls
RUN_SECS      = 3600  # total runtime


def create_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=3,
        acks='all'
    )


def fetch_arrivals(line):
    url = TFL_API_BASE.format(line=line)
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        logger.error("Failed to fetch %s: %s", line, exc)
        return []


def main():
    producer   = create_producer()
    total_sent = 0
    start_time = time.time()
    batch_num  = 0

    logger.info("Kafka Producer started. Topic: %s", KAFKA_TOPIC)

    while time.time() - start_time < RUN_SECS:
        batch_num   += 1
        batch_count  = 0

        for line in LINES:
            arrivals = fetch_arrivals(line)
            for arrival in arrivals:
                record = {
                    'id':              arrival.get('id', ''),
                    'vehicleId':       arrival.get('vehicleId', ''),
                    'stationName':     arrival.get('stationName', ''),
                    'lineName':        arrival.get('lineName', ''),
                    'platformName':    arrival.get('platformName', ''),
                    'expectedArrival': arrival.get('expectedArrival', ''),
                    'timeToStation':   arrival.get('timeToStation', 0),
                    'currentLocation': arrival.get('currentLocation', ''),
                    'direction':       arrival.get('direction', ''),
                    'destinationName': arrival.get('destinationName', ''),
                    'timestamp':       datetime.utcnow().isoformat(),
                }
                for attempt in range(3):
                    try:
                        producer.send(KAFKA_TOPIC, value=record)
                        batch_count += 1
                        break
                    except Exception as exc:
                        logger.warning("Retry %d sending record: %s", attempt + 1, exc)

        producer.flush()
        total_sent += batch_count
        logger.info("Batch %d: sent %d messages (total: %d)", batch_num, batch_count, total_sent)
        print(f"[{datetime.utcnow().isoformat()}] Batch {batch_num}: "
              f"{batch_count} msgs | Total: {total_sent}")
        time.sleep(POLL_INTERVAL)

    producer.close()
    logger.info("Producer finished. Total messages sent: %d", total_sent)
    print(f"Producer completed. Total messages sent: {total_sent}")


if __name__ == '__main__':
    main()
