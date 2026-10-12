import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError


KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = os.getenv("KAFKA_TOPIC", "traffic-events")
PRODUCE_INTERVAL = float(os.getenv("TRAFFIC_PRODUCE_INTERVAL_SECONDS", "1.0"))
MAX_MESSAGES = os.getenv("TRAFFIC_MAX_MESSAGES")
MAX_MESSAGES = int(MAX_MESSAGES) if MAX_MESSAGES else None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("traffic_producer")


ROAD_SEGMENTS = [
    {
        "road_id": "ROAD_001",
        "road_name": "Central Avenue",
        "latitude": 16.5062,
        "longitude": 80.6480,
        "free_flow_speed": 60,
    },
    {
        "road_id": "ROAD_002",
        "road_name": "Market Road",
        "latitude": 16.5100,
        "longitude": 80.6300,
        "free_flow_speed": 40,
    },
    {
        "road_id": "ROAD_003",
        "road_name": "Airport Road",
        "latitude": 16.5300,
        "longitude": 80.8000,
        "free_flow_speed": 80,
    },
    {
        "road_id": "ROAD_004",
        "road_name": "River Bridge",
        "latitude": 16.5200,
        "longitude": 80.6100,
        "free_flow_speed": 50,
    },
    {
        "road_id": "ROAD_005",
        "road_name": "Highway Junction",
        "latitude": 16.4700,
        "longitude": 80.7000,
        "free_flow_speed": 70,
    },
]


def create_event():
    road = random.choice(ROAD_SEGMENTS)

    # Simulate occasional heavy traffic
    if random.random() < 0.30:
        speed = random.uniform(5, 20)
        vehicle_count = random.randint(40, 100)
    else:
        speed = random.uniform(25, road["free_flow_speed"])
        vehicle_count = random.randint(5, 50)

    # Simulate occasional accident
    accident_reported = random.random() < 0.03

    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "road_id": road["road_id"],
        "road_name": road["road_name"],
        "latitude": road["latitude"],
        "longitude": road["longitude"],
        "average_speed": round(speed, 2),
        "free_flow_speed": float(road["free_flow_speed"]),
        "vehicle_count": vehicle_count,
        "accident_reported": accident_reported,
    }


def serialize_event(event):
    """JSON-encode a single traffic event (used by the producer and tests)."""
    return json.dumps(event).encode("utf-8")


def validate_event(event):
    """Check that an event has all required fields with usable types."""
    required = {
        "event_id": str,
        "timestamp": str,
        "road_id": str,
        "road_name": str,
        "latitude": (int, float),
        "longitude": (int, float),
        "average_speed": (int, float),
        "free_flow_speed": (int, float),
        "vehicle_count": int,
        "accident_reported": bool,
    }
    missing = [k for k in required if k not in event]
    if missing:
        raise ValueError(f"Missing event fields: {missing}")
    for key, types in required.items():
        if not isinstance(event[key], types):
            raise TypeError(
                f"Field {key!r} must be {types}, got {type(event[key]).__name__}"
            )
    return True


def main():

    producer = None

    try:

        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda value: serialize_event(value),
            acks="all",
            retries=5,
            retry_backoff_ms=500,
        )

        log.info("Connected to Kafka at %s", KAFKA_SERVER)
        log.info("Streaming events to topic: %s", TOPIC)
        print("Press Ctrl+C to stop.\n")

        count = 0

        while True:
            if MAX_MESSAGES is not None and count >= MAX_MESSAGES:
                log.info("Reached TRAFFIC_MAX_MESSAGES=%s, stopping.", MAX_MESSAGES)
                break

            event = create_event()
            validate_event(event)

            try:

                producer.send(
                    TOPIC,
                    value=event
                ).get(timeout=10)

                count += 1

                print(
                    f"[{count}] "
                    f"{event['road_name']:<20} "
                    f"Speed: {event['average_speed']:>5.1f} km/h | "
                    f"Vehicles: {event['vehicle_count']:>3} | "
                    f"Accident: {event['accident_reported']}"
                )

            except KafkaError as exc:
                log.error("Kafka send failed: %s", exc)

            time.sleep(PRODUCE_INTERVAL)

    except KeyboardInterrupt:

        print("\nStopping traffic producer...")

    finally:

        if producer is not None:
            producer.flush()
            producer.close()

        print("Producer stopped.")


if __name__ == "__main__":
    main()