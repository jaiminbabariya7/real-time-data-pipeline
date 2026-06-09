"""
Kafka event producer — simulates a high-throughput web application.

Generates realistic user-activity events (page_view, add_to_cart,
purchase, search, login, logout) and publishes them to the Kafka
raw-events topic at a configurable rate.

Usage:
    python producer.py --rate 100 --duration 60
"""
from __future__ import annotations
import argparse, logging, os, random, time
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from schemas import EventType, UserEvent, KAFKA_TOPICS

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("kafka_producer")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC     = KAFKA_TOPICS["raw_events"]

USER_IDS  = [f"usr_{i:05d}" for i in range(1, 10_001)]
PRODUCTS  = ["laptop_pro","wireless_earbuds","standing_desk","mechanical_keyboard",
             "usb_hub","monitor_4k","webcam_hd","mouse_ergonomic","chair_gaming","led_strips"]
PAGES     = ["/home","/category/electronics","/product/","/cart","/checkout","/search"]
SEARCHES  = ["laptop","headphones","keyboard","monitor","desk setup","gaming chair"]
COUNTRIES = ["US","IN","GB","DE","CA","AU","SG","FR","BR","JP"]
PLATFORMS = ["web","ios","android"]
WEIGHTS   = [0.45, 0.20, 0.08, 0.15, 0.07, 0.05]


def _props(et: EventType) -> dict:
    if et == EventType.PAGE_VIEW:
        return {"page": random.choice(PAGES), "load_time_ms": random.randint(80,2500),
                "referrer": random.choice(["google","direct","email",""])}
    if et == EventType.ADD_TO_CART:
        return {"product_id": random.choice(PRODUCTS), "quantity": random.randint(1,4),
                "price_usd": round(random.uniform(9.99,1299.99), 2)}
    if et == EventType.PURCHASE:
        return {"order_id": f"ord_{random.randint(100000,999999)}",
                "items": random.randint(1,5),
                "total_usd": round(random.uniform(19.99,3999.99), 2),
                "payment_method": random.choice(["card","paypal","upi"])}
    if et == EventType.SEARCH:
        return {"query": random.choice(SEARCHES), "results_count": random.randint(0,200)}
    return {"method": random.choice(["email","google","apple"])}


def _on_delivery(err, msg):
    if err: logger.error("Delivery failed: %s", err)


def ensure_topic(admin: AdminClient) -> None:
    existing = admin.list_topics(timeout=5).topics
    if TOPIC not in existing:
        admin.create_topics([NewTopic(TOPIC, num_partitions=6, replication_factor=1)])
        logger.info("Created topic: %s", TOPIC)


def run(rate: int = 50, duration: int | None = None) -> None:
    """Produce events to Kafka at `rate` events/second."""
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 5,
                          "batch.num.messages": 100, "compression.type": "lz4"})
    ensure_topic(AdminClient({"bootstrap.servers": BOOTSTRAP}))

    types  = list(EventType)
    start  = time.time()
    sent   = 0
    logger.info("Producing %d events/sec to %s ...", rate, TOPIC)
    try:
        while duration is None or (time.time()-start) < duration:
            et    = random.choices(types, weights=WEIGHTS)[0]
            event = UserEvent.create(et, random.choice(USER_IDS),
                        f"sess_{random.randint(10**9,10**10)}", _props(et))
            event.country  = random.choice(COUNTRIES)
            event.platform = random.choice(PLATFORMS)
            producer.produce(TOPIC, key=event.user_id.encode(),
                             value=event.to_json(), callback=_on_delivery)
            sent += 1
            if sent % 1000 == 0:
                producer.poll(0)
                logger.info("Produced %d events (%.1f/s)", sent, sent/(time.time()-start))
            time.sleep(1/rate)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        logger.info("Finished. Total: %d events in %.1fs", sent, time.time()-start)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--rate",     type=int, default=50)
    p.add_argument("--duration", type=int, default=None)
    a = p.parse_args()
    run(a.rate, a.duration)
