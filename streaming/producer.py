"""
producer.py
-----------
High-velocity Kafka producer for the "user-events" streaming demo.

Every message is printed to the terminal *before* it is sent, so you can
compare what goes into Kafka with what PySpark prints on the other side.

Shopping events (page_view, click, add_to_cart, remove_from_cart, purchase)
carry the product_id of a FakeStore product (1-20, the same ids as
silver_products in Databricks and lakehouse.products), so dashboards can join
events to products. login / logout have no product_id. Each user has a small
simulated cart: remove_from_cart and purchase take a product that user added
earlier when there is one.

A small share of messages is deliberately broken (null user_id, missing
field, malformed JSON) so the Spark job's validation step has something
to filter out.

Usage
-----
    python streaming/producer.py                  # 5 msg/s, runs until Ctrl+C
    python streaming/producer.py --rate 200       # high velocity
    python streaming/producer.py --count 50       # stop after 50 messages
    python streaming/producer.py --bad-ratio 0    # only valid messages
"""

from __future__ import annotations  # Python 3.9 on EMR Serverless

import argparse
import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# --------------------------------------------------
# Defaults
# --------------------------------------------------

BOOTSTRAP_SERVER = "localhost:9092"
TOPIC = "user-events"

USERS = [f"user_{n:03d}" for n in range(1, 21)]
EVENTS = ["page_view", "click", "add_to_cart", "remove_from_cart", "purchase", "login", "logout"]

# Events about a product carry a product_id; login / logout don't
PRODUCT_EVENTS = {"page_view", "click", "add_to_cart", "remove_from_cart", "purchase"}

# The FakeStore catalog (fakestoreapi.com/products): ids 1-20. Popularity is
# skewed (a few best sellers, a long tail) so per-product charts show a ranking;
# the fixed seed keeps the same products popular on every run.
PRODUCT_IDS = list(range(1, 21))
_ranked = random.Random(42).sample(PRODUCT_IDS, len(PRODUCT_IDS))
PRODUCT_WEIGHTS = [1 / (_ranked.index(pid) + 1) ** 0.8 for pid in PRODUCT_IDS]

# user_id -> product_ids in that user's cart (add_to_cart puts one in,
# remove_from_cart / purchase take one out)
_carts: dict[str, list[int]] = {}

# ANSI colours so producer lines stand out in the terminal
GREEN = "\033[92m"
YELLOW = "\033[93m"
RESET = "\033[0m"


# --------------------------------------------------
# Topic setup
# --------------------------------------------------


def ensure_topic(bootstrap: str, topic: str, partitions: int = 3) -> None:
    """Create the topic if it does not exist yet (no-op otherwise)."""

    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="user-events-admin")

    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=partitions, replication_factor=1)])
        print(f"Created topic '{topic}' with {partitions} partitions")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic}' already exists")
    finally:
        admin.close()


# --------------------------------------------------
# Event generation
# --------------------------------------------------


def now_iso() -> str:
    """UTC timestamp as an ISO-8601 string, e.g. 2026-09-23T10:15:30.123Z"""

    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def pick_product(user_id: str, event: str) -> int:
    """The product an event is about, keeping each user's cart consistent."""

    cart = _carts.setdefault(user_id, [])

    if event in ("remove_from_cart", "purchase") and cart:
        return cart.pop(random.randrange(len(cart)))

    product_id = random.choices(PRODUCT_IDS, weights=PRODUCT_WEIGHTS)[0]
    if event == "add_to_cart":
        cart.append(product_id)
    return product_id


def valid_event() -> dict:
    """A well-formed event matching the agreed JSON contract."""

    user_id = random.choice(USERS)
    event = random.choice(EVENTS)
    payload = {"user_id": user_id, "event": event, "timestamp": now_iso()}
    if event in PRODUCT_EVENTS:
        payload["product_id"] = pick_product(user_id, event)
    return payload


def invalid_event() -> str:
    """
    A deliberately bad payload, returned as a raw string so we can also
    send JSON that does not parse at all.
    """

    kind = random.choice(["null_user", "missing_event", "malformed"])

    if kind == "null_user":
        return json.dumps({"user_id": None, "event": random.choice(EVENTS), "timestamp": now_iso()})

    if kind == "missing_event":
        return json.dumps({"user_id": random.choice(USERS), "timestamp": now_iso()})

    # Truncated JSON -> from_json() returns null for every field
    return '{"user_id": "user_999", "event": "click", "timest'


# --------------------------------------------------
# Main loop
# --------------------------------------------------


def run(bootstrap: str, topic: str, rate: float, count: int | None, bad_ratio: float) -> None:

    ensure_topic(bootstrap, topic)

    # Keys and values are encoded to UTF-8 bytes by hand below (so malformed
    # JSON can be sent too). Keys = user_id, so one user's events always land
    # on the same partition and keep their order.
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        acks=1,
        linger_ms=5,  # small batching window for throughput
    )

    delay = 1.0 / rate if rate > 0 else 0
    sent = 0

    print(f"Producing to '{topic}' on {bootstrap} at ~{rate} msg/s (Ctrl+C to stop)\n")

    try:
        while count is None or sent < count:
            if random.random() < bad_ratio:
                payload = invalid_event()
                key = None
                colour, tag = YELLOW, "SEND (bad)"
            else:
                event = valid_event()
                payload = json.dumps(event)
                key = event["user_id"]
                colour, tag = GREEN, "SEND      "

            # 1) Print the JSON exactly as it will be written to Kafka
            print(f"{colour}[{tag}] #{sent + 1:<6} {payload}{RESET}", flush=True)

            # 2) Send it (asynchronous; delivery errors are printed via callback)
            producer.send(
                topic,
                key=key.encode("utf-8") if key else None,
                value=payload.encode("utf-8"),
            ).add_errback(lambda exc: print(f"[ERROR] delivery failed: {exc}", flush=True))

            sent += 1

            if delay:
                time.sleep(delay)

    except KeyboardInterrupt:
        print("\nStopping producer...")

    finally:
        producer.flush()
        producer.close()
        print(f"Producer closed. Total messages sent: {sent}")


def parse_args() -> argparse.Namespace:

    parser = argparse.ArgumentParser(description="Kafka producer for the user-events demo")
    parser.add_argument("--bootstrap", default=BOOTSTRAP_SERVER, help="Kafka bootstrap server")
    parser.add_argument("--topic", default=TOPIC, help="Kafka topic")
    parser.add_argument(
        "--rate", type=float, default=5, help="Messages per second (0 = as fast as possible)"
    )
    parser.add_argument(
        "--count", type=int, default=None, help="Stop after N messages (default: run forever)"
    )
    parser.add_argument(
        "--bad-ratio", type=float, default=0.1, help="Share of deliberately invalid messages"
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.bootstrap, args.topic, args.rate, args.count, args.bad_ratio)
