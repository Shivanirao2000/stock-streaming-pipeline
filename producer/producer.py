import json
import time
import random
import os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from datetime import datetime, timezone

TICKERS = ["AAPL", "GOOGL", "MSFT", "TSLA", "NVDA"]

BASE_PRICES = {
    "AAPL": 175.0,
    "GOOGL": 140.0,
    "MSFT": 380.0,
    "TSLA": 250.0,
    "NVDA": 800.0,
}

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "stock-prices"


def make_producer() -> KafkaProducer:
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
            )
            print(f"Connected to Kafka at {BOOTSTRAP_SERVERS}")
            return producer
        except NoBrokersAvailable:
            print(f"Kafka not ready at {BOOTSTRAP_SERVERS}, retrying in 5s…")
            time.sleep(5)


def next_price(current: float) -> float:
    # Gaussian random walk with slight mean-reversion
    change = random.gauss(0, current * 0.005)
    return round(max(0.01, current + change), 2)


def main() -> None:
    producer = make_producer()
    prices = dict(BASE_PRICES)

    print(f"Streaming prices for {TICKERS} → topic '{TOPIC}'")
    while True:
        for ticker in TICKERS:
            prices[ticker] = next_price(prices[ticker])
            message = {
                "ticker": ticker,
                "price": prices[ticker],
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            producer.send(TOPIC, value=message)
            print(f"  {ticker}: ${prices[ticker]:.2f}")

        producer.flush()
        time.sleep(1)


if __name__ == "__main__":
    main()
