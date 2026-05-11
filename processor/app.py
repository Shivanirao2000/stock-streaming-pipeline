import asyncio
import json
import asyncpg
from aiokafka import AIOKafkaConsumer
from collections import defaultdict

TICKERS = ["AAPL", "GOOGL", "MSFT", "TSLA", "NVDA"]
price_history = defaultdict(list)

async def get_db():
    return await asyncpg.connect(
        host="localhost", port=5432,
        database="stocks", user="admin", password="secret"
    )

async def main():
    db = await get_db()
    print("✅ Connected to PostgreSQL")

    consumer = AIOKafkaConsumer(
        "stock-prices",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest"
    )
    await consumer.start()
    print("✅ Connected to Kafka, consuming...")

    try:
        async for msg in consumer:
            data = msg.value
            ticker = data["ticker"]
            price = data["price"]

            # Rolling average over last 5 prices
            price_history[ticker].append(price)
            if len(price_history[ticker]) > 5:
                price_history[ticker].pop(0)
            rolling_avg = round(sum(price_history[ticker]) / len(price_history[ticker]), 2)

            await db.execute(
                "INSERT INTO stock_prices (ticker, price, rolling_avg) VALUES ($1, $2, $3)",
                ticker, price, rolling_avg
            )
            print(f"💾 {ticker} | price={price} | avg={rolling_avg}")
    finally:
        await consumer.stop()
        await db.close()

if __name__ == "__main__":
    asyncio.run(main())
