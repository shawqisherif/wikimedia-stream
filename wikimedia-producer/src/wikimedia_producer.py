#! ../.venv/bin/python3.14
import asyncio
import logging
import sys
import time

import httpx
from confluent_kafka import Producer

topic = "wikimedia-recentchange"


conf: dict[str, str | int | float | bool] = {
    "bootstrap.servers": "localhost:9092",
    "acks": "all",
    "retries": "10000",
    "max.in.flight.requests.per.connection": "5",
    "enable.idempotence": "true",
    "compression.type": "snappy",
    "linger.ms": "20",
    "batch.size": f"{32 * 1024}",
}
producer = Producer(conf)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    filename="wiki_producer.log",
)
logger = logging.getLogger("WikiMediaProducer")


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"message delibery failed: {err}")
    else:
        logger.info(
            f"message delivered to {msg.topic()}, partition {msg.partition()}, offset {msg.offset()}"
        )


headers = {"User-Agent": "KafkaProducerBot/1.0"}


async def stream_from_external():
    url = "https://stream.wikimedia.org/v2/stream/recentchange"
    async with httpx.AsyncClient(headers=headers) as client:
        async with client.stream("GET", url) as response:
            async for chunk in response.aiter_bytes():
                if chunk is not None:
                    producer.produce(topic, value=chunk, callback=delivery_report)
                    producer.flush()
                else:
                    time.sleep(3)
                    continue


def websocket_endpoint():
    try:
        asyncio.run(stream_from_external())
    except httpx.ConnectError:
        # connection closed
        logger.info("Client disconnected")
    except KeyboardInterrupt as e:
        logger.info("User killed the producer")
        sys.exit()
    except Exception as e:
        # connection error
        logger.error(f"Connection error: {e}")


if __name__ == "__main__":
    websocket_endpoint()
