"""
Publisher - Event Simulator
Mengirim event ke aggregator dengan configurable duplicate rate.
Mendukung pengiriman 20.000+ event dengan 30% duplikat.
"""

import asyncio
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone
from typing import List

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("publisher")

# Config dari environment
TARGET_URL = os.getenv("TARGET_URL", "http://aggregator:8080")
EVENT_COUNT = int(os.getenv("EVENT_COUNT", "20000"))
DUPLICATE_RATE = float(os.getenv("DUPLICATE_RATE", "0.30"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "10"))
TOPICS = os.getenv("TOPICS", "sensor-data,app-logs,security-logs,system-events").split(",")
SLEEP_BETWEEN_BATCHES = float(os.getenv("SLEEP_BETWEEN_BATCHES", "0.05"))


def generate_event(topic: str = None) -> dict:
    """Generate a single unique event."""
    topic = topic or random.choice(TOPICS)
    sources = ["iot-device", "web-server", "mobile-app", "backend-service", "edge-node"]
    return {
        "topic": topic,
        "event_id": f"evt-{uuid.uuid4()}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": f"{random.choice(sources)}-{random.randint(1, 50):02d}",
        "payload": {
            "temperature": round(random.uniform(20.0, 45.0), 2),
            "humidity": round(random.uniform(30.0, 90.0), 2),
            "pressure": round(random.uniform(1000.0, 1020.0), 2),
            "level": random.choice(["INFO", "WARN", "ERROR", "DEBUG"]),
            "sequence": random.randint(1, 1_000_000),
        },
    }


def prepare_event_pool(count: int, duplicate_rate: float) -> List[dict]:
    """
    Buat pool event dengan sejumlah duplikat.
    Misalnya: 20.000 event, 30% duplikat → 14.000 unik + 6.000 duplikat.
    """
    unique_count = int(count * (1 - duplicate_rate))
    duplicate_count = count - unique_count

    logger.info(f"Preparing {count} events: {unique_count} unique + {duplicate_count} duplicates ({duplicate_rate*100:.0f}%)")

    unique_events = [generate_event() for _ in range(unique_count)]
    duplicates = [random.choice(unique_events).copy() for _ in range(duplicate_count)]

    all_events = unique_events + duplicates
    random.shuffle(all_events)  # Shuffle agar duplikat tersebar merata

    return all_events


async def send_batch(client: httpx.AsyncClient, batch: List[dict], semaphore: asyncio.Semaphore):
    """Send batch of events to aggregator."""
    async with semaphore:
        try:
            response = await client.post(
                f"{TARGET_URL}/publish/batch",
                json={"events": batch},
                timeout=30.0,
            )
            if response.status_code == 200:
                data = response.json()
                return data.get("queued", 0), data.get("failed", 0)
            else:
                logger.error(f"Batch failed: HTTP {response.status_code} - {response.text[:200]}")
                return 0, len(batch)
        except httpx.TimeoutException:
            logger.error(f"Batch timeout - {len(batch)} events lost")
            return 0, len(batch)
        except Exception as e:
            logger.error(f"Batch error: {e}")
            return 0, len(batch)


async def run_publisher():
    """Main publisher loop."""
    logger.info("=" * 60)
    logger.info(f"Publisher starting")
    logger.info(f"  Target:        {TARGET_URL}")
    logger.info(f"  Event count:   {EVENT_COUNT:,}")
    logger.info(f"  Duplicate rate: {DUPLICATE_RATE * 100:.0f}%")
    logger.info(f"  Batch size:    {BATCH_SIZE}")
    logger.info(f"  Concurrency:   {CONCURRENCY}")
    logger.info("=" * 60)

    # Wait for aggregator to be ready
    logger.info("Waiting for aggregator to be ready...")
    async with httpx.AsyncClient() as client:
        for attempt in range(30):
            try:
                resp = await client.get(f"{TARGET_URL}/health", timeout=5.0)
                if resp.status_code == 200:
                    logger.info("Aggregator is ready!")
                    break
            except Exception:
                pass
            logger.info(f"  Retry {attempt + 1}/30 ...")
            await asyncio.sleep(3)
        else:
            logger.error("Aggregator not ready after 90s, exiting")
            return

    # Prepare events
    events = prepare_event_pool(EVENT_COUNT, DUPLICATE_RATE)
    batches = [events[i:i + BATCH_SIZE] for i in range(0, len(events), BATCH_SIZE)]
    total_batches = len(batches)

    logger.info(f"Starting to send {total_batches} batches of ~{BATCH_SIZE} events each")

    semaphore = asyncio.Semaphore(CONCURRENCY)
    total_queued = 0
    total_failed = 0
    start_time = time.time()

    async with httpx.AsyncClient() as client:
        tasks = []
        for i, batch in enumerate(batches):
            task = asyncio.create_task(send_batch(client, batch, semaphore))
            tasks.append(task)

            if (i + 1) % 20 == 0:
                elapsed = time.time() - start_time
                logger.info(
                    f"Progress: {i+1}/{total_batches} batches "
                    f"| {(i+1)*BATCH_SIZE:,}/{EVENT_COUNT:,} events "
                    f"| {elapsed:.1f}s elapsed"
                )

            if SLEEP_BETWEEN_BATCHES > 0:
                await asyncio.sleep(SLEEP_BETWEEN_BATCHES)

        results = await asyncio.gather(*tasks)
        for queued, failed in results:
            total_queued += queued
            total_failed += failed

    elapsed = time.time() - start_time
    throughput = total_queued / elapsed if elapsed > 0 else 0

    logger.info("=" * 60)
    logger.info("Publisher DONE")
    logger.info(f"  Total events:   {EVENT_COUNT:,}")
    logger.info(f"  Queued:         {total_queued:,}")
    logger.info(f"  Failed:         {total_failed:,}")
    logger.info(f"  Elapsed:        {elapsed:.2f}s")
    logger.info(f"  Throughput:     {throughput:.0f} events/s")
    logger.info("=" * 60)

    # Fetch stats
    await asyncio.sleep(5)  # wait for workers to process
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(f"{TARGET_URL}/stats", timeout=10.0)
            stats = resp.json()
            logger.info("Final stats from aggregator:")
            logger.info(f"  Received:          {stats.get('received', 'N/A'):,}")
            logger.info(f"  Unique processed:  {stats.get('unique_processed', 'N/A'):,}")
            logger.info(f"  Duplicate dropped: {stats.get('duplicate_dropped', 'N/A'):,}")
        except Exception as e:
            logger.error(f"Could not fetch stats: {e}")


if __name__ == "__main__":
    asyncio.run(run_publisher())
