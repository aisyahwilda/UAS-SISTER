"""
Pub-Sub Log Aggregator - Main Application
Sistem Paralel dan Terdistribusi - E2526
"""

import asyncio
import logging
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import redis.asyncio as aioredis
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from config import settings
from database import Database
from models import (
    BatchPublishRequest,
    BatchPublishResponse,
    EventModel,
    EventResponse,
    EventsListResponse,
    HealthResponse,
    PublishRequest,
    PublishResponse,
    StatsResponse,
)

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("aggregator")

# Global state
START_TIME = time.time()
db: Database = None
redis_client: aioredis.Redis = None
worker_tasks: List[asyncio.Task] = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle - startup and shutdown."""
    global db, redis_client, worker_tasks

    logger.info("=== Aggregator starting up ===")

    # Initialize database
    db = Database(settings.DATABASE_URL)
    await db.init()
    logger.info("Database initialized and schema applied")

    # Initialize Redis
    redis_client = aioredis.from_url(
        settings.BROKER_URL,
        encoding="utf-8",
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5,
    )
    # Test Redis connection
    await redis_client.ping()
    logger.info("Redis broker connected")

    # Start consumer workers
    for i in range(settings.WORKER_COUNT):
        task = asyncio.create_task(consumer_worker(i))
        worker_tasks.append(task)
        logger.info(f"Worker {i} started")

    logger.info(f"=== Aggregator ready | {settings.WORKER_COUNT} workers ===")

    yield  # Application runs here

    # Shutdown
    logger.info("=== Aggregator shutting down ===")
    for task in worker_tasks:
        task.cancel()
    await asyncio.gather(*worker_tasks, return_exceptions=True)
    await redis_client.aclose()
    await db.close()
    logger.info("=== Aggregator stopped ===")


app = FastAPI(
    title="Pub-Sub Log Aggregator",
    description="Sistem log aggregator terdistribusi dengan idempotency dan deduplication",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─────────────────────────────────────────────
# CONSUMER WORKER
# ─────────────────────────────────────────────

async def consumer_worker(worker_id: int):
    """
    Background worker that consumes events from Redis queue.
    Multiple workers run concurrently; unique constraint in DB prevents double-processing.
    Isolation level: READ COMMITTED - sufficient because unique constraint (topic, event_id)
    provides atomicity at the DB level. SERIALIZABLE would add unnecessary overhead.
    """
    logger.info(f"[Worker {worker_id}] Starting consumer loop")
    while True:
        try:
            # BLPOP blocks until item available (timeout=2s to allow graceful shutdown)
            result = await redis_client.blpop(
                settings.REDIS_QUEUE_KEY, timeout=2
            )
            if result is None:
                continue

            _, raw_event = result
            import json
            event_data = json.loads(raw_event)

            await process_event(event_data, worker_id)

        except asyncio.CancelledError:
            logger.info(f"[Worker {worker_id}] Cancelled, stopping")
            break
        except Exception as e:
            logger.error(f"[Worker {worker_id}] Unexpected error: {e}", exc_info=True)
            await asyncio.sleep(1)  # backoff on error


async def process_event(event_data: dict, worker_id: int = 0):
    """
    Process a single event with idempotency guarantee.
    Uses INSERT ... ON CONFLICT DO NOTHING inside a transaction.
    If the (topic, event_id) pair already exists → duplicate, silently skip.
    Setiap hasil operasi (PROCESSED / DUPLICATE / ERROR) dicatat ke audit_log.
    """
    topic = event_data.get("topic")
    event_id = event_data.get("event_id")
    timestamp = event_data.get("timestamp")
    source = event_data.get("source")
    payload = event_data.get("payload", {})

    logger.debug(f"[Worker {worker_id}] Processing event {event_id} topic={topic}")

    try:
        inserted = await db.insert_event(
            topic=topic,
            event_id=event_id,
            timestamp=timestamp,
            source=source,
            payload=payload,
        )

        if inserted:
            await db.increment_stat("unique_processed")
            logger.info(
                f"[Worker {worker_id}] ✅ Processed event_id={event_id} topic={topic}"
            )
            await db.write_audit_log(
                event_type="PROCESSED",
                topic=topic,
                event_id=event_id,
                worker_id=worker_id,
                detail=f"source={source}",
            )
        else:
            await db.increment_stat("duplicate_dropped")
            logger.info(
                f"[Worker {worker_id}] 🔁 Duplicate detected event_id={event_id} topic={topic}"
            )
            await db.write_audit_log(
                event_type="DUPLICATE",
                topic=topic,
                event_id=event_id,
                worker_id=worker_id,
                detail="ON CONFLICT DO NOTHING triggered",
            )

    except Exception as e:
        logger.error(
            f"[Worker {worker_id}] ❌ Failed to process event {event_id}: {e}",
            exc_info=True,
        )
        await db.write_audit_log(
            event_type="ERROR",
            topic=topic,
            event_id=event_id,
            worker_id=worker_id,
            detail=str(e),
        )


# ─────────────────────────────────────────────
# API ENDPOINTS
# ─────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse, tags=["Observability"])
async def health_check():
    """Liveness & readiness probe."""
    db_ok = await db.ping()
    try:
        await redis_client.ping()
        broker_ok = True
    except Exception:
        broker_ok = False

    status = "healthy" if (db_ok and broker_ok) else "degraded"
    return HealthResponse(
        status=status,
        database="connected" if db_ok else "disconnected",
        broker="connected" if broker_ok else "disconnected",
        uptime_seconds=round(time.time() - START_TIME, 2),
        version="1.0.0",
        workers_active=len([t for t in worker_tasks if not t.done()]),
    )


@app.post("/publish", response_model=PublishResponse, tags=["Events"])
async def publish_event(request: PublishRequest):
    """
    Menerima single event dan mengirim ke Redis queue.
    Validasi skema dilakukan oleh Pydantic model.
    Increment received counter langsung (bukan setelah diproses) untuk akurasi stats.
    """
    import json

    await db.increment_stat("received")

    event_dict = {
        "topic": request.topic,
        "event_id": request.event_id,
        "timestamp": request.timestamp.isoformat() if hasattr(request.timestamp, 'isoformat') else request.timestamp,
        "source": request.source,
        "payload": request.payload,
    }

    # Push to Redis queue → workers will consume
    await redis_client.rpush(settings.REDIS_QUEUE_KEY, json.dumps(event_dict))
    logger.info(f"📥 Received event_id={request.event_id} topic={request.topic}")

    return PublishResponse(
        success=True,
        message="Event queued for processing",
        event_id=request.event_id,
        received_at=datetime.now(timezone.utc).isoformat(),
    )


@app.post("/publish/batch", response_model=BatchPublishResponse, tags=["Events"])
async def publish_batch(request: BatchPublishRequest):
    """
    Menerima batch event secara atomik.
    Semua event valid dimasukkan ke queue; event invalid dilaporkan.
    """
    import json

    total = len(request.events)
    queued = 0
    failed = 0

    pipeline = redis_client.pipeline()
    for evt in request.events:
        try:
            await db.increment_stat("received")
            event_dict = {
                "topic": evt.topic,
                "event_id": evt.event_id,
                "timestamp": evt.timestamp.isoformat() if hasattr(evt.timestamp, 'isoformat') else evt.timestamp,
                "source": evt.source,
                "payload": evt.payload,
            }
            pipeline.rpush(settings.REDIS_QUEUE_KEY, json.dumps(event_dict))
            queued += 1
        except Exception as e:
            logger.error(f"Batch item failed: {e}")
            failed += 1

    await pipeline.execute()
    logger.info(f"📦 Batch received: {queued} queued, {failed} failed")

    return BatchPublishResponse(
        success=True,
        total_received=total,
        queued=queued,
        failed=failed,
    )


@app.get("/events", response_model=EventsListResponse, tags=["Events"])
async def get_events(
    topic: Optional[str] = Query(None, description="Filter by topic"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Menampilkan daftar event unik yang berhasil diproses."""
    events = await db.get_events(topic=topic, limit=limit, offset=offset)
    return EventsListResponse(
        success=True,
        topic=topic,
        count=len(events),
        events=events,
    )


@app.get("/stats", response_model=StatsResponse, tags=["Observability"])
async def get_stats():
    """Menampilkan statistik sistem secara real-time."""
    stats = await db.get_stats()
    topics = await db.get_topics()
    topic_counts = await db.get_topic_counts()
    queue_size = await redis_client.llen(settings.REDIS_QUEUE_KEY)

    uptime_seconds = round(time.time() - START_TIME, 2)
    days = int(uptime_seconds // 86400)
    hours = int((uptime_seconds % 86400) // 3600)
    mins = int((uptime_seconds % 3600) // 60)
    uptime_formatted = f"{days}d {hours}h {mins}m"

    return StatsResponse(
        received=stats.get("received", 0),
        unique_processed=stats.get("unique_processed", 0),
        duplicate_dropped=stats.get("duplicate_dropped", 0),
        topics=topics,
        topic_counts=topic_counts,
        uptime_seconds=uptime_seconds,
        uptime_formatted=uptime_formatted,
        workers_active=len([t for t in worker_tasks if not t.done()]),
        queue_size=queue_size,
    )


@app.get("/audit-log", tags=["Observability"])
async def get_audit_log(
    event_type: Optional[str] = Query(None, description="Filter: PROCESSED | DUPLICATE | ERROR"),
    limit: int = Query(50, ge=1, le=500),
):
    """
    Menampilkan audit log operasi kritis sistem.
    Setiap event yang diproses atau terdeteksi duplikat dicatat di sini.
    """
    entries = await db.get_audit_log(limit=limit, event_type=event_type)
    return {
        "success": True,
        "count": len(entries),
        "filter": event_type,
        "entries": entries,
    }


@app.delete("/events", tags=["Admin"])
async def reset_events():
    """Reset semua data (untuk testing saja)."""
    await db.reset_all()
    await redis_client.delete(settings.REDIS_QUEUE_KEY)
    logger.warning("⚠️  All events and stats reset via API")
    return {"success": True, "message": "All data reset"}
