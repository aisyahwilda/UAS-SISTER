"""
Test Suite - Pub-Sub Log Aggregator
20 tests covering: schema validation, deduplication, concurrency,
transactions, API endpoints, persistence, performance, edge cases.

Run: pytest tests/test_aggregator.py -v
Requires: aggregator running at http://localhost:8080
"""

import asyncio
import os
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import List

import pytest
import requests

BASE_URL = os.getenv("BASE_URL", "http://localhost:8080")

# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def make_event(topic: str = "test-topic", event_id: str = None) -> dict:
    return {
        "topic": topic,
        "event_id": event_id or f"evt-{uuid.uuid4()}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test-suite",
        "payload": {"test": True, "value": 42},
    }


def publish(event: dict) -> requests.Response:
    return requests.post(f"{BASE_URL}/publish", json=event, timeout=10)


def reset():
    requests.delete(f"{BASE_URL}/events", timeout=10)


# ─────────────────────────────────────────────
# Setup / Teardown
# ─────────────────────────────────────────────

@pytest.fixture(autouse=True)
def clean_state():
    """Reset aggregator state before each test."""
    reset()
    time.sleep(0.2)
    yield
    time.sleep(0.1)


# ─────────────────────────────────────────────
# 1. Schema Validation Tests (3 tests)
# ─────────────────────────────────────────────

class TestSchemaValidation:

    def test_valid_event_schema(self):
        """TC01: Valid event schema should return 200."""
        event = make_event()
        resp = publish(event)
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True
        assert data["event_id"] == event["event_id"]

    def test_missing_topic_rejected(self):
        """TC02: Missing required field 'topic' should return 422."""
        event = {
            "event_id": f"evt-{uuid.uuid4()}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "test",
            "payload": {},
        }
        resp = publish(event)
        assert resp.status_code == 422

    def test_missing_event_id_rejected(self):
        """TC03: Missing required field 'event_id' should return 422."""
        event = {
            "topic": "test-topic",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "test",
            "payload": {},
        }
        resp = publish(event)
        assert resp.status_code == 422


# ─────────────────────────────────────────────
# 2. Idempotency & Deduplication Tests (4 tests)
# ─────────────────────────────────────────────

class TestIdempotencyAndDeduplication:

    def test_duplicate_event_processed_once(self):
        """TC04: Same event_id sent 10x should only be processed once."""
        event_id = f"evt-dedup-{uuid.uuid4()}"
        event = make_event(event_id=event_id)

        for _ in range(10):
            resp = publish(event)
            assert resp.status_code == 200

        time.sleep(1.5)  # Wait for workers to process

        stats = requests.get(f"{BASE_URL}/stats").json()
        events = requests.get(
            f"{BASE_URL}/events", params={"topic": "test-topic"}
        ).json()

        matching = [e for e in events["events"] if e["event_id"] == event_id]
        assert len(matching) == 1, f"Expected 1 event in DB, got {len(matching)}"

    def test_different_event_ids_all_processed(self):
        """TC05: Different event_ids should all be processed."""
        topic = f"topic-{uuid.uuid4()}"
        event_ids = [f"evt-{uuid.uuid4()}" for _ in range(5)]

        for eid in event_ids:
            publish(make_event(topic=topic, event_id=eid))

        time.sleep(1.0)

        events = requests.get(
            f"{BASE_URL}/events", params={"topic": topic}
        ).json()
        assert events["count"] == 5

    def test_same_event_id_different_topics_both_processed(self):
        """TC06: Same event_id on different topics should both be stored."""
        event_id = f"evt-shared-{uuid.uuid4()}"
        publish(make_event(topic="topic-A", event_id=event_id))
        publish(make_event(topic="topic-B", event_id=event_id))

        time.sleep(1.0)

        events_a = requests.get(
            f"{BASE_URL}/events", params={"topic": "topic-A"}
        ).json()
        events_b = requests.get(
            f"{BASE_URL}/events", params={"topic": "topic-B"}
        ).json()

        assert events_a["count"] >= 1
        assert events_b["count"] >= 1

    def test_stats_reflect_deduplication(self):
        """TC07: Stats should correctly count duplicates vs unique."""
        topic = f"dedup-stats-{uuid.uuid4()}"
        event_id = f"evt-{uuid.uuid4()}"

        # Send 1 unique + 4 duplicates
        for i in range(5):
            publish(make_event(topic=topic, event_id=event_id))

        time.sleep(1.0)

        stats_before = requests.get(f"{BASE_URL}/stats").json()
        # duplicate_dropped must be >= 4
        assert stats_before["duplicate_dropped"] >= 4


# ─────────────────────────────────────────────
# 3. Concurrency & Transaction Tests (4 tests)
# ─────────────────────────────────────────────

class TestConcurrencyAndTransactions:

    def test_concurrent_publish_no_double_process(self):
        """TC08: 50 threads sending same event_id concurrently → only 1 stored."""
        event_id = f"evt-concurrent-{uuid.uuid4()}"
        event = make_event(event_id=event_id)
        topic = event["topic"]

        responses = []
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(publish, event) for _ in range(50)]
            for f in as_completed(futures):
                responses.append(f.result())

        # All requests should succeed (200)
        assert all(r.status_code == 200 for r in responses)

        time.sleep(2.0)

        events = requests.get(
            f"{BASE_URL}/events", params={"topic": topic}
        ).json()
        matching = [e for e in events["events"] if e["event_id"] == event_id]
        assert len(matching) == 1, f"Race condition! Found {len(matching)} copies"

    def test_concurrent_unique_events_all_processed(self):
        """TC09: 20 threads sending different events → all 20 stored."""
        topic = f"concurrent-unique-{uuid.uuid4()}"
        events = [make_event(topic=topic) for _ in range(20)]

        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(publish, evt) for evt in events]
            [f.result() for f in as_completed(futures)]

        time.sleep(2.0)

        result = requests.get(
            f"{BASE_URL}/events", params={"topic": topic}
        ).json()
        assert result["count"] == 20

    def test_batch_atomic_success(self):
        """TC10: Batch of valid events should all be queued."""
        topic = f"batch-test-{uuid.uuid4()}"
        batch = {"events": [make_event(topic=topic) for _ in range(10)]}
        resp = requests.post(f"{BASE_URL}/publish/batch", json=batch, timeout=10)
        assert resp.status_code == 200
        data = resp.json()
        assert data["queued"] == 10
        assert data["failed"] == 0

    def test_batch_deduplication(self):
        """TC11: Batch with duplicate event_ids processed correctly."""
        topic = f"batch-dedup-{uuid.uuid4()}"
        shared_id = f"evt-{uuid.uuid4()}"

        batch = {
            "events": [
                make_event(topic=topic, event_id=shared_id),
                make_event(topic=topic, event_id=shared_id),  # duplicate
                make_event(topic=topic, event_id=shared_id),  # duplicate
                make_event(topic=topic),  # unique
                make_event(topic=topic),  # unique
            ]
        }
        resp = requests.post(f"{BASE_URL}/publish/batch", json=batch, timeout=10)
        assert resp.status_code == 200

        time.sleep(1.5)

        result = requests.get(
            f"{BASE_URL}/events", params={"topic": topic}
        ).json()
        # Only 3 unique events (1 shared_id + 2 unique)
        assert result["count"] == 3


# ─────────────────────────────────────────────
# 4. API Endpoints Tests (3 tests)
# ─────────────────────────────────────────────

class TestAPIEndpoints:

    def test_health_endpoint(self):
        """TC12: /health should return healthy status."""
        resp = requests.get(f"{BASE_URL}/health", timeout=5)
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] in ["healthy", "degraded"]
        assert "database" in data
        assert "broker" in data
        assert "uptime_seconds" in data

    def test_get_events_filter_by_topic(self):
        """TC13: GET /events?topic=X should only return events for that topic."""
        topic_a = f"topic-filter-A-{uuid.uuid4()}"
        topic_b = f"topic-filter-B-{uuid.uuid4()}"

        for _ in range(3):
            publish(make_event(topic=topic_a))
        for _ in range(2):
            publish(make_event(topic=topic_b))

        time.sleep(1.0)

        resp_a = requests.get(f"{BASE_URL}/events", params={"topic": topic_a}).json()
        resp_b = requests.get(f"{BASE_URL}/events", params={"topic": topic_b}).json()

        assert resp_a["count"] == 3
        assert resp_b["count"] == 2
        assert all(e["topic"] == topic_a for e in resp_a["events"])

    def test_stats_endpoint_structure(self):
        """TC14: GET /stats should return correct structure."""
        resp = requests.get(f"{BASE_URL}/stats", timeout=5)
        assert resp.status_code == 200
        data = resp.json()
        required_fields = [
            "received", "unique_processed", "duplicate_dropped",
            "topics", "uptime_seconds", "workers_active", "queue_size"
        ]
        for field in required_fields:
            assert field in data, f"Missing field: {field}"


# ─────────────────────────────────────────────
# 5. Persistence Tests (2 tests)
# ─────────────────────────────────────────────

class TestPersistence:

    def test_events_persist_in_database(self):
        """TC15: Events should be retrievable after being processed."""
        topic = f"persist-{uuid.uuid4()}"
        event_ids = [f"evt-{uuid.uuid4()}" for _ in range(3)]

        for eid in event_ids:
            publish(make_event(topic=topic, event_id=eid))

        time.sleep(1.0)

        events = requests.get(
            f"{BASE_URL}/events", params={"topic": topic}
        ).json()

        stored_ids = {e["event_id"] for e in events["events"]}
        assert set(event_ids) == stored_ids

    def test_dedup_persists_after_reset_and_republish(self):
        """TC16: After publishing, re-sending same events should still be deduplicated."""
        topic = f"persist-dedup-{uuid.uuid4()}"
        event_id = f"evt-{uuid.uuid4()}"

        # First publish
        publish(make_event(topic=topic, event_id=event_id))
        time.sleep(0.8)

        # Re-send same event (simulating at-least-once delivery / retry)
        for _ in range(5):
            publish(make_event(topic=topic, event_id=event_id))

        time.sleep(1.0)

        events = requests.get(
            f"{BASE_URL}/events", params={"topic": topic}
        ).json()
        matching = [e for e in events["events"] if e["event_id"] == event_id]
        assert len(matching) == 1


# ─────────────────────────────────────────────
# 6. Performance / Stress Tests (2 tests)
# ─────────────────────────────────────────────

class TestPerformance:

    def test_batch_100_events_fast(self):
        """TC17: 100-event batch should be accepted within 2 seconds."""
        topic = f"perf-{uuid.uuid4()}"
        batch = {"events": [make_event(topic=topic) for _ in range(100)]}

        start = time.time()
        resp = requests.post(f"{BASE_URL}/publish/batch", json=batch, timeout=10)
        elapsed = time.time() - start

        assert resp.status_code == 200
        assert elapsed < 2.0, f"Batch took too long: {elapsed:.2f}s"

    def test_throughput_1000_events(self):
        """TC18: 1000 unique events should be queued in under 30 seconds."""
        topic = f"throughput-{uuid.uuid4()}"
        batch_size = 50
        total = 1000
        batches = [
            {"events": [make_event(topic=topic) for _ in range(batch_size)]}
            for _ in range(total // batch_size)
        ]

        start = time.time()
        for batch in batches:
            resp = requests.post(f"{BASE_URL}/publish/batch", json=batch, timeout=15)
            assert resp.status_code == 200
        elapsed = time.time() - start

        assert elapsed < 30.0, f"1000 events took too long: {elapsed:.2f}s"
        throughput = total / elapsed
        print(f"\n  Throughput: {throughput:.0f} events/s over {elapsed:.1f}s")


# ─────────────────────────────────────────────
# 7. Edge Cases Tests (2 tests)
# ─────────────────────────────────────────────

class TestEdgeCases:

    def test_empty_payload_accepted(self):
        """TC19: Event with empty payload dict should be accepted."""
        event = {
            "topic": "edge-case",
            "event_id": f"evt-empty-{uuid.uuid4()}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "test",
            "payload": {},
        }
        resp = publish(event)
        assert resp.status_code == 200

    def test_large_payload_accepted(self):
        """TC20: Event with large payload should be accepted (within limits)."""
        large_data = {f"field_{i}": f"value_{i}" * 10 for i in range(100)}
        event = {
            "topic": "edge-case-large",
            "event_id": f"evt-large-{uuid.uuid4()}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "test",
            "payload": large_data,
        }
        resp = publish(event)
        assert resp.status_code == 200


# ─────────────────────────────────────────────
# Run with: pytest tests/test_aggregator.py -v
# ─────────────────────────────────────────────
