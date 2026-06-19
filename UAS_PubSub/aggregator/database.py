"""
Database module - PostgreSQL operations with transaction safety.
Semua operasi kritis menggunakan transaksi eksplisit.
Isolation level: READ COMMITTED (default PostgreSQL).
Unique constraint (topic, event_id) menjamin idempotency atomik.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import asyncpg

logger = logging.getLogger("aggregator.database")


class Database:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.pool: asyncpg.Pool = None

    async def init(self):
        """Initialize connection pool and create schema."""
        self.pool = await asyncpg.create_pool(
            self.database_url,
            min_size=2,
            max_size=10,
            command_timeout=60,
        )
        await self._create_schema()

    async def _create_schema(self):
        """Create database schema if not exists."""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS processed_events (
                    id          BIGSERIAL PRIMARY KEY,
                    topic       TEXT        NOT NULL,
                    event_id    TEXT        NOT NULL,
                    source      TEXT,
                    timestamp   TIMESTAMPTZ,
                    payload     JSONB,
                    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CONSTRAINT uq_topic_event_id UNIQUE (topic, event_id)
                );

                CREATE INDEX IF NOT EXISTS idx_events_topic
                    ON processed_events (topic);

                CREATE INDEX IF NOT EXISTS idx_events_processed_at
                    ON processed_events (processed_at DESC);

                CREATE TABLE IF NOT EXISTS system_stats (
                    key   TEXT PRIMARY KEY,
                    value BIGINT NOT NULL DEFAULT 0
                );

                INSERT INTO system_stats (key, value) VALUES
                    ('received', 0),
                    ('unique_processed', 0),
                    ('duplicate_dropped', 0)
                ON CONFLICT (key) DO NOTHING;

                CREATE TABLE IF NOT EXISTS audit_log (
                    id          BIGSERIAL PRIMARY KEY,
                    event_type  TEXT NOT NULL,
                    topic       TEXT,
                    event_id    TEXT,
                    worker_id   INT,
                    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    detail      TEXT
                );
            """)
        logger.info("Database schema ready")

    async def insert_event(
        self,
        topic: str,
        event_id: str,
        timestamp: str,
        source: str,
        payload: dict,
    ) -> bool:
        """
        Insert event dengan INSERT ... ON CONFLICT DO NOTHING.
        Returns True jika berhasil diinsert (baru), False jika duplikat.

        Transaksi: BEGIN → INSERT (ON CONFLICT DO NOTHING) → COMMIT
        Race condition: jika 2 worker mencoba insert event yang sama secara bersamaan,
        PostgreSQL unique constraint memastikan hanya 1 yang berhasil.
        Isolation level READ COMMITTED sudah cukup karena constraint bekerja di level row.
        """
        # asyncpg membutuhkan objek datetime (bukan string) untuk kolom TIMESTAMPTZ
        try:
            if isinstance(timestamp, str):
                ts = datetime.fromisoformat(timestamp)
            elif isinstance(timestamp, datetime):
                ts = timestamp
            else:
                ts = datetime.now(timezone.utc)
        except (ValueError, TypeError):
            ts = datetime.now(timezone.utc)

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                result = await conn.execute(
                    """
                    INSERT INTO processed_events
                        (topic, event_id, source, timestamp, payload)
                    VALUES
                        ($1, $2, $3, $4, $5::JSONB)
                    ON CONFLICT (topic, event_id) DO NOTHING
                    """,
                    topic,
                    event_id,
                    source,
                    ts,
                    json.dumps(payload),
                )
                # asyncpg returns "INSERT 0 1" or "INSERT 0 0"
                inserted = result == "INSERT 0 1"
                return inserted

    async def increment_stat(self, key: str, amount: int = 1):
        """
        Increment statistik secara transaksional untuk mencegah lost-update.
        UPDATE ... SET value = value + 1 bersifat atomic di PostgreSQL.
        """
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE system_stats
                SET value = value + $1
                WHERE key = $2
                """,
                amount,
                key,
            )

    async def get_stats(self) -> Dict[str, int]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT key, value FROM system_stats")
            return {row["key"]: row["value"] for row in rows}

    async def get_events(
        self,
        topic: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        async with self.pool.acquire() as conn:
            if topic:
                rows = await conn.fetch(
                    """
                    SELECT topic, event_id, source, timestamp, payload,
                           received_at, processed_at
                    FROM processed_events
                    WHERE topic = $1
                    ORDER BY processed_at DESC
                    LIMIT $2 OFFSET $3
                    """,
                    topic, limit, offset,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT topic, event_id, source, timestamp, payload,
                           received_at, processed_at
                    FROM processed_events
                    ORDER BY processed_at DESC
                    LIMIT $1 OFFSET $2
                    """,
                    limit, offset,
                )
            result = []
            for row in rows:
                result.append({
                    "topic": row["topic"],
                    "event_id": row["event_id"],
                    "source": row["source"],
                    "timestamp": row["timestamp"].isoformat() if row["timestamp"] else None,
                    "payload": json.loads(row["payload"]) if row["payload"] else {},
                    "received_at": row["received_at"].isoformat(),
                    "processed_at": row["processed_at"].isoformat(),
                })
            return result

    async def get_topics(self) -> List[str]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT DISTINCT topic FROM processed_events ORDER BY topic"
            )
            return [row["topic"] for row in rows]

    async def get_topic_counts(self) -> Dict[str, int]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT topic, COUNT(*) as cnt FROM processed_events GROUP BY topic"
            )
            return {row["topic"]: row["cnt"] for row in rows}

    async def ping(self) -> bool:
        try:
            async with self.pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            return True
        except Exception:
            return False

    async def reset_all(self):
        """Reset semua data (untuk testing)."""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("TRUNCATE TABLE processed_events RESTART IDENTITY")
                await conn.execute("TRUNCATE TABLE audit_log RESTART IDENTITY")
                await conn.execute(
                    "UPDATE system_stats SET value = 0"
                )
        logger.warning("All data reset")

    async def write_audit_log(
        self,
        event_type: str,
        topic: str = None,
        event_id: str = None,
        worker_id: int = None,
        detail: str = None,
    ):
        """
        Tulis entri ke audit_log untuk setiap operasi kritis:
        - 'PROCESSED'  → event baru berhasil di-insert
        - 'DUPLICATE'  → event duplikat terdeteksi dan diabaikan
        - 'ERROR'      → kegagalan saat memproses event
        """
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO audit_log (event_type, topic, event_id, worker_id, detail)
                    VALUES ($1, $2, $3, $4, $5)
                    """,
                    event_type,
                    topic,
                    event_id,
                    worker_id,
                    detail,
                )
        except Exception as e:
            # Jangan sampai audit log failure mengganggu alur utama
            logger.error(f"Failed to write audit log: {e}")

    async def get_audit_log(
        self,
        limit: int = 100,
        event_type: str = None,
    ) -> list:
        """Ambil entri audit log terbaru untuk observability."""
        async with self.pool.acquire() as conn:
            if event_type:
                rows = await conn.fetch(
                    """
                    SELECT id, event_type, topic, event_id, worker_id, created_at, detail
                    FROM audit_log
                    WHERE event_type = $1
                    ORDER BY created_at DESC
                    LIMIT $2
                    """,
                    event_type, limit,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT id, event_type, topic, event_id, worker_id, created_at, detail
                    FROM audit_log
                    ORDER BY created_at DESC
                    LIMIT $1
                    """,
                    limit,
                )
            return [
                {
                    "id": row["id"],
                    "event_type": row["event_type"],
                    "topic": row["topic"],
                    "event_id": row["event_id"],
                    "worker_id": row["worker_id"],
                    "created_at": row["created_at"].isoformat(),
                    "detail": row["detail"],
                }
                for row in rows
            ]

    async def close(self):
        if self.pool:
            await self.pool.close()
