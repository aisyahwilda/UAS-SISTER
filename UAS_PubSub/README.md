# 🚀 Pub-Sub Log Aggregator Terdistribusi

Sistem Pub-Sub log aggregator multi-service dengan **Idempotent Consumer**, **Deduplication**, dan **Transaksi/Kontrol Konkurensi**.

**Mata Kuliah:** Sistem Paralel dan Terdistribusi - E2526

---

## 📋 Daftar Isi

- [Deskripsi Sistem](#deskripsi-sistem)
- [Arsitektur](#arsitektur)
- [Teknologi](#teknologi)
- [Instalasi & Menjalankan](#instalasi--menjalankan)
- [API Endpoints](#api-endpoints)
- [Fitur Utama](#fitur-utama)
- [Testing](#testing)
- [Video Demo](#video-demo)

---

## 📝 Deskripsi Sistem

Sistem ini adalah **Log Aggregator** berbasis arsitektur **Publish-Subscribe** yang dirancang untuk mengumpulkan dan memproses log dari berbagai sumber secara terdistribusi. Sistem menjamin:

1. **Idempotency** — Event yang sama tidak akan diproses ulang, walau diterima berkali-kali
2. **Deduplication** — Duplikat event dideteksi via unique constraint `(topic, event_id)` di PostgreSQL
3. **Transaction Safety** — Operasi database dilakukan secara atomik dengan `INSERT ... ON CONFLICT DO NOTHING`
4. **Concurrency Control** — Multiple worker berjalan paralel tanpa race condition

---

## 🏗 Arsitektur

```
┌─────────────────────────────────────────────────────────────┐
│                   Docker Compose Network                     │
│                                                             │
│  ┌──────────────┐    ┌──────────────┐   ┌──────────────┐  │
│  │  Publisher   │───▶│    Redis     │◀──│  Aggregator  │  │
│  │  (Simulator) │    │   (Broker)   │   │  (FastAPI)   │  │
│  └──────────────┘    └──────────────┘   └──────┬───────┘  │
│                                                 │           │
│                                                 ▼           │
│                                          ┌──────────────┐  │
│                                          │  PostgreSQL  │  │
│                                          │  (Storage)   │  │
│                                          └──────┬───────┘  │
│                                                 │           │
│                                                 ▼           │
│                                          ┌──────────────┐  │
│                                          │   pg_data    │  │
│                                          │   (Volume)   │  │
│                                          └──────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### Alur Data

```
Publisher → POST /publish/batch
         → Redis RPUSH (queue)
         → Worker BLPOP (consume)
         → PostgreSQL INSERT ON CONFLICT DO NOTHING
         → Stats UPDATE (atomic)
```

### Komponen

| Service | Deskripsi | Port |
|---|---|---|
| **aggregator** | FastAPI API + consumer workers | 8080 |
| **publisher** | Event simulator (30% duplikat) | — |
| **broker** | Redis message queue | 6379 (internal) |
| **storage** | PostgreSQL persistent store | 5432 (internal) |

---

## 🛠 Teknologi

| Komponen | Teknologi |
|---|---|
| Language | Python 3.11 |
| Framework | FastAPI + Uvicorn |
| Database | PostgreSQL 16 |
| Message Broker | Redis 7 |
| Container | Docker + Docker Compose |
| Testing | Pytest (20 tests) + K6 |

---

## 🚀 Instalasi & Menjalankan

### Prerequisites

- Docker & Docker Compose terinstall
- Port 8080 tersedia

### Quick Start

```bash
# Clone repository
git clone <repository-url>
cd UAS_PubSub

# Build dan jalankan semua service
docker compose up --build

# Atau di background
docker compose up --build -d
```

### Jalankan Publisher (Simulator 20.000 event)

```bash
# Jalankan publisher dengan konfigurasi default (20.000 event, 30% duplikat)
docker compose --profile publisher up publisher

# Custom configuration
docker compose --profile publisher run \
  -e EVENT_COUNT=5000 \
  -e DUPLICATE_RATE=0.4 \
  publisher
```

### Jalankan Multiple Workers (Uji Konkurensi)

```bash
docker compose --profile workers up -d
```

### Stop Service

```bash
# Stop tanpa hapus data
docker compose down

# Stop dan hapus data (reset)
docker compose down -v
```

### Bukti Persistensi

```bash
# 1. Publish beberapa event
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{"topic":"sensor","event_id":"test-1","timestamp":"2026-06-16T10:00:00Z","source":"device","payload":{}}'

# 2. Hapus container (tanpa hapus volume)
docker compose down

# 3. Jalankan ulang
docker compose up -d

# 4. Data masih ada!
curl http://localhost:8080/events?topic=sensor
```

---

## 📡 API Endpoints

### Health Check

```
GET /health
```

### Publish Single Event

```
POST /publish
Content-Type: application/json

{
  "topic": "sensor-data",
  "event_id": "evt-550e8400-e29b-41d4",
  "timestamp": "2026-06-16T10:30:00Z",
  "source": "iot-device-01",
  "payload": { "temperature": 30, "humidity": 75 }
}
```

### Publish Batch Events

```
POST /publish/batch
Content-Type: application/json

{
  "events": [ { ...event1 }, { ...event2 } ]
}
```

### Get Events

```
GET /events?topic=sensor-data&limit=100&offset=0
```

### Get Statistics

```
GET /stats
```

Response:
```json
{
  "received": 20000,
  "unique_processed": 14000,
  "duplicate_dropped": 6000,
  "topics": ["sensor-data", "app-logs"],
  "uptime_seconds": 3600.5,
  "workers_active": 4,
  "queue_size": 0
}
```

### Audit Log

```
GET /audit-log?event_type=PROCESSED&limit=50
GET /audit-log?event_type=DUPLICATE&limit=50
GET /audit-log?event_type=ERROR&limit=50
```

Response:
```json
{
  "success": true,
  "count": 3,
  "filter": "DUPLICATE",
  "entries": [
    {
      "id": 42,
      "event_type": "DUPLICATE",
      "topic": "sensor-data",
      "event_id": "evt-abc123",
      "worker_id": 2,
      "created_at": "2026-06-19T10:00:00+00:00",
      "detail": "ON CONFLICT DO NOTHING triggered"
    }
  ]
}
```

### Reset (Testing Only)

```
DELETE /events
```

---

## ✨ Fitur Utama

### 1. Idempotency & Deduplication

- **Unique Constraint**: `UNIQUE(topic, event_id)` di PostgreSQL
- **Atomic Dedup**: `INSERT ... ON CONFLICT DO NOTHING`
- **Persistent**: Dedup store tetap ada meski container di-restart

### 2. Transaction & Concurrency Control

- **Isolation Level**: READ COMMITTED (default PostgreSQL)
- **Atomic Operations**: Setiap insert dalam satu transaction
- **4 Concurrent Workers**: Memproses queue paralel tanpa double-processing
- **Stats atomik**: `UPDATE ... SET value = value + 1` mencegah lost-update

### 3. Reliability

- **At-least-once Delivery**: Publisher bisa kirim ulang; sistem tetap konsisten
- **Crash Tolerance**: Data persisten via Docker named volumes
- **Graceful Shutdown**: Workers selesaikan task sebelum berhenti

### 4. Observability

- **Structured Logging**: Level INFO/ERROR/DEBUG dengan context
- **Real-time Metrics**: `/stats` endpoint
- **Health Probe**: `/health` untuk readiness/liveness

---

## 🧪 Testing

### Jalankan Unit/Integration Tests

```bash
# Install dependencies
pip install -r tests/requirements.txt

# Pastikan aggregator sudah berjalan (docker compose up)
pytest tests/test_aggregator.py -v

# Dengan coverage
pytest tests/test_aggregator.py -v --cov=aggregator
```

### Cakupan 20 Tests

| Kategori | Tests |
|---|---|
| Schema Validation | TC01–TC03 |
| Idempotency & Dedup | TC04–TC07 |
| Concurrency & Transactions | TC08–TC11 |
| API Endpoints | TC12–TC14 |
| Persistence | TC15–TC16 |
| Performance / Stress | TC17–TC18 |
| Edge Cases | TC19–TC20 |

### Load Testing dengan K6

```bash
# Install K6: https://k6.io/docs/get-started/installation/

# Jalankan smoke test (1 VU, 30s - sanity check cepat)
k6 run --env SCENARIO=smoke k6/load_test.js

# Jalankan load test (ramp 50 VU, 4 menit)
k6 run --env SCENARIO=load k6/load_test.js

# Jalankan stress test (ramp 100 VU)
k6 run --env SCENARIO=stress k6/load_test.js

# Jalankan soak test (20 VU, 5 menit)
k6 run --env SCENARIO=soak k6/load_test.js

# Custom VUs dan durasi
k6 run --vus 50 --duration 5m k6/load_test.js

# Hasil disimpan otomatis ke: k6/load_test_result.json
```

---

## 🎬 Video Demo

> **⚠️ WAJIB DIISI SEBELUM SUBMIT**

**Link YouTube:** `[GANTI DENGAN LINK YOUTUBE KAMU]`

Contoh: `https://youtu.be/XXXXXXXXXX`

Video mencakup (≥ 25 menit):
1. ✅ Arsitektur multi-service dan alasan desain
2. ✅ Build image dan `docker compose up`
3. ✅ Pengiriman event duplikat dan bukti idempotency
4. ✅ Demonstrasi multi-worker dan hasil konsisten
5. ✅ `GET /events` dan `GET /stats`
6. ✅ Crash/recreate container + bukti data persisten
7. ✅ Keamanan jaringan lokal (tanpa dependensi eksternal)
8. ✅ Observability (logging, metrics)

---

## 📁 Struktur Direktori

```
UAS_PubSub/
├── aggregator/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py        # FastAPI app + consumer workers
│   ├── config.py      # Settings dari environment
│   ├── models.py      # Pydantic models
│   └── database.py    # PostgreSQL operations
├── publisher/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py        # Event simulator
├── tests/
│   ├── requirements.txt
│   └── test_aggregator.py  # 20 pytest tests
├── k6/
│   └── load_test.js   # K6 load testing
├── docker-compose.yml
├── README.md
└── report.md          # Laporan lengkap
```

---

## 📚 Referensi

- Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. (2011). *Distributed Systems: Concepts and Design* (5th ed.). Addison-Wesley.
- FastAPI Documentation: https://fastapi.tiangolo.com/
- PostgreSQL Documentation: https://www.postgresql.org/docs/
- Redis Documentation: https://redis.io/docs/
- K6 Documentation: https://k6.io/docs/

---

**© 2026 - Sistem Paralel dan Terdistribusi E2526**
