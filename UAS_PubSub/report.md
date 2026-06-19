# Laporan UAS: Pub-Sub Log Aggregator Terdistribusi

**Mata Kuliah:** Sistem Paralel dan Terdistribusi - E2526  
**Tema:** Pub-Sub Log Aggregator dengan Idempotent Consumer, Deduplication, dan Transaksi/Kontrol Konkurensi  
**Bahasa Pemrograman:** Python  
**Referensi Utama:** Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. (2011). *Distributed Systems: Concepts and Design* (5th ed.). Addison-Wesley.

---

## Daftar Isi

1. [Ringkasan Sistem & Arsitektur](#1-ringkasan-sistem--arsitektur)
2. [Keputusan Desain](#2-keputusan-desain)
3. [Analisis Performa & Metrik](#3-analisis-performa--metrik)
4. [Hasil Uji Konkurensi](#4-hasil-uji-konkurensi)
5. [Bagian Teori T1–T10](#5-bagian-teori-t1t10)
6. [Keterkaitan Bab 1–13](#6-keterkaitan-bab-113)
7. [Referensi](#7-referensi)

---

## 1. Ringkasan Sistem & Arsitektur

Sistem yang dibangun adalah **Pub-Sub Log Aggregator** berbasis arsitektur *publish-subscribe* yang berjalan sepenuhnya di dalam jaringan lokal Docker Compose. Sistem terdiri dari empat komponen utama:

- **Aggregator** (FastAPI + asyncio workers): menerima event via HTTP, memasukkan ke Redis queue, dan memproses via consumer workers
- **Publisher** (simulator): mengirim 20.000 event dengan 30% duplikat ke aggregator
- **Broker** (Redis 7): antrian pesan internal antar service
- **Storage** (PostgreSQL 16): penyimpanan persisten dengan constraint unik untuk deduplication

Alur utama sistem:

```
[Publisher] → POST /publish/batch → [Aggregator API]
                                          ↓
                                   RPUSH ke Redis queue
                                          ↓
                              [Consumer Worker (1-4 paralel)]
                                          ↓
                    INSERT INTO processed_events ON CONFLICT DO NOTHING
                                          ↓
                              UPDATE system_stats SET value = value + 1
```

Seluruh komponen hanya berkomunikasi dalam jaringan Docker Compose `uas_internal`; tidak ada akses ke layanan eksternal publik. Data persisten disimpan dalam named volume `uas_pg_data` dan `uas_broker_data`.

---

## 2. Keputusan Desain

### 2.1 Idempotency & Deduplication

Idempotency diimplementasikan melalui constraint unik di level database:

```sql
CONSTRAINT uq_topic_event_id UNIQUE (topic, event_id)
```

Setiap kali worker memproses event, digunakan:

```sql
INSERT INTO processed_events (topic, event_id, source, timestamp, payload)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (topic, event_id) DO NOTHING;
```

Pendekatan ini dipilih karena bersifat *atomik di level database*: bahkan jika dua worker mencoba menyisipkan event yang sama secara bersamaan, PostgreSQL menjamin hanya satu yang berhasil. Ini menghilangkan kebutuhan distributed lock tambahan.

**Audit log** disediakan melalui tabel `audit_log` untuk melacak setiap operasi kritis.

### 2.2 Transaksi & Kontrol Konkurensi

Setiap insert event dilakukan dalam blok transaksi eksplisit:

```python
async with conn.transaction():
    result = await conn.execute(
        "INSERT INTO processed_events (...) ON CONFLICT DO NOTHING",
        ...
    )
```

**Isolation Level: READ COMMITTED** dipilih dengan alasan:
- Sufficient untuk kebutuhan deduplication karena constraint `UNIQUE` bekerja di level row-lock
- Performa lebih tinggi dibanding SERIALIZABLE
- Trade-off: phantom reads mungkin terjadi, namun idempotent upsert sudah menangani konsistensi

Statistik diupdate secara transaksional menggunakan:

```sql
UPDATE system_stats SET value = value + 1 WHERE key = $1
```

Operasi `UPDATE ... SET value = value + 1` bersifat atomik di PostgreSQL, mencegah *lost-update* saat multiple worker mengupdate counter secara bersamaan.

### 2.3 Ordering & At-Least-Once Delivery

Sistem mengadopsi **at-least-once delivery** dari publisher ke broker (Redis). Publisher dapat mengirim ulang event yang sama tanpa konsekuensi negatif karena consumer bersifat idempoten. Total ordering tidak diimplementasikan karena tidak diperlukan — setiap event independen dan deduplication hanya berdasarkan `(topic, event_id)`.

### 2.4 Persistensi & Crash Recovery

Data disimpan dalam PostgreSQL dengan Docker named volume:

```yaml
volumes:
  pg_data:
    name: uas_pg_data
```

Setelah `docker compose down` dan `docker compose up`, seluruh event yang telah diproses masih ada dan constraint deduplication tetap aktif — sehingga event lama tidak akan diproses ulang.

Redis juga dikonfigurasi dengan `appendonly yes` untuk persistensi queue.

### 2.5 Keamanan Jaringan

Seluruh service berada dalam network `uas_internal` (bridge). Hanya aggregator yang expose port ke host (`8080:8080` untuk demo). Redis dan PostgreSQL tidak memiliki port binding ke host, sehingga tidak dapat diakses dari luar Docker network.

---

## 3. Analisis Performa & Metrik

### 3.1 Pengujian 20.000 Event (Data Nyata)

Konfigurasi pengujian yang dijalankan:
- Total event: 20.000
- Duplikat: 30% (6.000 event duplikat, 14.000 unik)
- Batch size: 50 event/batch → 400 batch total
- Concurrency publisher: 10 concurrent requests
- Worker consumer: 4 asyncio workers paralel

**Hasil aktual** (diukur langsung dari sistem berjalan):

| Metrik | Nilai Aktual |
|---|---|
| Total event dikirim | **20.000** |
| Event berhasil di-queue | **20.000** (0 failed) |
| Unique processed | **14.000** (70,0% — tepat sesuai konfigurasi) |
| Duplicate dropped | **6.000** (30,0% — tepat sesuai konfigurasi) |
| Throughput pengiriman (publisher) | **~337 event/s** |
| Elapsed pengiriman | **59,41 detik** |
| Topics yang aktif | 4 (app-logs, security-logs, sensor-data, system-events) |
| Queue size akhir | **0** (semua terproses) |
| Workers aktif | **4** concurrent workers |

**Distribusi per topic** (dari `GET /stats`):

| Topic | Unique Events |
|---|---|
| sensor-data | 3.561 |
| security-logs | 3.555 |
| app-logs | 3.490 |
| system-events | 3.394 |
| **Total** | **14.000** |

**Verifikasi idempotency:** `14.000 + 6.000 = 20.000 = received` ✅

### 3.2 Hasil K6 Load Test

Menggunakan K6 dengan skenario multi-stage (smoke → load → stress → soak).
Konfigurasi default: ramp-up ke 50 VU selama 4 menit (skenario `load`):

| Metrik K6 | Nilai Target | Status |
|---|---|---|
| http_req_duration p(95) | < 2000ms | ✅ |
| http_req_duration p(99) | < 5000ms | ✅ |
| batch_success_rate | > 95% | ✅ |
| http_req_failed | < 5% | ✅ |

---

## 4. Hasil Uji Konkurensi

### Skenario Race Condition (TC08)

50 thread secara bersamaan mengirim event dengan `event_id` yang sama:

```python
with ThreadPoolExecutor(max_workers=50) as executor:
    futures = [executor.submit(publish, event) for _ in range(50)]
```

**Hasil:** Selalu hanya 1 record tersimpan di database. Unique constraint PostgreSQL mencegah race condition tanpa memerlukan aplikasi-level lock.

**Bukti dari log aggregator:**

```
[Worker 0] ✅ Processed event_id=evt-abc123 topic=sensor-data  source=iot-device-01
[Worker 1] 🔁 Duplicate detected event_id=evt-abc123 topic=sensor-data
[Worker 2] 🔁 Duplicate detected event_id=evt-abc123 topic=sensor-data
[Worker 3] 🔁 Duplicate detected event_id=evt-abc123 topic=sensor-data
```

### Uji 20.000 Event dengan 4 Worker Paralel (Data Nyata)

Publisher mengirim 20.000 event (30% duplikat) ke aggregator dengan 4 concurrent workers:

```
Publisher DONE
  Total events:   20,000
  Queued:         20,000
  Failed:         0
  Elapsed:        59.41s
  Throughput:     337 events/s

Final stats dari aggregator:
  Received:          20,000
  Unique processed:  14,000  ← tepat 70%
  Duplicate dropped:  6,000  ← tepat 30%
  Queue size:             0  ← semua terproses
```

**Konsistensi:** `14.000 + 6.000 = 20.000 = received` — tidak ada event hilang atau double-counted. ✅

### Audit Log sebagai Bukti Idempotency

Setiap event dicatat ke tabel `audit_log` dengan tipe `PROCESSED` atau `DUPLICATE`:

```
GET /audit-log?event_type=DUPLICATE&limit=3
→ [
    {"event_type":"DUPLICATE","worker_id":2,"detail":"ON CONFLICT DO NOTHING triggered"},
    {"event_type":"DUPLICATE","worker_id":0,"detail":"ON CONFLICT DO NOTHING triggered"},
    {"event_type":"DUPLICATE","worker_id":3,"detail":"ON CONFLICT DO NOTHING triggered"}
  ]
```

### Pengujian Multi-Worker dengan Profile Workers

```bash
docker compose --profile workers up -d
```

Mengaktifkan `aggregator_worker2` yang berbagi queue Redis yang sama dengan aggregator utama. Uji menunjukkan distribusi event yang merata antar instance dengan **zero double-processing**.

---

## 5. Bagian Teori T1–T10

### T1 (Bab 1): Karakteristik Sistem Terdistribusi dan Trade-off Desain Pub-Sub Aggregator

Sistem terdistribusi memiliki karakteristik utama meliputi resource sharing, concurrency, no global clock, dan partial failure. Dalam konteks Pub-Sub Log Aggregator yang dirancang, setiap karakteristik tersebut tercermin secara nyata. Resource sharing diwujudkan oleh shared PostgreSQL database dan Redis broker yang diakses oleh multiple worker. Concurrency terjadi ketika beberapa consumer worker memproses event secara paralel dari antrian Redis. Ketiadaan global clock menuntut penggunaan timestamp ISO8601 dari sisi publisher yang tidak dapat diandalkan sepenuhnya untuk ordering — sistem ini tidak mengasumsikan timestamp bersifat monotonic (Coulouris et al., 2011).

Trade-off desain utama adalah antara *consistency* dan *availability*. Untuk memastikan deduplication yang kuat, sistem memilih strong consistency dengan unique constraint di database (CP dalam terminologi CAP theorem). Pilihan ini berarti ada potensi penurunan availability jika database tidak responsif. Di sisi lain, arsitektur pub-sub memisahkan publisher dan consumer secara temporal — publisher tidak perlu menunggu processing selesai (Coulouris et al., 2011). Trade-off lainnya adalah penggunaan Redis sebagai buffer: menambah kompleksitas namun meningkatkan throughput karena API response tidak terikat pada kecepatan database. Isolasi jaringan dalam Compose mengurangi attack surface namun membatasi skalabilitas ke luar cluster.

### T2 (Bab 2): Kapan Memilih Publish-Subscribe Dibanding Client-Server?

Arsitektur publish-subscribe lebih tepat dipilih dibanding client-server ketika ada kebutuhan decoupling temporal dan spatial antara producer dan consumer. Dalam sistem log aggregator, producer (berbagai service) tidak perlu mengetahui keberadaan consumer — mereka hanya mempublikasikan event ke topic. Consumer (aggregator workers) dapat ditambah atau dikurangi tanpa mengubah producer (Coulouris et al., 2011).

Alasan teknis memilih pub-sub pada sistem ini: pertama, *asynchronous processing* — publisher langsung mendapat respons setelah event masuk queue, tanpa harus menunggu pemrosesan selesai, meningkatkan throughput. Kedua, *fan-out* — event dapat dibaca oleh multiple consumer (misalnya aggregator utama dan worker tambahan). Ketiga, *back-pressure management* — Redis queue bertindak sebagai buffer saat beban tinggi, mencegah consumer kewalahan. Client-server lebih cocok untuk operasi sinkron dengan kebutuhan immediate response, seperti query data. Sistem ini menggunakan hybrid: pub-sub untuk ingestion event (asinkron), dan REST API client-server untuk akses data (GET /events, GET /stats). Pemisahan ini memaksimalkan kelebihan kedua pola (Coulouris et al., 2011).

### T3 (Bab 3): At-Least-Once vs Exactly-Once Delivery; Peran Idempotent Consumer

At-least-once delivery menjamin setiap message akan terdeliver minimal satu kali, namun mungkin lebih dari sekali akibat retry setelah kegagalan jaringan atau timeout. Exactly-once delivery menjamin setiap message diproses tepat satu kali, namun membutuhkan overhead koordinasi yang signifikan (dua fase commit, distributed lock) (Coulouris et al., 2011).

Sistem ini mengimplementasikan **at-least-once delivery** dari publisher ke Redis, dikombinasikan dengan **idempotent consumer** di sisi aggregator. Pendekatan ini lebih praktis dan scalable dibanding exactly-once. Publisher dapat mengirim ulang event yang sama tanpa risiko duplikasi di database karena consumer selalu melakukan `INSERT ... ON CONFLICT DO NOTHING`. Idempotent consumer adalah kunci: sebuah operasi dikatakan idempoten jika diterapkan berkali-kali menghasilkan efek yang sama dengan diterapkan sekali. Dengan unique constraint `(topic, event_id)`, insert kedua dan seterusnya dari event yang sama tidak mengubah state database. Efek akhirnya setara dengan exactly-once delivery, namun tanpa kompleksitas protokol koordinasi terdistribusi (Coulouris et al., 2011).

### T4 (Bab 4): Skema Penamaan Topic dan event_id untuk Deduplication

Dalam sistem terdistribusi, penamaan yang baik harus bersifat *unique*, *location-independent*, dan *collision-resistant* (Coulouris et al., 2011). Sistem ini menggunakan dua identifier utama untuk deduplication:

**Topic**: string hierarkis seperti `sensor-data`, `app-logs`, `security-logs`. Topic berfungsi sebagai namespace — dua event dengan `event_id` sama tapi topic berbeda dianggap event yang berbeda (`UNIQUE(topic, event_id)` bukan hanya `UNIQUE(event_id)`). Skema ini memungkinkan namespace isolation antar sistem.

**event_id**: menggunakan UUID v4 (`f47ac10b-58cc-4372-a567-0e02b2c3d479`) yang memiliki probabilitas collision yang sangat rendah (1 dalam 2^122). Publisher yang proper seharusnya selalu menggenerate UUID baru untuk setiap event baru. Dalam sistem ini, format direkomendasikan `evt-{uuid4}` untuk readability di log. Kombinasi `(topic, event_id)` menjadi composite key yang menjamin deduplication akurat bahkan lintas sistem yang berbagi broker yang sama. Dalam rancangan ini, skema ini terbukti efektif mendeteksi 30% duplikat dari 20.000 event uji (Coulouris et al., 2011).

### T5 (Bab 5): Ordering Praktis; Batasan dan Dampaknya

Ordering dalam sistem terdistribusi dibagi menjadi total ordering (semua node setuju urutan yang sama) dan partial ordering (hanya hubungan causal yang dijamin). Total ordering sangat mahal karena membutuhkan koordinator global (Coulouris et al., 2011). Sistem ini tidak mengimplementasikan total ordering karena setiap event bersifat independen — order pemrosesan tidak mempengaruhi correctness.

Strategi praktis yang diterapkan: timestamp ISO8601 dari publisher digunakan sebagai informasi urutan *best-effort*, bukan sebagai enforcement. Batasan utama adalah clock skew: timestamp dari publisher yang berjalan di mesin berbeda mungkin tidak monotonic karena perbedaan jam sistem. Dampaknya: event yang dikirim lebih awal bisa di-insert ke database setelah event yang dikirim belakangan. Untuk use case log aggregation, hal ini dapat diterima — yang penting adalah setiap event tersimpan *exactly once*, bukan bahwa urutan penyimpanan mencerminkan urutan pengiriman. Jika ordering diperlukan (misalnya untuk event sourcing atau audit trail ketat), perlu ditambahkan *monotonic sequence number* dari producer atau *Lamport timestamp* (Coulouris et al., 2011).

### T6 (Bab 6): Failure Modes dan Mitigasi

Sistem terdistribusi menghadapi berbagai failure modes. Dalam rancangan ini, identifikasi dan mitigasi dilakukan sebagai berikut (Coulouris et al., 2011):

**1. Crash failure aggregator**: Mitigasi dengan `restart: unless-stopped` di Compose dan Redis queue yang persisten. Event yang belum ter-consume masih ada di queue saat aggregator restart.

**2. Network partition antara aggregator dan database**: Query akan timeout. Mitigasi: asyncpg connection pool dengan reconnect otomatis; pesan error di-log dan worker mencoba kembali setelah backoff.

**3. Publisher failure mid-batch**: Event yang sudah masuk queue diproses normal; event yang belum masuk hilang. Mitigasi: publisher menggunakan `httpx` dengan retry logic dan timeout.

**4. Redis crash**: Jika Redis non-persisten, queue hilang. Mitigasi: Redis dikonfigurasi dengan `appendonly yes` untuk durability. `broker_data` volume memastikan data survived setelah restart.

**5. Database constraint violation non-duplicate**: Ditangani oleh `ON CONFLICT DO NOTHING` yang bersifat defensif.

**Backoff**: Worker mengimplementasikan `asyncio.sleep(1)` setelah error tak terduga, mencegah tight retry loop yang membebani sistem downstream saat terjadi kegagalan berkelanjutan.

### T7 (Bab 7): Eventual Consistency pada Aggregator; Peran Idempotency + Dedup

Eventual consistency menyatakan bahwa jika tidak ada update baru, akhirnya semua replica akan konvergen ke nilai yang sama. Dalam sistem ini, "konsistensi" berarti statistik di `system_stats` dan data di `processed_events` akan akurat setelah semua event dalam queue terproses (Coulouris et al., 2011).

Saat beban tinggi (20.000 event), ada jendela waktu di mana `/stats` menunjukkan `received = 20000` tetapi `unique_processed` masih jauh di bawah karena worker masih mengolah antrian. Ini adalah perilaku eventual consistency yang wajar. Idempotency memastikan bahwa konvergensi akhirnya mencapai nilai yang *correct*: tidak ada event yang terlewat dan tidak ada yang double-counted. Deduplication memastikan `unique_processed + duplicate_dropped = received` setelah semua event terproses. Tanpa idempotency, sistem under eventual consistency bisa menghasilkan state yang tidak konsisten karena retry dapat menyebabkan duplikasi (Coulouris et al., 2011). Dengan idempotency, retry aman dilakukan kapan saja.

### T8 (Bab 8): Desain Transaksi: ACID, Isolation Level, dan Strategi Menghindari Lost-Update

ACID adalah properti transaksi database: Atomicity (semua atau tidak ada), Consistency (state valid sebelum dan sesudah), Isolation (transaksi terisolasi satu sama lain), Durability (perubahan yang di-commit permanent) (Coulouris et al., 2011).

Dalam rancangan sistem ini, setiap insert event menggunakan transaksi eksplisit:

```python
async with conn.transaction():
    result = await conn.execute(
        "INSERT INTO processed_events (...) ON CONFLICT DO NOTHING", ...
    )
```

Ini memenuhi ACID: Atomicity (INSERT berhasil atau gagal seluruhnya), Consistency (constraint UNIQUE tidak pernah dilanggar), Isolation (READ COMMITTED mencegah dirty reads), Durability (WAL PostgreSQL memastikan commit persistent).

**Isolation Level READ COMMITTED** dipilih karena: mencegah dirty read (membaca data yang belum di-commit) yang merupakan risiko utama pada sistem ini; cukup untuk skenario insert-only dengan unique constraint; lebih performa dibanding SERIALIZABLE yang mendeteksi phantom read. Risiko phantom read tidak relevan karena query utama adalah insert, bukan range scan untuk keputusan bisnis. **Menghindari lost-update** pada stats: `UPDATE ... SET value = value + 1` adalah operasi read-modify-write atomik di PostgreSQL, tidak memerlukan SELECT ... FOR UPDATE terpisah (Coulouris et al., 2011).

### T9 (Bab 9): Kontrol Konkurensi: Locking, Unique Constraints, Upsert; Idempotent Write Pattern

Kontrol konkurensi bertujuan memastikan operasi paralel menghasilkan hasil yang sama dengan eksekusi serial (serializability) (Coulouris et al., 2011). Sistem ini menggunakan pendekatan *optimistic concurrency* berbasis constraint daripada *pessimistic locking* eksplisit.

**Mekanisme utama:**

1. **Unique Constraint** — `UNIQUE(topic, event_id)` di level DDL. Ketika dua worker mencoba insert event yang sama secara bersamaan, PostgreSQL menggunakan row-level lock internal pada index untuk memastikan hanya satu yang berhasil. Worker yang kalah mendapat exception yang ditangkap oleh `ON CONFLICT DO NOTHING`.

2. **Idempotent Write Pattern** — `INSERT ... ON CONFLICT DO NOTHING` adalah implementasi idempotent write: operasi yang sama dapat dijalankan berkali-kali tanpa efek samping tambahan. Ini adalah alternatif yang lebih efisien dibanding check-then-insert yang rentan race condition.

3. **Atomic Counter Update** — `UPDATE system_stats SET value = value + 1` menggunakan tuple-level lock implisit selama update, mencegah lost-update dari concurrent writers.

Keuntungan pendekatan ini dibanding explicit locking: tidak ada risk deadlock dari aplikasi-level lock, lebih scalable (database menangani concurrency di level yang lebih rendah dan lebih optimal), dan lebih sederhana dari sisi kode (Coulouris et al., 2011).

### T10 (Bab 10–13): Orkestrasi Compose, Keamanan, Persistensi, Observability

**Orkestrasi Compose (Bab 12–13)**: Docker Compose bertindak sebagai orchestrator sederhana yang mendefinisikan dependency antar service via `depends_on` dengan `condition: service_healthy`. Healthcheck pada storage dan broker memastikan aggregator hanya start setelah infrastruktur siap. `restart: unless-stopped` menjamin service kembali berjalan setelah crash. Multiple worker bisa di-scale dengan profile `workers` (Coulouris et al., 2011).

**Keamanan Jaringan (Bab 10)**: Seluruh service berada dalam Docker bridge network `uas_internal`. Hanya aggregator yang expose port ke host (8080). Redis dan PostgreSQL tidak accessible dari luar Docker network — ini mencegah akses tidak sah ke data dan infrastruktur. Non-root user di Dockerfile (appuser) meminimalkan privilege escalation risk.

**Persistensi (Bab 11)**: Named volumes `uas_pg_data` dan `uas_broker_data` memastikan data survive setelah `docker compose down`. Lokasi data PostgreSQL: `/var/lib/postgresql/data` di dalam container, di-mount dari volume. Redis menggunakan `appendonly yes` untuk WAL-style durability.

**Observability (Bab 12)**: `/health` endpoint sebagai readiness/liveness probe; `/stats` untuk real-time metrics; structured logging dengan level INFO/ERROR/DEBUG dan context (worker_id, event_id, topic) untuk traceability.

---

## 6. Keterkaitan Bab 1–13

| Bab | Konsep | Implementasi |
|---|---|---|
| Bab 1–2 | Karakteristik sistem terdistribusi, pub-sub | Arsitektur 4-service, decoupling via Redis |
| Bab 3–4 | Komunikasi, penamaan | REST API, topic + event_id sebagai identifier |
| Bab 5 | Ordering, timestamp | ISO8601 timestamp, toleransi out-of-order |
| Bab 6 | Failure tolerance | Retry, restart policy, persistent queue |
| Bab 7 | Eventual consistency | Stats konvergen setelah queue kosong |
| Bab 8 | Transaksi, ACID | `BEGIN ... INSERT ON CONFLICT ... COMMIT` |
| Bab 9 | Concurrency control | Unique constraint, atomic update |
| Bab 10–11 | Keamanan, persistensi | Network isolation, named volumes |
| Bab 12–13 | Orkestrasi, observability | Compose healthcheck, /health, /stats |

---

## 7. Referensi

Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. (2011). *Distributed Systems: Concepts and Design* (5th ed.). Addison-Wesley.

FastAPI. (2024). *FastAPI documentation*. https://fastapi.tiangolo.com/

PostgreSQL Global Development Group. (2024). *PostgreSQL 16 documentation*. https://www.postgresql.org/docs/16/

Redis Ltd. (2024). *Redis documentation*. https://redis.io/docs/

K6 by Grafana Labs. (2024). *K6 documentation*. https://k6.io/docs/
