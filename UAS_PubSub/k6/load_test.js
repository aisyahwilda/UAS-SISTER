/**
 * K6 Load Test - Pub-Sub Log Aggregator Terdistribusi
 * =====================================================
 * Mengikuti pola resmi K6 (https://github.com/grafana/k6)
 *
 * Skenario:
 *   1. smoke   — sanity check (1 VU, 30s)
 *   2. load    — beban normal (ramp ke 50 VU, 4 menit)
 *   3. stress  — batas sistem (ramp ke 100 VU, 2 menit)
 *   4. soak    — durabilitas (20 VU, 5 menit)
 *
 * Jalankan semua skenario:
 *   k6 run k6/load_test.js
 *
 * Jalankan skenario tertentu:
 *   k6 run --env SCENARIO=smoke k6/load_test.js
 *
 * Custom base URL:
 *   k6 run --env BASE_URL=http://localhost:8080 k6/load_test.js
 */

import http from 'k6/http';
import { check, group, sleep, fail } from 'k6';
import { Counter, Rate, Trend, Gauge } from 'k6/metrics';
import { randomString, randomIntBetween } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

// ─── Konfigurasi ───────────────────────────────────────────────────────────
const BASE_URL   = __ENV.BASE_URL   || 'http://localhost:8080';
const SCENARIO   = __ENV.SCENARIO   || 'all';
const BATCH_SIZE = parseInt(__ENV.BATCH_SIZE || '50');
const TOPICS     = ['sensor-data', 'app-logs', 'security-logs', 'system-events', 'user-activity'];

// ─── Custom Metrics ─────────────────────────────────────────────────────────
const eventsQueued      = new Counter('events_queued_total');
const duplicatesSent    = new Counter('duplicates_sent_total');
const batchErrors       = new Counter('batch_errors_total');
const batchSuccessRate  = new Rate('batch_success_rate');
const batchDuration     = new Trend('batch_duration_ms', true);   // true = percentilesOutput
const queueSizeGauge    = new Gauge('queue_size_current');

// ─── Pool event_id untuk simulasi duplikat (30%) ────────────────────────────
// Setiap VU memiliki pool-nya sendiri; 30% iterasi memilih dari pool ini
const VU_POOL_SIZE = 100; // jumlah event_id unik per VU

// ─── Definisi Skenario ───────────────────────────────────────────────────────
const SCENARIOS = {
    smoke: {
        executor: 'constant-vus',
        vus: 1,
        duration: '30s',
        tags: { scenario: 'smoke' },
        env: { SCENARIO_NAME: 'smoke' },
    },
    load: {
        executor: 'ramping-vus',
        startVUs: 1,
        stages: [
            { duration: '30s', target: 20 },   // warm-up
            { duration: '3m',  target: 50 },   // sustain
            { duration: '30s', target: 0  },   // cool-down
        ],
        tags: { scenario: 'load' },
        env: { SCENARIO_NAME: 'load' },
    },
    stress: {
        executor: 'ramping-vus',
        startVUs: 1,
        stages: [
            { duration: '30s', target: 50  },  // ramp
            { duration: '1m',  target: 100 },  // stress peak
            { duration: '30s', target: 0   },  // cool-down
        ],
        tags: { scenario: 'stress' },
        env: { SCENARIO_NAME: 'stress' },
    },
    soak: {
        executor: 'constant-vus',
        vus: 20,
        duration: '5m',
        tags: { scenario: 'soak' },
        env: { SCENARIO_NAME: 'soak' },
    },
};

// Pilih skenario yang dijalankan
function buildScenarios() {
    if (SCENARIO === 'smoke')  return { smoke:  SCENARIOS.smoke  };
    if (SCENARIO === 'load')   return { load:   SCENARIOS.load   };
    if (SCENARIO === 'stress') return { stress: SCENARIOS.stress };
    if (SCENARIO === 'soak')   return { soak:   SCENARIOS.soak   };
    // default: jalankan smoke + load saja (agar tidak terlalu lama)
    return { smoke: SCENARIOS.smoke, load: SCENARIOS.load };
}

export const options = {
    scenarios: buildScenarios(),

    // ─── Threshold (pass/fail criteria) ────────────────────────────────
    thresholds: {
        // Semua request harus < 2s di P95
        http_req_duration: ['p(95)<2000', 'p(99)<5000'],

        // Request gagal < 5%
        http_req_failed: ['rate<0.05'],

        // Batch success rate > 95%
        batch_success_rate: ['rate>0.95'],

        // Batch duration P95 < 3s
        batch_duration_ms: ['p(95)<3000'],
    },
};

// ─── Helpers ─────────────────────────────────────────────────────────────────
function getRandomTopic() {
    return TOPICS[randomIntBetween(0, TOPICS.length - 1)];
}

// Setiap VU membuat pool event_id saat pertama kali diinisialisasi
let vuPool = null;

function getVuPool() {
    if (!vuPool) {
        vuPool = [];
        for (let i = 0; i < VU_POOL_SIZE; i++) {
            vuPool.push(`k6-vu${__VU}-${randomString(12)}`);
        }
    }
    return vuPool;
}

/**
 * Buat satu event JSON.
 * @param {boolean} forceDuplicate - paksa ambil dari pool (duplikat)
 */
function makeEvent(forceDuplicate = false) {
    const pool = getVuPool();
    let eventId;

    if (forceDuplicate || Math.random() < 0.30) {
        // 30% chance duplikat — ambil dari pool VU ini
        eventId = pool[randomIntBetween(0, pool.length - 1)];
        duplicatesSent.add(1);
    } else {
        eventId = `k6-vu${__VU}-iter${__ITER}-${randomString(8)}`;
    }

    return {
        topic:     getRandomTopic(),
        event_id:  eventId,
        timestamp: new Date().toISOString(),
        source:    `k6-vuser-${__VU}`,
        payload: {
            temperature: (20 + Math.random() * 25).toFixed(2),
            humidity:    (30 + Math.random() * 60).toFixed(2),
            level:       ['INFO', 'WARN', 'ERROR', 'DEBUG'][randomIntBetween(0, 3)],
            vu:          __VU,
            iteration:   __ITER,
        },
    };
}

/**
 * Kirim batch ke POST /publish/batch
 */
function sendBatch(size) {
    const events = [];
    for (let i = 0; i < size; i++) {
        events.push(makeEvent());
    }

    const payload = JSON.stringify({ events });
    const params  = {
        headers: { 'Content-Type': 'application/json' },
        timeout: '30s',
        tags:    { endpoint: 'publish_batch' },
    };

    const start = Date.now();
    const res   = http.post(`${BASE_URL}/publish/batch`, payload, params);
    const ms    = Date.now() - start;

    batchDuration.add(ms);

    const ok = check(res, {
        'POST /publish/batch → 200':     (r) => r.status === 200,
        'response.success === true':     (r) => {
            try { return JSON.parse(r.body).success === true; } catch { return false; }
        },
        'queued > 0':                    (r) => {
            try { return JSON.parse(r.body).queued > 0; } catch { return false; }
        },
        'latency < 2000ms':              () => ms < 2000,
    });

    batchSuccessRate.add(ok);

    if (ok) {
        eventsQueued.add(size);
    } else {
        batchErrors.add(1);
        console.warn(`[VU ${__VU}] Batch error: HTTP ${res.status} | ${res.body.substring(0, 150)}`);
    }

    return ok;
}

/**
 * Cek health endpoint
 */
function checkHealth() {
    const res = http.get(`${BASE_URL}/health`, {
        tags: { endpoint: 'health' },
        timeout: '5s',
    });

    check(res, {
        'GET /health → 200':            (r) => r.status === 200,
        'status === healthy|degraded':  (r) => {
            try {
                const b = JSON.parse(r.body);
                return b.status === 'healthy' || b.status === 'degraded';
            } catch { return false; }
        },
        'workers_active > 0':          (r) => {
            try { return JSON.parse(r.body).workers_active > 0; } catch { return false; }
        },
    });
}

/**
 * Cek stats endpoint dan update gauge queue_size
 */
function checkStats() {
    const res = http.get(`${BASE_URL}/stats`, {
        tags: { endpoint: 'stats' },
        timeout: '5s',
    });

    const ok = check(res, {
        'GET /stats → 200':                     (r) => r.status === 200,
        'stats has received field':             (r) => {
            try { return 'received' in JSON.parse(r.body); } catch { return false; }
        },
        'stats has unique_processed field':     (r) => {
            try { return 'unique_processed' in JSON.parse(r.body); } catch { return false; }
        },
        'stats has duplicate_dropped field':    (r) => {
            try { return 'duplicate_dropped' in JSON.parse(r.body); } catch { return false; }
        },
    });

    if (ok) {
        try {
            const s = JSON.parse(res.body);
            queueSizeGauge.add(s.queue_size || 0);
        } catch {}
    }
}

// ─── Default Function (dipanggil setiap iterasi VU) ─────────────────────────
export default function () {
    const scenarioName = __ENV.SCENARIO_NAME || 'default';

    group('Publish Batch Events', () => {
        sendBatch(BATCH_SIZE);
        sleep(0.05);  // 50ms antar batch agar tidak flood
    });

    // Setiap 10 iterasi, cek health & stats
    if (__ITER % 10 === 0) {
        group('Observability Checks', () => {
            checkHealth();
            checkStats();
        });
    }

    // Rate limiting: pause singkat antar iterasi
    sleep(randomIntBetween(1, 3) * 0.1);
}

// ─── Setup: Verifikasi aggregator ready sebelum test ─────────────────────────
export function setup() {
    console.log(`\n=== K6 Load Test - Pub-Sub Log Aggregator ===`);
    console.log(`Target URL  : ${BASE_URL}`);
    console.log(`Scenario    : ${SCENARIO}`);
    console.log(`Batch size  : ${BATCH_SIZE}`);
    console.log(`Dup rate    : 30%`);
    console.log(`=============================================\n`);

    // Health check sebelum mulai
    const res = http.get(`${BASE_URL}/health`, { timeout: '10s' });
    if (res.status !== 200) {
        fail(`Aggregator tidak ready: HTTP ${res.status}. Pastikan docker compose up sudah berjalan.`);
    }

    try {
        const h = JSON.parse(res.body);
        console.log(`Health check: ${h.status} | DB: ${h.database} | Broker: ${h.broker}`);
    } catch {}

    return { startTime: new Date().toISOString() };
}

// ─── Teardown & Summary Report ───────────────────────────────────────────────
export function teardown(data) {
    console.log(`\nTest selesai. Start: ${data.startTime}, End: ${new Date().toISOString()}`);
}

export function handleSummary(data) {
    // Ambil stats akhir dari aggregator
    const statsRes = http.get(`${BASE_URL}/stats`, { timeout: '10s' });
    let aggStats = {};
    try { aggStats = JSON.parse(statsRes.body); } catch {}

    // Ambil sample audit log
    const auditRes = http.get(`${BASE_URL}/audit-log?limit=5`, { timeout: '5s' });
    let auditSample = [];
    try { auditSample = JSON.parse(auditRes.body).entries || []; } catch {}

    const dur  = data.metrics.http_req_duration?.values;
    const rate = data.metrics.batch_success_rate?.values;

    const summary = {
        test_info: {
            scenario:    SCENARIO,
            base_url:    BASE_URL,
            batch_size:  BATCH_SIZE,
        },
        k6_metrics: {
            total_requests:     data.metrics.http_reqs?.values?.count          || 0,
            avg_duration_ms:    dur?.avg?.toFixed(2)                           || 0,
            p90_duration_ms:    dur?.['p(90)']?.toFixed(2)                     || 0,
            p95_duration_ms:    dur?.['p(95)']?.toFixed(2)                     || 0,
            p99_duration_ms:    dur?.['p(99)']?.toFixed(2)                     || 0,
            req_failed_rate:    data.metrics.http_req_failed?.values?.rate     || 0,
            batch_success_rate: ((rate?.rate || 0) * 100).toFixed(2) + '%',
            events_queued:      data.metrics.events_queued_total?.values?.count|| 0,
            duplicates_sent:    data.metrics.duplicates_sent_total?.values?.count || 0,
            batch_errors:       data.metrics.batch_errors_total?.values?.count || 0,
        },
        aggregator_stats: {
            received:          aggStats.received          || 0,
            unique_processed:  aggStats.unique_processed  || 0,
            duplicate_dropped: aggStats.duplicate_dropped || 0,
            queue_size:        aggStats.queue_size        || 0,
            workers_active:    aggStats.workers_active    || 0,
            topics:            aggStats.topics            || [],
            uptime_formatted:  aggStats.uptime_formatted  || '',
        },
        audit_log_sample: auditSample,
        thresholds_passed: !Object.values(data.metrics).some(m => m.thresholds &&
            Object.values(m.thresholds).some(t => !t.ok)),
    };

    const jsonOutput = JSON.stringify(summary, null, 2);

    console.log('\n========== LOAD TEST SUMMARY ==========');
    console.log(`Batch success rate : ${summary.k6_metrics.batch_success_rate}`);
    console.log(`Avg duration       : ${summary.k6_metrics.avg_duration_ms} ms`);
    console.log(`P95 duration       : ${summary.k6_metrics.p95_duration_ms} ms`);
    console.log(`Events queued      : ${summary.k6_metrics.events_queued}`);
    console.log(`Duplicates sent    : ${summary.k6_metrics.duplicates_sent}`);
    console.log(`--- Aggregator ---`);
    console.log(`Received           : ${summary.aggregator_stats.received}`);
    console.log(`Unique processed   : ${summary.aggregator_stats.unique_processed}`);
    console.log(`Duplicate dropped  : ${summary.aggregator_stats.duplicate_dropped}`);
    console.log(`=======================================\n`);

    return {
        'k6/load_test_result.json': jsonOutput,
        stdout: '\n' + jsonOutput,
    };
}
