# catqueue

> A Redis-free, PostgreSQL-native job queue for Node.js.

Most job queues require Redis as a broker. catqueue doesn't. If you're already running PostgreSQL, you have everything you need — one table, one migration, three methods.

In this README:

- [Why catqueue?](#why-catqueue)
- [Benchmark](#benchmark)
- [Quick Start](#quick-start)
- [API Reference](#api-reference)
- [Automatic Cleanup](<#automatic-cleanup-(built-in-cron)>)
- [Job Dependencies (DAG Execution)](<#job-dependencies-(dag-execution)-work-in-progress>)
- [Job Lifecycle](#job-lifecycle)
- [Retry Schedule](#retry-schedule)
- [Error Logging](#error-logging)
- [TypeScript](#typeScript)
- [When to use catqueue vs BullMQ](#when-to-use-catqueue-vs-bullmq)
- [Requirements](#requirements)

```bash
npm install catqueue
```

---

## Why catqueue?

| Feature               | catqueue                        | BullMQ           |
| --------------------- | ------------------------------- | ---------------- |
| Broker required       | PostgreSQL only                 | Redis required   |
| Idempotency keys      | ✅ Built-in (DB constraint)     | ❌ Manual        |
| Per-attempt error log | ✅ JSON array in Postgres       | ❌               |
| Dead-letter + replay  | ✅                              | ✅               |
| Atomic job locking    | `SELECT FOR UPDATE SKIP LOCKED` | Redis SETNX      |
| Crash recovery        | ✅ Visibility timeout           | ✅               |
| Batch concurrency     | ✅ `Promise.all` per batch      | ❌ One at a time |
| TypeScript support    | ✅ Full generics                | ✅               |
| Queryable job history | ✅ Plain SQL forever            | ❌ Redis expiry  |

---

## Benchmark

Measured on localhost — no network latency. BullMQ connected to Upstash Redis (remote).

**Machine**

```
CPU:      Intel Core i9-13900H (14 cores, 20 threads)
RAM:      16 GB
OS:       Windows 11
Node:     v22.16.0
Postgres: 18 (localhost)
Redis:    Upstash (remote)
catqueue: 1.1.1
BullMQ:   5.79.1
```

**Scenario**

```
Jobs:        100
Batch size:  50 (catqueue default)
Workers:     1
Handler:     50ms simulated I/O per job (setTimeout)
Payload:     { index: N }
```

**Results**

| Queue    | Time    | Throughput      |
| -------- | ------- | --------------- |
| catqueue | 2,340ms | **43 jobs/sec** |
| BullMQ   | 9,611ms | 10 jobs/sec     |

catqueue processes jobs in concurrent batches of 50 via `Promise.all` — while each job waits on I/O, other jobs in the batch run. BullMQ processes one job at a time per worker in this configuration.

**Why catqueue wins on I/O-bound jobs:**

```
catqueue batch of 50 jobs with 50ms I/O each:
  → all 50 start simultaneously via Promise.all
  → total time ≈ 50ms + DB overhead
  → not 50 × 50ms = 2500ms

BullMQ with 1 worker, 50ms handler:
  → job 1 runs (50ms) → job 2 runs (50ms) → ...
  → total time ≈ 100 × 50ms = 5000ms + Redis overhead
```

**Reproduce it yourself:**

```bash
git clone https://github.com/karanrajsurya/CatQueue_npm_package
cd CatQueue_npm_package
npm install
# set DATABASE_URL and REDIS_URL in .env
node benchmark.js
```

---

## Quick Start

### 1. Run the migration

Run `migrations/001_init.sql` against your PostgreSQL database once:

```bash
psql YOUR_CONNECTION_STRING -f node_modules/catqueue/migrations/001_init.sql
```

Or paste it into your database's SQL editor (Neon, Supabase, pgAdmin, etc).

### 2. Use it

```typescript
import { CatQueue } from "catqueue";

const queue = new CatQueue({
  connectionString: process.env.DATABASE_URL!,
});

// Register handlers
queue.register("send-email", async (payload) => {
  await mailer.send({ to: payload.to, subject: payload.subject });
});

queue.register("resize-image", async (payload) => {
  await sharp(payload.url).resize(800).toFile(payload.output);
});

// Start worker
queue.start();

// Enqueue from anywhere in your app
const jobId = await queue.enqueue("send-email", {
  to: "user@example.com",
  subject: "Welcome!",
});
```

---

## API Reference

### `new CatQueue(config)`

```typescript
const queue = new CatQueue({
  connectionString: string,  // required
  pollInterval?: number,     // ms between polls when queue is empty, default: 2000
  lockDuration?: number,     // seconds a job stays locked, default: 30
  batchSize?: number,        // jobs per batch, default: 50
  maxAttempts?: number       // maximum attempts per job, default: 5
});
```

---

### `queue.enqueue(jobName, payload, options?)`

Inserts a job into the queue. Returns the job ID. Durable — job is committed to Postgres before this resolves.

```typescript
const jobId = await queue.enqueue("send-email", { to: "user@example.com" });

// With options
const jobId = await queue.enqueue(
  "send-email",
  { to: "user@example.com" },
  {
    priority: 1, // 1 = urgent, 5 = low. default: 3
    maxAttempts: 3, // default: 5
    runAt: new Date(Date.now() + 60_000), // delay 60 seconds
    idempotencyKey: "welcome-email-user-123", // prevent duplicates
  },
);
```

**Idempotency keys** — if you enqueue a job with an `idempotencyKey` that already exists, the insert is rejected with a unique constraint violation. Prevents duplicate emails, charges, or webhook deliveries. The stale idempotency keys are deleted which are atleast 1 minute old

---

### `queue.register(jobName, handler)`

Registers a handler for a job type. Must be called before `queue.start()`.

```typescript
queue.register<{ to: string; subject: string }>(
  "send-email",
  async (payload) => {
    // payload is fully typed
    await mailer.send({ to: payload.to, subject: payload.subject });
  },
);
```

---

### `queue.start()`

Starts the worker loop. Polls continuously while jobs are available, sleeps for `pollInterval` ms when the queue is empty.

Each poll cycle:

1. Recovers stuck `PROCESSING` jobs with expired locks every 20 seconds → resets to `PENDING`
2. Atomically claims next batch via `SELECT FOR UPDATE SKIP LOCKED`
3. Runs all jobs in batch concurrently via `Promise.all`
4. On success → batch `UPDATE status = COMPLETED`
5. On failure → retry with exponential backoff or move to `DEAD`

---

### `queue.stop()`

Gracefully stops the worker and closes the database pool.

```typescript
process.on("SIGINT", async () => {
  await queue.stop();
  process.exit(0);
});
```

---

## Automatic Cleanup (Built-in Cron)

catqueue runs a built-in weekly cron job every **Monday at 5:00 AM IST** that automatically deletes `COMPLETED` jobs older than 7 days — preventing unbounded table growth without any configuration.

```sql
-- what the cron runs internally
DELETE FROM catqueue_jobs
WHERE status = 'COMPLETED'
AND completed_at < NOW() - INTERVAL '7 days'
```

This runs automatically when you call `queue.start()` and stops when you call `queue.stop()`. No configuration needed.

If you want to keep completed jobs longer for auditing, you can query them before they're cleaned up:

```sql
SELECT * FROM catqueue_jobs
WHERE status = 'COMPLETED'
ORDER BY completed_at DESC;
```

(Soon to be added feature - allow user to manually choose if jobs should disappear or not, if yes, then after how many days)

## Job Dependencies (DAG Execution) — Work in Progress

catqueue is adding support for **job dependency graphs**, so jobs can declare "run only after these other jobs complete" instead of only being ordered by `priority`.

**How it's designed to work:**

- Each job carries a `dependencies: string[]` — the ids of jobs it depends on.
- Before a batch runs, catqueue walks the dependency graph with a recursive query, pulling in every ancestor of the pending batch (not just edges within the current page), so a job never gets scheduled without its full dependency chain considered.
- A job is only eligible to run once every dependency in that chain has a `COMPLETED` status.
- Execution order within a batch is computed with a topological sort (Kahn's algorithm) over the dependency graph, instead of a flat `Promise.all`.
- If a cycle is detected (e.g. A depends on B, B depends on A), the jobs involved are excluded from that run rather than deadlocking the batch.

**Current status:** this lands the core algorithm (graph traversal + topological execution order), but two pieces of wiring aren't complete yet:

- `migrations/001_init.sql` doesn't yet create the `job_dependencies` table or a `dependencies` column on `catqueue_jobs` — you'll need to add that schema yourself if you want to experiment with this before the migration is updated.
- `queue.enqueue()` doesn't yet expose a `dependencies` option in `JobOptions` — for now, dependency edges must be set directly via SQL/DB access rather than through the public API.

Treat this as a preview of the direction, not a ready-to-use feature — a future release will ship the migration update and the `enqueue()` API to make it usable end-to-end without touching the database directly.

---

## Job Lifecycle

```
PENDING → PROCESSING → COMPLETED
               ↓
          (on failure)
               ↓
        attemptCount++
        errorLog.push({ attempt, error, at })
        runAt = now + 2^attemptCount seconds
               ↓
          back to PENDING
               ↓
     (after maxAttempts exceeded)
               ↓
            DEAD
```

Dead jobs are removed from the database after 7 days through cron. Before that, they are queryable, replayable, auditable.

---

## Retry Schedule

It uses exponential backoff algorithm to ensure no job is enqueued simultaneously.

| Attempt | Retry after |
| ------- | ----------- |
| 1       | 2 seconds   |
| 2       | 4 seconds   |
| 3       | 8 seconds   |
| 4       | 16 seconds  |
| 5       | → DEAD      |

---

## Error Logging

Every failed attempt is appended to `error_log` as a JSON array:

```json
[
  { "attempt": 1, "error": "Connection timeout", "at": "2026-06-26T10:00:00Z" },
  { "attempt": 2, "error": "Connection timeout", "at": "2026-06-26T10:00:02Z" },
  { "attempt": 3, "error": "Null pointer", "at": "2026-06-26T10:00:06Z" }
]
```

---

## TypeScript

Full generic support for typed payloads:

```code
interface EmailPayload {
  to: string;
  subject: string;
  body: string;
}

queue.register<EmailPayload>("send-email", async (payload) => {
  // payload.to, payload.subject, payload.body — all typed
});

await queue.enqueue<EmailPayload>("send-email", {
  to: "user@example.com",
  subject: "Hello",
  body: "Welcome!",
});
```

---

## When to use catqueue vs BullMQ

**Use catqueue when:**

- You already have PostgreSQL and don't want to manage Redis
- You need durable, queryable job history (compliance, auditing, billing)
- You need idempotency keys enforced at the database level
- Your workload is under ~500 jobs/minute
- You want simple stack: one database, zero brokers

**Use BullMQ when:**

- You need 10,000+ jobs/minute throughput
- You already have Redis in your stack
- You need sub-10ms job pickup latency (Redis pub/sub vs polling)
- You need advanced features: rate limiting, job flows, repeatable jobs

---

## Requirements

- Node.js 18+
- PostgreSQL 13+ (`gen_random_uuid()` and `SKIP LOCKED` support)

---

## License

MIT © [Karan Raj Surya](https://github.com/karanrajsurya)
