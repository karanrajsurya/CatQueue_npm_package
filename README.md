# catqueue

> A Redis-free, PostgreSQL-native job queue for Node.js.

Most job queues require Redis as a broker. catqueue doesn't. If you're already running PostgreSQL, you have everything you need — one table, one migration, no broker to run.

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
- [Known Issues](#known-issues)
- [Requirements](#requirements)

```bash
npm install catqueue
```

---

## Why catqueue?

| Feature               | catqueue                                                                                   | BullMQ                          |
| --------------------- | ------------------------------------------------------------------------------------------ | ------------------------------- |
| Broker required       | PostgreSQL only                                                                            | Redis required                  |
| Idempotency keys      | ⚠️ Built-in by design, currently broken in `enqueue()` — see [Known Issues](#known-issues) | ❌ Manual                       |
| Per-attempt error log | ✅ JSON array in Postgres                                                                  | ❌                              |
| Dead-letter + replay  | ✅                                                                                         | ✅                              |
| Atomic job locking    | `SELECT FOR UPDATE SKIP LOCKED`                                                            | Redis SETNX                     |
| Crash recovery        | ✅ Visibility timeout                                                                      | ✅                              |
| Worker scheduling     | ✅ Chunked prefetch + concurrency-windowed streaming                                       | ❌ One job at a time per worker |
| TypeScript support    | ✅ Full generics                                                                           | ✅                              |
| Queryable job history | ✅ Plain SQL forever                                                                       | ❌ Redis expiry                 |

---

## Benchmark

Four scenarios, three queues, two consecutive runs on the same code — shown side by side rather than averaged, so run-to-run variance is visible instead of hidden.

**Run 1**

| Scenario                                            | catqueue | BullMQ | pg-boss |
| --------------------------------------------------- | -------- | ------ | ------- |
| Sequential Add — 2,500 jobs (jobs/sec)              | 4,095    | 2,641  | 1,507   |
| Parallel Add — 60,000 jobs, batch=1000 (jobs/sec)   | 16,986   | 41,233 | 10,007  |
| Bulk Add — 60,000 jobs, chunk=2000 (jobs/sec)       | 66,453   | 37,174 | 54,586  |
| Processing — 60,000 jobs, concurrency=30 (jobs/sec) | 1,679    | 17,423 | 1,298   |

**Run 2**

| Scenario                                            | catqueue | BullMQ | pg-boss |
| --------------------------------------------------- | -------- | ------ | ------- |
| Sequential Add — 2,500 jobs (jobs/sec)              | 3,219    | 2,697  | 1,568   |
| Parallel Add — 60,000 jobs, batch=1000 (jobs/sec)   | 17,005   | 38,553 | 9,667   |
| Bulk Add — 60,000 jobs, chunk=2000 (jobs/sec)       | 62,424   | 36,499 | 57,426  |
| Processing — 60,000 jobs, concurrency=30 (jobs/sec) | 9,477    | 22,031 | 1,029   |

**Reading this honestly:**

- **catqueue wins Sequential Add and Bulk Add**, consistently, both runs — roughly 1.2–1.7x BullMQ and 1.1–2.6x pg-boss. Single-row and chunked inserts are catqueue's strongest case.
- **BullMQ wins Parallel Add and Processing**, consistently, both runs. Redis's per-op overhead is lower than a Postgres round trip once job counts and concurrency ramp up.
- **catqueue beats pg-boss on every scenario, both runs** — the closest apples-to-apples Postgres-native comparison.
- **catqueue's Processing throughput has high run-to-run variance** — 1,679 → 9,477 jobs/sec, a 5.6x swing on identical code. BullMQ and pg-boss don't show this. Not root-caused yet; likely candidates are connection-pool sizing relative to `concurrency=30` and Postgres plan/buffer-cache warm-up between runs. Don't treat either Processing number as a stable figure yet.

**Reproduce it yourself:**

```bash
git clone https://github.com/karanrajsurya/CatQueue_npm_package
cd CatQueue_npm_package
npm install
# set DATABASE_URL, REDIS_URL in .env
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
  pollInterval?: number,     // ms to sleep when a pass finds no work, default: 1000
  lockDuration?: number,     // seconds a job stays locked, default: 30
  batchSize?: number,        // max concurrent jobs in flight, default: 50
  maxAttempts?: number,      // maximum attempts per job, default: 5
  maxPoolSize?: number,      // pg Pool max connections, default: 20
});
```

---

### `queue.enqueue(jobName, payload, options?)`

Inserts a job into the queue. Returns the job ID. Durable — job is committed to Postgres before this resolves.

```typescript
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

**Idempotency keys** — if you enqueue a job with an `idempotencyKey` that already exists, the insert is rejected with a unique constraint violation. Prevents duplicate emails, charges, or webhook deliveries. Stale idempotency keys are cleared automatically once they're at least 1 minute old.

> **Known issue:** in the current implementation, `enqueue()` does not actually use `options.idempotencyKey` — it always inserts an internally generated, always-unique key instead. Duplicate calls with the same `idempotencyKey` will **not** currently be deduplicated. `enqueueBatch()` is not affected. See [Known Issues](#known-issues).

---

### `queue.enqueueBatch(jobs)`

Inserts many jobs in a single round trip via `UNNEST`. Returns an array of job IDs in insertion order.

```typescript
const jobIds = await queue.enqueueBatch([
  { jobName: "send-email", payload: { to: "a@example.com" } },
  {
    jobName: "send-email",
    payload: { to: "b@example.com" },
    options: { priority: 1, idempotencyKey: "welcome-b" },
  },
]);
```

Prefer this over looping `enqueue()` when adding many jobs at once — it's the difference between the Sequential Add and Bulk Add numbers in the benchmark above.

---

## `queue.register(jobName, handler)`

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

## `queue.start()`

Starts the worker loop. Three background timers run alongside the main loop:

1. **Stuck-job recovery** — every 20 seconds, `PROCESSING` jobs whose lock has expired are reset to `PENDING`.
2. **Stale idempotency-key cleanup** — every 3 seconds, idempotency keys at least ~1 minute old are cleared.
3. **Weekly cleanup cron** — see [Automatic Cleanup](#automatic-cleanup-built-in-cron).

The main loop runs processing passes back-to-back, and only sleeps `pollInterval` ms once a pass finds no work. Each pass:

1. **Claims a chunk, not one job at a time** — `SELECT ... FOR UPDATE SKIP LOCKED`, ordered by priority then creation time, sized `max(concurrency × 10, 500)`. One query locks hundreds of jobs instead of one round trip per job.
2. **Streams claimed jobs into a concurrency-bounded pool.** `batchSize` in the config is really a concurrency limit, not a fixed batch: as soon as one job finishes, the next queued job starts immediately — it doesn't wait for the whole chunk to finish before picking up more work.
3. **Prefetches the next chunk in the background** once the current one is close to drained, so workers rarely go idle waiting on a query.
4. **Batches successful completions.** Job IDs that succeed are buffered and flushed as one `UPDATE ... WHERE id = ANY(...)` once 100 accumulate (or when the chunk fully drains) — success doesn't cost a round trip per job.
5. **Failures are updated individually** — attempt count, backoff `run_at`, and the appended error-log entry differ per job, so these can't be batched the same way. See [Retry Schedule](#retry-schedule).
6. **A job with no registered handler** is reset to `PENDING` (not counted as a failed attempt) and a warning is logged — it waits for a handler to be registered instead of retrying or dying.

## `queue.stop()`

Gracefully stops the worker, the cron, and closes the database pool.

```typescript
process.on("SIGINT", async () => {
  await queue.stop();
  process.exit(0);
});
```

## `queue.stats()`

`stats` is a method, not a property — call it, then chain:

```typescript
await queue.stats().overview(); // job counts grouped by status
await queue.stats().failureRate("6 min"); // DEAD / total ratio, e.g. "6 min", "2 hour", "3 day"
await queue.stats().retryCount(jobId); // attempts made so far for a job
await queue.stats().deadJobs(); // all jobs currently DEAD
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

If you want to keep completed jobs longer for auditing, query them before they're cleaned up:

```sql
SELECT * FROM catqueue_jobs
WHERE status = 'COMPLETED'
ORDER BY completed_at DESC;
```

## Job Dependencies (DAG Execution) — Work in Progress

More of this exists in the codebase than before, but it isn't safe to use yet. Current state:

- `catqueue_jobs.dependencies` and a `job_dependencies` edge table exist and are read by the live claim query.
- `claimRunnableJobs` already excludes a job from being claimed if it has an edge in `job_dependencies` pointing at a dependency that isn't `COMPLETED`.
- **But** edges are only written by `insertDependencyEdges`, which runs _after_ a batch is already claimed. On a job's very first claim attempt, no edge rows exist for it yet, so the dependency check passes vacuously — a job can be marked `PROCESSING` and start running before its dependencies are even represented in the graph. Edge insertion needs to happen before the claim query, not after.
- `dependencies` is currently set once per **queue instance** (`new CatQueue({ dependencies: [...] })`), not per job — every job enqueued via `enqueue()` on that instance gets the same list. `enqueueBatch()` doesn't attach dependencies at all; the column isn't in that insert.
- A topological-sort implementation (Kahn's algorithm, in `GraphProcess.ts`) exists for computing safe execution order and quarantining cyclic jobs, but it isn't called anywhere from the live processing path (`processNextBatch`) — currently dead code.

**Do not rely on this feature yet.** Treat it as a preview of the direction, not a usable feature. Landing it end-to-end needs: edge insertion moved before the claim query, `dependencies` moved to a per-job `enqueue()`/`enqueueBatch()` option, and `GraphProcess` either wired into `processNextBatch` or removed.

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

Exponential backoff, so no job retries at the same instant twice.

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

```typescript
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
- Your workload is dominated by inserts (Sequential/Bulk Add) rather than raw processing throughput
- You want a simple stack: one database, zero brokers

**Use BullMQ when:**

- You need the highest possible Parallel Add or Processing throughput (see [Benchmark](#benchmark))
- You already have Redis in your stack
- You need sub-10ms job pickup latency (Redis pub/sub vs polling)
- You need advanced features: rate limiting, job flows, repeatable jobs

**vs pg-boss:** across every scenario benchmarked above, catqueue is faster, in both runs. pg-boss remains a longer-established, actively maintained option — worth it if you're already invested in it or need functionality catqueue doesn't reliably have yet (working DAG support, for instance, until the issues above are fixed).

---

## Known Issues

- **`enqueue()` ignores `options.idempotencyKey`.** It always generates its own unique key internally, so duplicate-key deduplication does not currently work through `enqueue()`. `enqueueBatch()` is unaffected. Fix is one line: use `options.idempotencyKey ?? <generated fallback>` instead of always generating.
- **Dependency edges are inserted after jobs are claimed**, so a job's first claim never sees its own dependency edges. See [Job Dependencies](<#job-dependencies-(dag-execution)-work-in-progress>).
- **`GraphProcess.ts` (topological sort) is not called anywhere in `process.ts`.** Either wire it into `processNextBatch` or remove it — right now it implies more DAG support than actually ships.
- **Processing throughput shows ~5.6x run-to-run variance** in the current benchmark suite. Not yet root-caused. See [Benchmark](#benchmark).

---

## Requirements

- Node.js 18+
- PostgreSQL 13+ (`gen_random_uuid()` and `SKIP LOCKED` support)

---

## License

MIT © [Karan Raj Surya](https://github.com/karanrajsurya)
