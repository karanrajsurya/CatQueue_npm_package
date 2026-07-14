import { Pool } from "pg";
import {
  cleanAllCompletedJobs,
  processNextJob,
  recoverStuckJobs,
  setAllCompletedJobsToNull,
} from "../../worker.js";
import { describe, it, expect, beforeAll, afterAll, beforeEach } from "vitest";
import { Handler } from "../../types.js";

const handlersSP = new Map<string, Handler>();
const handlersFP = new Map<string, Handler>();

handlersSP.set("test-job", async (_payload) => {
  return;
});
handlersFP.set("test-job", async (_payload) => {
  throw new Error("The handler failed");
});

const pool = new Pool({
  connectionString: process.env.DATABASE_TEST_URI,
});

// runs once before all tests
beforeAll(async () => {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS catqueue_jobs (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      job_name TEXT NOT NULL,
      payload JSONB NOT NULL,
      status TEXT DEFAULT 'PENDING',
      priority INT DEFAULT 3,
      attempt_count INT DEFAULT 0,
      max_attempts INT DEFAULT 5,
      run_at TIMESTAMPTZ DEFAULT NOW(),
      locked_until TIMESTAMPTZ,
      worker_id TEXT,
      idempotency_key TEXT UNIQUE,
      error_log JSONB,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      completed_at TIMESTAMPTZ
    )
  `);
});

// cleaning table before each test
beforeEach(async () => {
  await pool.query(`DELETE FROM catqueue_jobs`);
});

// close pool after all tests
afterAll(async () => {
  await pool.end();
});

// tests for processing next job
describe("processNextJob tests", () => {
  it("takes enqueues new job with Success Path handler", async () => {
    //     const {
    //        rows: [inserted]
    //     } = result;
    const {
      rows: [inserted],
    } = await pool.query(`
    INSERT INTO catqueue_jobs (job_name, payload, status, run_at)
    VALUES ('test-job', '{}', 'PENDING', NOW() - INTERVAL '1 minute')
    RETURNING id
  `);

    const jobDone = await processNextJob(pool, handlersSP, "worker-1", 60, 1);

    expect(jobDone).toBeTruthy();

    const { rows } = await pool.query(`SELECT * FROM catqueue_jobs`);
    console.log(rows[0]);

    expect(rows[0].status).toBe("COMPLETED");
    expect(rows[0].locked_until).toBeNull();
    expect(rows[0].completed_at).toBeInstanceOf(Date);
  });

  it("takes enqueues new job with Failure Path handler", async () => {
    const {
      rows: [inserted],
    } = await pool.query(`
    INSERT INTO catqueue_jobs (job_name, payload, status, run_at)
    VALUES ('test-job', '{}', 'PENDING', NOW() - INTERVAL '1 minute')
    RETURNING id
  `);

    const jobDone = await processNextJob(pool, handlersFP, "worker-1", 60, 1);

    expect(jobDone).toBeTruthy();

    const { rows } = await pool.query(`SELECT * FROM catqueue_jobs`);

    expect(rows[0].status).toBe("PENDING");
    expect(rows[0].locked_until).toBeNull();
    expect(rows[0].attempt_count).toBe(1);
    expect(rows[0].error_log).toHaveLength(1);
  });

  it("finds all jobs with status COMPLETED and set other attributes to NULL", async () => {
    const result = await pool.query(`
        INSERT INTO catqueue_jobs (job_name, payload, status, run_at, worker_id)
        VALUES ('test-job', '{}', 'PENDING', NOW() - INTERVAL '1 minute', 'active-worker')
        RETURNING id
        `);

    const job_id = result.rows[0].id;

    const completedIds = result.rows.map((r) => r.id);

    await setAllCompletedJobsToNull(pool, completedIds);

    const { rows } = await pool.query(`SELECT * FROM catqueue_jobs`);

    expect(rows[0].worker_id).toBeNull();
    expect(rows[0].locked_until).toBeNull();
    expect(rows[0].status).toBe("COMPLETED");
  });
});

// tests for job recovery function
describe("recoverStuckJobs tests", () => {
  it("resets PROCESSING jobs with expired locks back to PENDING", async () => {
    // insert a stuck job — locked_until is in the past
    await pool.query(`
      INSERT INTO catqueue_jobs (job_name, payload, status, locked_until, worker_id)
      VALUES ('test-job', '{}', 'PROCESSING', NOW() - INTERVAL '1 minute', 'dead-worker')
      RETURNING id
    `);

    await recoverStuckJobs(pool);

    const { rows } = await pool.query(`SELECT * FROM catqueue_jobs`);

    expect(rows[0].status).toBe("PENDING");
    expect(rows[0].locked_until).toBeNull();
    expect(rows[0].worker_id).toBeNull();
  });

  it("does NOT reset PROCESSING jobs with active locks", async () => {
    // locked_until is in the future — should stay PROCESSING
    await pool.query(`
      INSERT INTO catqueue_jobs (job_name, payload, status, locked_until, worker_id)
      VALUES ('test-job', '{}', 'PROCESSING', NOW() + INTERVAL '30 seconds', 'active-worker')
    `);

    await recoverStuckJobs(pool);

    const { rows } = await pool.query(`SELECT * FROM catqueue_jobs`);
    expect(rows[0].status).toBe("PROCESSING");
  });
});

// tests for cron jobs
describe("cron job testing", () => {
  it("runs the cron job", async () => {
    await pool.query(`
      INSERT INTO catqueue_jobs (job_name, payload, status, completed_at)
      VALUES ('test-job', '{}', 'COMPLETED', NOW() - INTERVAL '8 days')
    `);
    const {
      rows: [oldJob],
    } = await pool.query(`
      INSERT INTO catqueue_jobs (job_name, payload, status, completed_at)
      VALUES ('test-job', '{}', 'COMPLETED', NOW() - INTERVAL '6 days')
    `);

    const {
      rows: [recentJob],
    } = await pool.query(`
      SELECT * FROM catqueue_jobs
      WHERE completed_at > NOW() - INTERVAL '7 days'
      `);

    const result = await cleanAllCompletedJobs(pool);
    expect(result.rowCount).toBe(1);

    const { rows } = await pool.query(`SELECT id FROM catqueue_jobs`);
    expect(rows).toHaveLength(1);
    expect(rows[0].id).toBe(recentJob.id);
  });
});
