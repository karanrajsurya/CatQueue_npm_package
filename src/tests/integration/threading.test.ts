import { Pool } from "pg";
import { describe, it, expect, beforeAll, afterAll, beforeEach } from "vitest";
import { workerThread } from "../../threading";
import { Handler, Job } from "../../types";

const handlersFP = new Map<string, Handler>();

const pool = new Pool({
  connectionString: process.env.DATABASE_TEST_URL,
});

handlersFP.set("test-job", async (_payload) => {
  throw new Error("The handler failed");
});

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

beforeEach(async () => {
  await pool.query(`
        DELETE FROM catqueue_jobs
        `);
});

afterAll(async () => {
  await pool.end();
});

describe("Worker thread test", () => {
  it("declares the job dead", async () => {
    const {
      rows: [job],
    } = await pool.query(`
    INSERT INTO catqueue_jobs (job_name, payload, status, attempt_count, max_attempts)
    VALUES ('test-job', '{}', 'PENDING', 4, 5)
    RETURNING *
  `);

    const jobDone = await workerThread(job, pool, handlersFP);

    expect(jobDone).toBeFalsy();

    const { rows } = await pool.query(
      `SELECT * FROM catqueue_jobs WHERE id = $1`,
      [job.id],
    );

    expect(rows[0].status).toBe("DEAD");
    expect(rows[0].run_at).toBeNull();
    expect(rows[0].attempt_count).toBe(5);
  });
});
