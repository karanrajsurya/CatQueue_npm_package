import { Pool } from "pg";
import { computeRetryState } from "../../threading";
import {
  describe,
  it,
  expect,
  beforeAll,
  afterAll,
  beforeEach,
  vi,
} from "vitest";

const pool = new Pool({
  connectionString: process.env.DATABASE_TEST_URL,
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

describe("computeRetryState", () => {
  it("increments attempt count and stays alive when attempts remain", () => {
    const result = computeRetryState(1, 5);
    expect(result.nextAttempt).toBe(2);
    expect(result.isDead).toBe(false);
    expect(result.nextRunAt).toBeInstanceOf(Date);
  });

  it("marks job dead exactly when nextAttempt reaches maxAttempts", () => {
    const result = computeRetryState(4, 5);
    expect(result.nextAttempt).toBe(5);
    expect(result.isDead).toBe(true);
    expect(result.nextRunAt).toBeNull();
  });

  it("computes exponential backoff for the next attempt", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-01-01T00:00:00Z"));

    const result = computeRetryState(2, 5); // nextAttempt = 3
    expect(result.nextRunAt).toEqual(new Date(Date.now() + 2 ** 3 * 1000));

    vi.useRealTimers();
  });
});
