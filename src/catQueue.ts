import { Pool } from "pg";
import { randomUUID } from "crypto";
import {
  cronJobHandler,
  deleteStaleIdempotencyKeys,
} from "./delayedProcesses.js";
import { processNextJob } from "./worker.js";
import { recoverStuckJobs } from "./delayedProcesses.js";
import {
  CatQueueConfig,
  Handler,
  JobOptions,
  Job,
  StatsOptions,
  StatsObject,
} from "./types.js";

const sleep = (ms: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, ms));

export class CatQueue {
  private pool: Pool;
  private id: string;
  private job_name: string;
  private handlers: Map<string, Handler> = new Map();
  private running = false;
  private workerPromise?: Promise<void>;
  private workerId: string = randomUUID();
  private pollInterval: number;
  private lockDuration: number;
  private batchSize: number;
  private maxAttempts: number;
  private cron?: ReturnType<typeof cronJobHandler>;
  private dependencies?: string[];

  constructor(config: CatQueueConfig) {
    this.pool = new Pool({ connectionString: config.connectionString });
    this.pollInterval = config.pollInterval ?? 1000;
    this.lockDuration = config.lockDuration ?? 30;
    this.batchSize = config.batchSize ?? 50;
    this.maxAttempts = config.maxAttempts ?? 5;
    this.dependencies = config.dependencies ?? [];
    this.id = randomUUID();
    this.job_name = "";
  }

  async enqueue<T = any>(
    jobName: string,
    payload: T,
    options: JobOptions = {},
  ): Promise<string> {
    const { rows } = await this.pool.query(
      `
      INSERT INTO catqueue_jobs (job_name, payload, priority, max_attempts, run_at, idempotency_key)
      VALUES ($1, $2, $3, $4, $5, $6)
      RETURNING id
    `,
      [
        jobName,
        JSON.stringify(payload),
        options.priority ?? 3,
        options.maxAttempts ?? 5,
        options.runAt ?? new Date(),
        options.idempotencyKey ?? null,
      ],
    );
    return rows[0].id;
  }

  async enqueueBatch<T = any>(
    jobs: { jobName: string; payload: T; options?: JobOptions }[],
  ): Promise<string[]> {
    const jobNames = jobs.map((j) => j.jobName);
    const payloads = jobs.map((j) => JSON.stringify(j.payload));
    const priorities = jobs.map((j) => j.options?.priority ?? 3);
    const maxAttempts = jobs.map((j) => j.options?.maxAttempts ?? 5);
    const runAts = jobs.map((j) => j.options?.runAt ?? new Date());
    const idempotencyKeys = jobs.map((j) => j.options?.idempotencyKey ?? null);

    const { rows } = await this.pool.query(
      `
    INSERT INTO catqueue_jobs (job_name, payload, priority, max_attempts, run_at, idempotency_key)
    SELECT * FROM UNNEST(
      $1::text[], $2::jsonb[], $3::int[], $4::int[], $5::timestamptz[], $6::text[]
    )
    RETURNING id
    `,
      [jobNames, payloads, priorities, maxAttempts, runAts, idempotencyKeys],
    );
    return rows.map((r) => r.id);
  }

  register<T = any>(jobName: string, handler: Handler<T>): void {
    this.handlers.set(jobName, handler);
  }

  start(): void {
    if (this.running) return;
    this.running = true;

    this.cron = cronJobHandler(this.pool);

    console.log(
      `[catqueue] Worker ${this.workerId} started, polling every ${this.pollInterval}ms`,
    );

    this.workerPromise = (async () => {
      const recoveryInterval = setInterval(() => {
        recoverStuckJobs(this.pool).catch(console.error);
      }, 20000);

      const staleIdempotencyKeys = setInterval(() => {
        deleteStaleIdempotencyKeys(this.pool).catch(console.error);
      }, 3000);

      try {
        while (this.running) {
          let didWork = false;

          while (
            await processNextJob(
              this.pool,
              this.id,
              this.job_name,
              this.handlers,
              this.workerId,
              this.lockDuration,
              this.batchSize,
            )
          ) {
            didWork = true;
          }

          if (!didWork) {
            await sleep(this.pollInterval);
          }
        }
      } finally {
        clearInterval(recoveryInterval);
        clearInterval(staleIdempotencyKeys);
      }
    })();
  }

  stats(): StatsQuery {
    return new StatsQuery(this.pool);
  }

  async stop(): Promise<void> {
    this.running = false;

    this.cron?.stop();

    if (this.workerPromise) {
      await this.workerPromise;
    }

    await this.pool.end();
  }
}

class StatsQuery {
  constructor(private pool: Pool) {}

  async overview(): Promise<StatsOptions> {
    const result = await this.pool.query<StatsObject>(
      `SELECT status, COUNT(*)::int as count FROM catqueue_jobs GROUP BY status`,
    );
    return { stats: result.rows };
  }

  async failureRate(
    window: `${number} ${"min" | "hour" | "day"}`,
  ): Promise<number> {
    const result = await this.pool.query<{ rate: number }>(
      `SELECT
         COUNT(*) FILTER (WHERE status = 'DEAD')::float
         / NULLIF(COUNT(*), 0) as rate
       FROM catqueue_jobs
       WHERE created_at > now() - $1::interval`,
      [window],
    );
    return result.rows[0]?.rate ?? 0;
  }

  async retryCount(jobId: string): Promise<number> {
    const result = await this.pool.query<{ attempt_count: number }>(
      `SELECT attempt_count FROM catqueue_jobs WHERE id = $1`,
      [jobId],
    );
    return result.rows[0]?.attempt_count ?? 0;
  }

  async deadJobs(): Promise<Job[]> {
    const result = await this.pool.query<Job>(
      `SELECT * FROM catqueue_jobs WHERE status = 'DEAD'`,
    );
    return result.rows;
  }
}
