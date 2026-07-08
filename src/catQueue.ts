import { Pool } from "pg";
import { randomUUID } from "crypto";
import { CatQueueConfig, Handler, JobOptions } from "./types.js";
import { processNextJob, recoverStuckJobs, cronJob } from "./worker.js";

const sleep = (ms: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, ms));

export class CatQueue {
  private pool: Pool;
  private handlers: Map<string, Handler> = new Map();
  private running = false;
  private workerPromise?: Promise<void>;
  private workerId: string = randomUUID();
  private pollInterval: number;
  private lockDuration: number;
  private batchSize: number;
  private maxAttempts: number;
  private cron?: ReturnType<typeof cronJob>;

  constructor(config: CatQueueConfig) {
    this.pool = new Pool({ connectionString: config.connectionString });
    this.pollInterval = config.pollInterval ?? 1000;
    this.lockDuration = config.lockDuration ?? 30;
    this.batchSize = config.batchSize ?? 50;
    this.maxAttempts = config.maxAttempts ?? 5;
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

  register<T = any>(jobName: string, handler: Handler<T>): void {
    this.handlers.set(jobName, handler);
  }

  start(): void {
    if (this.running) return;
    this.running = true;

    this.cron = cronJob(this.pool);

    console.log(
      `[catqueue] Worker ${this.workerId} started, polling every ${this.pollInterval}ms`,
    );

    this.workerPromise = (async () => {
      const recoveryInterval = setInterval(() => {
        recoverStuckJobs(this.pool).catch(console.error);
      }, 20000);

      try {
        while (this.running) {
          let didWork = false;

          while (
            await processNextJob(
              this.pool,
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
      }
    })();
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
