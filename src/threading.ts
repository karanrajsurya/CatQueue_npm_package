import { Handler, Job } from "./types.js";
import { Pool } from "pg";

export const workerThread = async (
  job: Job,
  pool: Pool,
  handlers: Map<string, Handler>,
): Promise<boolean> => {
  const handler = handlers.get(job.job_name);

  try {
    if (!handler) throw new Error(`No handler registered for: ${job.job_name}`);
    await handler(job.payload);
    return true;
  } catch (error: any) {
    const nextAttempt = job.attempt_count + 1;
    const isDead = nextAttempt >= job.max_attempts;
    const existingLog = Array.isArray(job.error_log) ? job.error_log : [];
    const newLog = [
      ...existingLog,
      {
        attempt: nextAttempt,
        error: error.message,
        at: new Date().toISOString(),
      },
    ];

    await pool.query(
      `
      UPDATE catqueue_jobs SET
        status = $1,
        attempt_count = $2,
        run_at = $3,
        locked_until = NULL,
        worker_id = NULL,
        error_log = $4
      WHERE id = $5
    `,
      [
        isDead ? "DEAD" : "PENDING",
        nextAttempt,
        isDead
          ? job.run_at
          : new Date(Date.now() + Math.pow(2, nextAttempt) * 1000),
        JSON.stringify(newLog),
        job.id,
      ],
    );

    return false;
  }
};
