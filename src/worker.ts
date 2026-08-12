import { Handler, Job } from "./types.js";
import { Pool } from "pg";

export const workerThread = async (
  jobs: Job,
  pool: Pool,
  handlers: Map<string, Handler>,
): Promise<boolean> => {
  const handler = handlers.get(jobs.job_name);

  try {
    if (!handler) {
      console.warn(
        `[WorkerThread] No handler registered for ${jobs.id}. Releasing lock.`,
      );
      await pool.query(
        `
        UPDATE catqueue_jobs
        SET status = 'PENDING', worker_id = NULL, locked_until = NULL
        WHERE id = $1
      `,
        [jobs.id],
      );
      return true;
    }
    // throw new Error(`No handler registered for: ${jobs.job_name}`);
    await handler(jobs.payload);
    return true;
  } catch (error: any) {
    console.error("[workerThread] caught error:", error.message);
    const { nextAttempt, isDead, nextRunAt } = computeRetryState(
      jobs.attempt_count,
      jobs.max_attempts,
    );
    const existingLog = Array.isArray(jobs.error_log) ? jobs.error_log : [];
    const newLog = [
      ...existingLog,
      {
        attempt: nextAttempt,
        error: error.message,
        at: new Date().toISOString(),
      },
    ];

    await updateNewJobs(
      pool,
      isDead ? "DEAD" : "PENDING",
      nextAttempt,
      nextRunAt,
      JSON.stringify(newLog),
      jobs.id,
    );

    return false;
  }
};

export async function updateNewJobs(
  pool: Pool,
  status: string,
  nextAttempt: number,
  newRunAt: Date | null,
  newErrorLog: string,
  jobId: string,
) {
  return await pool.query(
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
    [status, nextAttempt, newRunAt, newErrorLog, jobId],
  );
}

export function computeRetryState(attemptCount: number, maxAttempts: number) {
  const nextAttempt = attemptCount + 1;
  const isDead = nextAttempt >= maxAttempts;
  return {
    nextAttempt,
    isDead,
    nextRunAt: isDead ? null : new Date(Date.now() + 2 ** nextAttempt * 1000),
  };
}
