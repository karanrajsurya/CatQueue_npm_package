import { computeRetryState, updateNewJobs } from "./inQueueProcesses.js";
import { Handler, Job } from "./types.js";
import { Pool } from "pg";

// worker.ts — drop the semaphore param and its release() calls entirely
export const workerThread = async (
  job: Job,
  pool: Pool,
  handlers: Map<string, Handler>,
): Promise<boolean> => {
  const handler = handlers.get(job.job_name);
  try {
    if (!handler) {
      await pool.query(
        `UPDATE catqueue_jobs SET status='PENDING', worker_id=NULL, locked_until=NULL WHERE id=$1`,
        [job.id],
      );
      console.warn(
        `[worker] No handler registered for job name: ${job.job_name}. Set to pending untill handler is registered.`,
      );
      return true;
    }
    await handler(job.payload);
    return true;
  } catch (error: any) {
    const { nextAttempt, isDead, nextRunAt } = computeRetryState(
      job.attempt_count,
      job.max_attempts,
    );
    const newLog = [
      ...(Array.isArray(job.error_log) ? job.error_log : []),
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
      job.id,
    );
    return false;
  }
};
