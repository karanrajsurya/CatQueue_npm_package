import { Handler, Job } from "./types.js";
import { Pool } from "pg";
import { workerThread } from "./threading.js";
import { CronJob } from "cron";

export const processNextJob = async (
  pool: Pool,
  handlers: Map<string, Handler>,
  workerId: string,
  lockDuration: number,
  batchSize: number,
): Promise<boolean> => {
  const { rows } = await pool.query<Job>(
    `
    UPDATE catqueue_jobs
    SET
      worker_id = $1,
      status = 'PROCESSING',
      locked_until = NOW() + make_interval(secs => $3)
    WHERE id IN (
      SELECT id FROM catqueue_jobs
      WHERE status = 'PENDING'
      AND run_at <= NOW()
      ORDER BY priority ASC, created_at ASC
      LIMIT $2
      FOR UPDATE SKIP LOCKED
    )
    RETURNING *
  `,
    [workerId, batchSize, lockDuration],
  );

  if (rows.length == 0) return false;

  const results = await Promise.allSettled(
    rows.map((job) => workerThread(job, pool, handlers)),
  );

  const completedIds = rows
    .filter(
      (_, i) => results[i].status === "fulfilled" && results[i].value === true,
    )
    .map((job) => job.id);

  // single batch UPDATE for all completed jobs
  if (completedIds.length > 0) {
    await setAllCompletedJobsToNull(pool, completedIds);
  }

  return true;
};

export const recoverStuckJobs = async (pool: Pool): Promise<void> => {
  await pool.query(`
    UPDATE catqueue_jobs
    SET status = 'PENDING', locked_until = NULL, worker_id = NULL
    WHERE status = 'PROCESSING' AND locked_until < NOW()
  `);
};

export const cronJob = (pool: Pool) =>
  new CronJob(
    "0 5 * * 1",
    async function () {
      const result = await cleanAllCompletedJobs(pool);
      console.log(
        `[catqueue] Cleanup: removed ${result.rowCount} completed jobs`,
      );
    },
    null, // on completed - set null
    true, // start automatically
    "Asia/Kolkata",
  );

// helper functinons

export async function cleanAllCompletedJobs(pool: Pool) {
  return pool.query(`
        DELETE FROM catqueue_jobs
        WHERE status = 'COMPLETED'
        AND completed_at < NOW() - INTERVAL '7 days'
    `);
}

export async function getAllIncommpleteJobs(pool: Pool) {
  return pool.query(`
        SELECT * FROM catqueue_jobs
        WHERE status = 'PENDING' OR status = 'PROCESSING' OR status = 'DEAD'
    `);
}

export async function setAllCompletedJobsToNull(
  pool: Pool,
  completedIds: string[],
) {
  return await pool.query(
    `
      UPDATE catqueue_jobs
      SET
        status = 'COMPLETED',
        locked_until = NULL,
        worker_id = NULL,
        completed_at = Now()
      WHERE id = ANY($1::uuid[])
    `,
    [completedIds],
  );
}
