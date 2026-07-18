import { Pool } from "pg";
import { CronJob } from "cron";
import { cleanAllCompletedJobs } from "./worker";

export async function deleteStaleIdempotencyKeys(pool: Pool) {
  await pool.query(`
        UPDATE catqueue_jobs
        SET
            idempotency_key = NULL
        WHERE
            (status = 'DEAD' OR status = 'COMPLETED')
            AND (completed_at < Now() - INTERVAL '1 minute' OR completed_at IS NULL)
    `);
}

export const recoverStuckJobs = async (pool: Pool): Promise<void> => {
  await pool.query(`
    UPDATE catqueue_jobs
    SET status = 'PENDING', locked_until = NULL, worker_id = NULL
    WHERE status = 'PROCESSING' AND locked_until < NOW()
  `);
};

export const cronJobHandler = (pool: Pool) =>
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
