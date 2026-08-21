import { Pool } from "pg";
import { Job } from "./types";

export async function claimRunnableJobs(
  pool: Pool,
  workerId: string,
  lockDuration: number,
  jobNames: string[],
  limit: number,
): Promise<Job[]> {
  if (limit <= 0) return [];

  const { rows } = await pool.query<Job>(
    //check this query
    `
    WITH target_jobs AS (
      SELECT c.id FROM catqueue_jobs c
      WHERE c.status = 'PENDING'
        AND c.run_at <= NOW()
        AND c.job_name = ANY($3::text[])
        AND (
          c.dependencies IS NULL
          OR cardinality(c.dependencies) = 0
          OR NOT EXISTS (
            SELECT 1 FROM job_dependencies jd
            JOIN catqueue_jobs dep ON dep.id = jd.depends_on
            WHERE jd.job_id = c.id AND dep.status <> 'COMPLETED'
          )
        )
      ORDER BY c.priority ASC, c.created_at ASC
      LIMIT $4
      FOR UPDATE SKIP LOCKED
    )
    UPDATE catqueue_jobs cj
    SET worker_id = $1,
        status = 'PROCESSING',
        locked_until = NOW() + ($2 * INTERVAL '1 second')
    FROM target_jobs t
    WHERE cj.id = t.id
    RETURNING cj.*;
    `,
    [workerId, lockDuration, jobNames, limit],
  );

  return rows;
}

export async function insertDependencyEdges(pool: Pool, rows: Job[]) {
  const jobIds: string[] = [];
  const depIds: string[] = [];
  for (const job of rows) {
    for (const dep of job.dependencies ?? []) {
      jobIds.push(job.id);
      depIds.push(dep);
    }
  }
  if (jobIds.length === 0) return;

  await pool.query(
    `
    INSERT INTO job_dependencies (job_id, depends_on)
    SELECT * FROM UNNEST($1::uuid[], $2::uuid[])
    ON CONFLICT DO NOTHING
    `,
    [jobIds, depIds],
  );
}

export async function cleanCompletedJobs(pool: Pool) {
  return pool.query(`
        DELETE FROM catqueue_jobs
        WHERE status = 'COMPLETED'
        AND completed_at < NOW() - INTERVAL '7 days'
    `);
}

export async function getIncommpleteJobs(pool: Pool) {
  return pool.query(`
        SELECT * FROM catqueue_jobs
        WHERE status IN('PENDING', 'PROCESSING', 'DEAD')
    `);
}

export async function setCompletedJobsToNull(
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
        completed_at = Now(),
        idempotency_key = NULL
      WHERE id = ANY($1::uuid[])
    `,
    [completedIds],
  );
}

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
