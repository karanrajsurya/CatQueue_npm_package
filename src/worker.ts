import { Pool } from "pg";
import { randomUUID } from "node:crypto";
import { Handler, Job } from "./types.js";
import { workerThread } from "./threading.js";
import { GraphProcess } from "./DagProcess.js";

export const processNextJob = async (
  pool: Pool,
  id: string,
  jobName: string,
  handlers: Map<string, Handler>,
  workerId: string,
  lockDuration: number,
  batchSize: number,
): Promise<boolean> => {
  const idempotency_key: string = `${id}-${jobName}-${randomUUID()}`;

  const uniqueIdempotencyKey: Promise<boolean> = checkIdempotencyKey(
    pool,
    idempotency_key,
  );
  if (!uniqueIdempotencyKey) {
    console.log(`The job with ID ${id} was found to be duplicate`);
    return false;
  }

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

  // Insert jobs into the job_dependencies table
  const edgeIds: string[] = [];
  const edgeDeps: string[] = [];
  for (const job of rows) {
    for (const dep of job.dependencies ?? []) {
      edgeIds.push(job.id);
      edgeDeps.push(dep);
    }
  }

  if (edgeIds.length > 0) {
    await pool.query(
      `
      INSERT INTO job_dependencies (id, depends_on)
      SELECT * FROM UNNEST($1::uuid[], $2::uuid[])
      ON CONFLICT DO NOTHING
    `,
      [edgeIds, edgeDeps],
    );
  }

  // Make DAG here
  const { executionOrder, cyclicJobs } = await GraphProcess(pool);

  if (executionOrder.size == 0) {
    console.log(
      `${executionOrder.size} jobs were found in deadlock (not executed)`,
    );
  }

  const results = await Promise.allSettled(
    Array.from(executionOrder).map((job) => workerThread(job, pool, handlers)),
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

async function checkIdempotencyKey(pool: Pool, key: string) {
  const result = await pool.query(
    `
    SELECT * FROM catqueue_jobs
    WHERE idempotency_key = $1
  `,
    [key],
  );

  return result.rowCount == 0 ? true : false;
}
