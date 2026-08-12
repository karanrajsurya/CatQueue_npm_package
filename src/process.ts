import { Pool } from "pg";
import { Handler, Job } from "./types.js";
import { workerThread } from "./worker.js";
import { GraphProcess } from "./DagProcess.js";

export const processNextJob = async (
  pool: Pool,
  handlers: Map<string, Handler>,
  workerId: string,
  lockDuration: number,
  batchSize: number,
): Promise<boolean> => {
  const currentJobs = Array.from(handlers.keys());
  if (currentJobs.length === 0) return false;

  const { rows } = await pool.query<Job>(
    `
    UPDATE catqueue_jobs
    SET
      worker_id = $1,
      status = 'PROCESSING',
      locked_until = NOW() + make_interval(secs => $2)
    WHERE id IN (
      SELECT id FROM catqueue_jobs
      WHERE status = 'PENDING'
      AND run_at <= NOW()
      AND job_name = ANY($3::text[])
      ORDER BY priority ASC, created_at ASC
      LIMIT $4
      FOR UPDATE SKIP LOCKED
    )
    RETURNING *;
  `,
    [workerId, lockDuration, currentJobs, batchSize],
  );

  if (rows.length == 0) return false;

  const jobMap = new Map<string, Job>(rows.map((j) => [j.id, j]));

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

  const claimedIds = rows.map((r) => r.id);
  const { executionOrder, cyclicJobs } = await GraphProcess(pool, claimedIds);

  if (cyclicJobs.length > 0) {
    console.log(`${cyclicJobs.length} jobs excluded due to a dependency cycle`);
  }

  const execArr = Array.from(executionOrder);

  const orderedJobs = execArr
    .map((jobId) => jobMap.get(jobId))
    .filter((j): j is Job => j !== undefined);

  const results = await Promise.allSettled(
    orderedJobs.map((job) => workerThread(job, pool, handlers)),
  );

  const resultById = new Map(execArr.map((jobId, i) => [jobId, results[i]]));

  const completedIds = rows
    .filter((r) => {
      const res = resultById.get(r.id);
      return res?.status === "fulfilled" && res.value === true;
    })
    .map((r) => r.id);

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
        WHERE status IN('PENDING', 'PROCESSING', 'DEAD')
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
