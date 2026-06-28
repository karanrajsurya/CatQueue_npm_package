import { Handler, Job } from "./types.js";
import { Pool } from "pg";
import { workerThread } from "./threading.js";

const chunkify = (array: string[], chunk: number) => {
  const arrayCopy: string[] = [...array];
  let chunks = [];
  for (var i = chunk; i > 0; i--) {
    chunks.push(arrayCopy.splice(0, Math.ceil(array.length / i)));
  }

  return chunks;
};

export const processNextJob = async (
  pool: Pool,
  handlers: Map<string, Handler>,
  workerId: string,
  lockDuration: number,
  batchSize: number,
): Promise<Boolean> => {
  const { rows } = await pool.query<Job>(
    `
    UPDATE catqueue_jobs
    SET status = 'PROCESSING',
        locked_until = NOW() + INTERVAL '${lockDuration} seconds',
        worker_id = $1
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
    [workerId, batchSize],
  );

  if (rows.length == 0) return false;

  const results = await Promise.all(
    rows.map((job) => workerThread(job, pool, handlers)),
  );

  const completedIds = rows
    .filter((_, i) => results[i] === true)
    .map((job) => job.id);

  // single batch UPDATE for all completed jobs
  if (completedIds.length > 0) {
    await pool.query(
      `
      UPDATE catqueue_jobs
      SET status = 'COMPLETED', locked_until = NULL, worker_id = NULL
      WHERE id = ANY($1::uuid[])
    `,
      [completedIds],
    );
  }

  return true;

  //   const handler = handlers.get(job.job_name);

  //   if (isMainThread) {
  //     const chunks = chunkify(completedIds, concurrencyThreads);
  //     chunks.forEach(() => {
  //       const worker = new Worker("./threading.ts", {});
  //     });
  //   }
  // }
};

export const recoverStuckJobs = async (pool: Pool): Promise<void> => {
  await pool.query(`
    UPDATE catqueue_jobs
    SET status = 'PENDING', locked_until = NULL, worker_id = NULL
    WHERE status = 'PROCESSING' AND locked_until < NOW()
  `);
};
