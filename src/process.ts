import { Pool } from "pg";
import { Handler, Job } from "./types.js";
import { workerThread } from "./worker.js";
import {
  insertDependencyEdges,
  claimRunnableJobs,
  setCompletedJobsToNull,
} from "./inQueueProcesses.js";

export const processNextBatch = async (
  pool: Pool,
  handlers: Map<string, Handler>,
  workerId: string,
  lockDuration: number,
  concurrencyLimit: number,
): Promise<boolean> => {
  const jobNames = Array.from(handlers.keys());
  if (jobNames.length === 0) return false;

  const fetchBatchSize = Math.max(concurrencyLimit * 10, 500);
  const flushThreshold = 100;

  let completedIds: string[] = [];
  let isFlushing = false;
  let claimedAny = false;

  const flushCompleted = async (force = false): Promise<void> => {
    if (completedIds.length === 0 || isFlushing) return;
    if (!force && completedIds.length < flushThreshold) return;

    const idsToFlush = completedIds;
    isFlushing = true;
    completedIds = [];

    try {
      await setCompletedJobsToNull(pool, idsToFlush);
    } catch (err) {
      console.error("Error flushing completed jobs:", err);
    } finally {
      isFlushing = false;
    }
  };

  let currentBatch = await claimRunnableJobs(
    pool,
    workerId,
    lockDuration,
    jobNames,
    fetchBatchSize,
  );

  if (currentBatch.length === 0) return false;
  claimedAny = true;
  await insertDependencyEdges(pool, currentBatch);

  let nextBatchPromise: Promise<Job[]> | null = null;
  let index = 0;
  const inProcess = new Set<Promise<void>>();

  while (index < currentBatch.length || inProcess.size > 0) {
    while (inProcess.size < concurrencyLimit && index < currentBatch.length) {
      const job = currentBatch[index++];

      if (currentBatch.length - index < concurrencyLimit && !nextBatchPromise) {
        nextBatchPromise = claimRunnableJobs(
          pool,
          workerId,
          lockDuration,
          jobNames,
          fetchBatchSize,
        );
      }

      const p: Promise<void> = workerThread(job, pool, handlers)
        .then((success) => {
          if (success) completedIds.push(job.id);
        })
        .catch(console.error)
        .then(() => flushCompleted())
        .finally(() => {
          inProcess.delete(p);
        });

      inProcess.add(p);
    }

    if (index >= currentBatch.length && nextBatchPromise) {
      const nextBatch = await nextBatchPromise;
      nextBatchPromise = null;

      if (nextBatch.length > 0) {
        await insertDependencyEdges(pool, nextBatch);
        currentBatch = nextBatch;
        index = 0;
        continue;
      }
    }

    if (inProcess.size > 0) {
      await Promise.race(inProcess);
    }
  }

  await flushCompleted(true);
  return claimedAny;
};
