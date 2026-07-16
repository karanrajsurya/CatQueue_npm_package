import { Pool } from "pg";
import { Edge } from "./types.js";

export async function GraphProcess(pool: Pool) {
  const result_jobIds = await pool.query(
    `
    WITH RECURSIVE deps AS (
      SELECT id FROM catqueue_jobs
      WHERE SKIP LOCKED, status = 'Pending'

      UNION

      SELECT jd.id, jd,depend_on
      FROM job_dependencies jd
      JOIN deps d ON jd.id = d.depends_on
    )
      SELECT DISTINCT jd.id, jd.depends_on, c.status
      FROM deps jd
      JOIN catqueue_jobs c ON c.id = jd.depends_on
    `,
  );

  const jobIds: string[] = result_jobIds.rows.map((row) => row.id);
  if (jobIds.length == 0)
    return { executionOrder: new Set<string>(), cyclicJobs: [] };

  const result_edges = await pool.query<Edge>(
    `
    SELECT jd.id, jd.depends_on
    FROM job_dependencies jd
    WHERE jd.id = ANY($1)
    AND jd.depends_on = ANY($1)
  `,
    [jobIds],
  );

  const result_blocked = await pool.query<{ id: string }>(
    `
    SELECT DISTINCT jd.id
    FROM job_dependencies jd
    JOIN catqueue_jobs dep ON dep.id = jd.depends_on
    WHERE jd.id = ANY($1)
    AND dep.status != 'COMPLETED'
    AND jd.depends_on != ALL($1)
  `,
    [jobIds],
  );

  const blockedIds = new Set(result_blocked.rows.map((r) => r.id));
  const runnableIds = jobIds.filter((id) => !blockedIds.has(id));
  const edges: Edge[] = result_edges.rows;

  const { adj, indegree } = buildGraph(runnableIds, edges);

  // Kahns algorithm
  const inDeg = new Map(indegree);
  let head = 0;
  const execOrder: string[] = [];
  const queue: string[] = jobIds.filter((id) => inDeg.get(id) === 0);

  while (head < queue.length) {
    let node = queue[head++];
    execOrder.push(node);

    for (const dependent of adj.get(node)!) {
      const newDeg = inDeg.get(dependent)! - 1;
      inDeg.set(dependent, newDeg);
      if (newDeg === 0) queue.push(dependent);
    }
  }

  const executionOrder = new Set(execOrder);
  const cyclicJobs: string[] = jobIds.filter((job) => !executionOrder.has(job));

  return { executionOrder, cyclicJobs };
}

function buildGraph(jobIds: string[], edges: Edge[]) {
  const adj = new Map<string, string[]>();
  const indegree = new Map<string, number>();

  for (const id of jobIds) {
    adj.set(id, []);
    indegree.set(id, 0);
  }

  for (const { id, depends_on } of edges) {
    adj.get(depends_on)!.push(id);
    indegree.set(id, indegree.get(id)! + 1);
  }

  return { adj, indegree };
}
