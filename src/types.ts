export type Handler<T = any> = (payload: T) => Promise<void>;

export interface Edge {
  id: string;
  depends_on: string;
}

export interface JobOptions {
  priority?: number;
  maxAttempts?: number;
  runAt?: Date;
  idempotencyKey?: string;
}

export interface CatQueueConfig {
  connectionString: string;
  pollInterval?: number;
  lockDuration?: number;
  batchSize?: number;
  maxAttempts?: number;
  dependencies?: string[];
}

export interface Job {
  id: string;
  job_name: string;
  payload: any;
  status: "PENDING" | "PROCESSING" | "COMPLETED" | "DEAD";
  priority: number;
  attempt_count: number;
  max_attempts: number;
  run_at: Date;
  locked_until: Date | null;
  worker_id: string | null;
  idempotency_key: string | null;
  error_log: any[];
  dependencies: string[];
  created_at: Date;
  completed_at: Date;
}
