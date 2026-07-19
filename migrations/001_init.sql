CREATE TYPE catqueue_status AS ENUM ('PENDING', 'PROCESSING', 'COMPLETED', 'DEAD');

CREATE TABLE IF NOT EXISTS catqueue_jobs (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  job_name        TEXT NOT NULL,
  payload         JSONB NOT NULL,
  status          catqueue_status DEFAULT 'PENDING',
  priority        INT DEFAULT 3,
  attempt_count   INT DEFAULT 0,
  max_attempts    INT DEFAULT 5,
  run_at          TIMESTAMPTZ DEFAULT NOW(),
  locked_until    TIMESTAMPTZ,
  worker_id       TEXT,
  idempotency_key TEXT UNIQUE,
  error_log       JSONB,
  created_at      TIMESTAMPTZ DEFAULT NOW(),
  completed_at    TIMESTAMPTZ,
  dependencies    VARCHAR[] DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS job_dependencies (
  id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id     UUID NOT NULL REFERENCES catqueue_jobs(id),
  depends_on UUID NOT NULL REFERENCES catqueue_jobs(id)
);

CREATE INDEX IF NOT EXISTS idx_catqueue_pending
  ON catqueue_jobs (priority ASC, run_at ASC)
  WHERE status = 'PENDING';