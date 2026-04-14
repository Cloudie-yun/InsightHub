BEGIN;

CREATE TABLE IF NOT EXISTS embedding_runs (
    run_id BIGSERIAL PRIMARY KEY,
    block_id UUID NOT NULL REFERENCES document_blocks(block_id) ON DELETE CASCADE,
    status VARCHAR(32) NOT NULL,
    error_message TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    model_name VARCHAR(120) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_embedding_runs_status_started_at
    ON embedding_runs (status, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_embedding_runs_block_id_completed_at
    ON embedding_runs (block_id, completed_at DESC);

COMMIT;
