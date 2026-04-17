BEGIN;

CREATE TABLE IF NOT EXISTS quota_state (
    project_id VARCHAR(120) NOT NULL,
    model_name VARCHAR(120) NOT NULL,
    window_type VARCHAR(16) NOT NULL CHECK (window_type IN ('rpm', 'rpd', 'tpm')),
    used_count BIGINT NOT NULL DEFAULT 0,
    reset_at TIMESTAMPTZ NOT NULL,
    last_error_at TIMESTAMPTZ,
    last_error_code VARCHAR(64),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (project_id, model_name, window_type)
);

CREATE INDEX IF NOT EXISTS idx_quota_state_model_reset_at
    ON quota_state (model_name, reset_at);

CREATE INDEX IF NOT EXISTS idx_quota_state_project_window
    ON quota_state (project_id, window_type, reset_at);

COMMIT;
