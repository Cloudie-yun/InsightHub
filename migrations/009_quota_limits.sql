BEGIN;

CREATE TABLE IF NOT EXISTS quota_limits (
    model_name VARCHAR(120) PRIMARY KEY,
    provider VARCHAR(64) NOT NULL DEFAULT 'gemini',
    rpm_limit BIGINT,
    tpm_limit BIGINT,
    rpd_limit BIGINT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_quota_limits_provider_active
    ON quota_limits (provider, is_active);

INSERT INTO quota_limits (
    model_name,
    provider,
    rpm_limit,
    tpm_limit,
    rpd_limit,
    is_active,
    created_at,
    updated_at
)
VALUES
    ('gemini-2.5-flash', 'gemini', 5, 250000, 20, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('gemini-embedding-001', 'gemini', 100, 30000, 1000, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('gemini-3-flash', 'gemini', 5, 250000, 20, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('gemini-3.1-flash-lite', 'gemini', 15, 250000, 500, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('gemini-2.5-flash-lite', 'gemini', 10, 250000, 20, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('gemini-embedding-002', 'gemini', 100, 30000, 1000, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT (model_name)
DO UPDATE SET
    provider = EXCLUDED.provider,
    rpm_limit = EXCLUDED.rpm_limit,
    tpm_limit = EXCLUDED.tpm_limit,
    rpd_limit = EXCLUDED.rpd_limit,
    is_active = EXCLUDED.is_active,
    updated_at = CURRENT_TIMESTAMP;

COMMIT;
