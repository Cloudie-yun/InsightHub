CREATE TABLE IF NOT EXISTS document_summaries (
    document_id UUID PRIMARY KEY REFERENCES documents(document_id) ON DELETE CASCADE,
    conversation_id UUID REFERENCES conversations(conversation_id) ON DELETE SET NULL,
    source_content_hash VARCHAR(64) NOT NULL,
    source_version VARCHAR(64) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'completed'
        CHECK (status IN ('completed', 'failed')),
    summary_text TEXT,
    title_hint VARCHAR(255),
    summary_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    provider_name VARCHAR(64),
    model_name VARCHAR(128),
    token_count INTEGER,
    error_message TEXT,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS conversation_summaries (
    conversation_id UUID PRIMARY KEY REFERENCES conversations(conversation_id) ON DELETE CASCADE,
    source_content_hash VARCHAR(64) NOT NULL,
    source_version VARCHAR(64) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'completed'
        CHECK (status IN ('completed', 'failed')),
    document_count INTEGER NOT NULL DEFAULT 0,
    summary_text TEXT,
    generated_title VARCHAR(255),
    summary_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    provider_name VARCHAR(64),
    model_name VARCHAR(128),
    token_count INTEGER,
    error_message TEXT,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS document_summary_jobs (
    job_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id UUID NOT NULL REFERENCES documents(document_id) ON DELETE CASCADE,
    conversation_id UUID REFERENCES conversations(conversation_id) ON DELETE CASCADE,
    content_hash VARCHAR(64) NOT NULL,
    content_version VARCHAR(64) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'processing', 'retrying', 'completed', 'failed')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    block_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error_message TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (document_id, content_hash, content_version)
);

CREATE TABLE IF NOT EXISTS conversation_summary_jobs (
    job_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    conversation_id UUID NOT NULL REFERENCES conversations(conversation_id) ON DELETE CASCADE,
    source_document_id UUID REFERENCES documents(document_id) ON DELETE SET NULL,
    content_hash VARCHAR(64),
    content_version VARCHAR(64),
    status VARCHAR(20) NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'processing', 'retrying', 'completed', 'failed')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error_message TEXT,
    trigger VARCHAR(64),
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_document_summaries_conversation_id
    ON document_summaries (conversation_id);

CREATE INDEX IF NOT EXISTS idx_document_summary_jobs_status_next_attempt
    ON document_summary_jobs (status, next_attempt_at, created_at);

CREATE INDEX IF NOT EXISTS idx_document_summary_jobs_conversation_id
    ON document_summary_jobs (conversation_id);

CREATE INDEX IF NOT EXISTS idx_conversation_summary_jobs_status_next_attempt
    ON conversation_summary_jobs (status, next_attempt_at, created_at);

CREATE OR REPLACE FUNCTION touch_summary_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_document_summaries_updated_at ON document_summaries;
CREATE TRIGGER update_document_summaries_updated_at
BEFORE UPDATE ON document_summaries
FOR EACH ROW
EXECUTE FUNCTION touch_summary_updated_at();

DROP TRIGGER IF EXISTS update_conversation_summaries_updated_at ON conversation_summaries;
CREATE TRIGGER update_conversation_summaries_updated_at
BEFORE UPDATE ON conversation_summaries
FOR EACH ROW
EXECUTE FUNCTION touch_summary_updated_at();

DROP TRIGGER IF EXISTS update_document_summary_jobs_updated_at ON document_summary_jobs;
CREATE TRIGGER update_document_summary_jobs_updated_at
BEFORE UPDATE ON document_summary_jobs
FOR EACH ROW
EXECUTE FUNCTION touch_summary_updated_at();

DROP TRIGGER IF EXISTS update_conversation_summary_jobs_updated_at ON conversation_summary_jobs;
CREATE TRIGGER update_conversation_summary_jobs_updated_at
BEFORE UPDATE ON conversation_summary_jobs
FOR EACH ROW
EXECUTE FUNCTION touch_summary_updated_at();
