BEGIN;

CREATE TABLE IF NOT EXISTS conversation_summaries (
    id BIGSERIAL PRIMARY KEY,
    conversation_id UUID NOT NULL UNIQUE REFERENCES conversations(conversation_id) ON DELETE CASCADE,
    title TEXT,
    summary_text TEXT NOT NULL DEFAULT '',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_version TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS conversation_summary_sources (
    id BIGSERIAL PRIMARY KEY,
    conversation_summary_id BIGINT NOT NULL REFERENCES conversation_summaries(id) ON DELETE CASCADE,
    document_summary_id BIGINT REFERENCES document_summaries(id) ON DELETE SET NULL,
    document_id UUID REFERENCES documents(document_id) ON DELETE SET NULL,
    source_rank INTEGER NOT NULL,
    source_excerpt TEXT,
    source_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(conversation_summary_id, source_rank)
);

CREATE INDEX IF NOT EXISTS idx_conversation_summary_sources_summary_id
    ON conversation_summary_sources(conversation_summary_id);

CREATE INDEX IF NOT EXISTS idx_conversation_summary_sources_document_summary_id
    ON conversation_summary_sources(document_summary_id);

COMMIT;
