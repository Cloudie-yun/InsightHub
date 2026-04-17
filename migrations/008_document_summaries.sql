BEGIN;

CREATE TABLE IF NOT EXISTS document_summaries (
    id BIGSERIAL PRIMARY KEY,
    document_id UUID NOT NULL UNIQUE REFERENCES documents(document_id) ON DELETE CASCADE,
    conversation_id UUID REFERENCES conversations(conversation_id) ON DELETE SET NULL,
    summary_text TEXT NOT NULL DEFAULT '',
    highlights JSONB NOT NULL DEFAULT '[]'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_content_hash TEXT,
    source_content_version TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_document_summaries_conversation_id
    ON document_summaries(conversation_id);

CREATE TABLE IF NOT EXISTS document_summary_sources (
    id BIGSERIAL PRIMARY KEY,
    document_summary_id BIGINT NOT NULL REFERENCES document_summaries(id) ON DELETE CASCADE,
    document_id UUID NOT NULL REFERENCES documents(document_id) ON DELETE CASCADE,
    block_id UUID REFERENCES document_blocks(block_id) ON DELETE SET NULL,
    source_rank INTEGER NOT NULL,
    source_snippet TEXT,
    source_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(document_summary_id, source_rank)
);

CREATE INDEX IF NOT EXISTS idx_document_summary_sources_document_summary_id
    ON document_summary_sources(document_summary_id);

CREATE INDEX IF NOT EXISTS idx_document_summary_sources_document_id
    ON document_summary_sources(document_id);

COMMIT;
