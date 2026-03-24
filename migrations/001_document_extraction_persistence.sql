BEGIN;

CREATE TABLE IF NOT EXISTS document_extractions (
    document_id UUID PRIMARY KEY REFERENCES documents(document_id) ON DELETE CASCADE,
    parser_status VARCHAR(20) NOT NULL DEFAULT 'pending' CHECK (parser_status IN ('pending', 'success', 'failed')),
    parser_version VARCHAR(50) NOT NULL,
    extraction_timestamp TIMESTAMPTZ,
    file_type VARCHAR(32),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    errors JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_document_extractions_status
    ON document_extractions (parser_status);

CREATE TABLE IF NOT EXISTS document_extraction_segments (
    extraction_segment_id BIGSERIAL PRIMARY KEY,
    document_id UUID NOT NULL REFERENCES document_extractions(document_id) ON DELETE CASCADE,
    segment_index INTEGER NOT NULL,
    segment_id VARCHAR(128),
    text TEXT NOT NULL,
    source_type VARCHAR(40),
    source_index INTEGER,
    block_index INTEGER,
    paragraph_index INTEGER,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(document_id, segment_index)
);

CREATE INDEX IF NOT EXISTS idx_document_extraction_segments_document_id
    ON document_extraction_segments (document_id, segment_index);

COMMIT;
