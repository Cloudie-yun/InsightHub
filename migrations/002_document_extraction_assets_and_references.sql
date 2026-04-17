BEGIN;

CREATE TABLE IF NOT EXISTS document_extraction_assets (
    extraction_asset_id BIGSERIAL PRIMARY KEY,
    document_id UUID NOT NULL REFERENCES document_extractions(document_id) ON DELETE CASCADE,
    asset_index INTEGER NOT NULL,
    asset_id VARCHAR(128),
    asset_type VARCHAR(40) NOT NULL,
    storage_path TEXT NOT NULL DEFAULT '',
    upload_path TEXT NOT NULL DEFAULT '',
    original_zip_path TEXT NOT NULL DEFAULT '',
    mime_type VARCHAR(128),
    byte_size BIGINT,
    content_hash VARCHAR(128),
    source_index INTEGER,
    bbox JSONB NOT NULL DEFAULT '[]'::jsonb,
    caption_segment_id VARCHAR(128),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(document_id, asset_index)
);

CREATE INDEX IF NOT EXISTS idx_document_extraction_assets_document_id
    ON document_extraction_assets (document_id, asset_index);

CREATE INDEX IF NOT EXISTS idx_document_extraction_assets_content_hash
    ON document_extraction_assets (document_id, content_hash);

CREATE TABLE IF NOT EXISTS document_extraction_references (
    extraction_reference_id BIGSERIAL PRIMARY KEY,
    document_id UUID NOT NULL REFERENCES document_extractions(document_id) ON DELETE CASCADE,
    reference_index INTEGER NOT NULL,
    reference_id VARCHAR(160),
    source_segment_id VARCHAR(128),
    reference_kind VARCHAR(40) NOT NULL,
    reference_label TEXT NOT NULL DEFAULT '',
    target_segment_id VARCHAR(128),
    target_asset_id VARCHAR(128),
    normalized_target_key VARCHAR(160) NOT NULL DEFAULT '',
    confidence DOUBLE PRECISION,
    resolution_status VARCHAR(40),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(document_id, reference_index)
);

CREATE INDEX IF NOT EXISTS idx_document_extraction_references_document_id
    ON document_extraction_references (document_id, reference_index);

CREATE INDEX IF NOT EXISTS idx_document_extraction_references_target_key
    ON document_extraction_references (document_id, normalized_target_key);

COMMIT;
