BEGIN;

CREATE TABLE IF NOT EXISTS document_blocks (
    block_id UUID PRIMARY KEY,
    document_id UUID NOT NULL REFERENCES documents(document_id) ON DELETE CASCADE,
    conversation_id UUID NULL REFERENCES conversations(conversation_id) ON DELETE SET NULL,
    block_type VARCHAR(20) NOT NULL CHECK (block_type IN ('text', 'table', 'diagram')),
    subtype VARCHAR(40),
    source_unit_type VARCHAR(20) NOT NULL CHECK (source_unit_type IN ('page', 'slide', 'image', 'document')),
    source_unit_index INTEGER NOT NULL DEFAULT 1,
    reading_order INTEGER,
    source_location JSONB NOT NULL DEFAULT '{}'::jsonb,
    bbox JSONB NOT NULL DEFAULT '{}'::jsonb,
    raw_content JSONB NOT NULL DEFAULT '{}'::jsonb,
    normalized_content JSONB NOT NULL DEFAULT '{}'::jsonb,
    display_text TEXT,
    caption_text TEXT,
    caption_block_id UUID NULL,
    source_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    linked_context JSONB NOT NULL DEFAULT '{}'::jsonb,
    confidence DOUBLE PRECISION,
    extraction_status VARCHAR(20) NOT NULL DEFAULT 'success' CHECK (extraction_status IN ('pending', 'success', 'partial', 'failed')),
    embedding_status VARCHAR(20) NOT NULL DEFAULT 'not_ready' CHECK (embedding_status IN ('not_ready', 'ready', 'embedded', 'failed')),
    processing_status VARCHAR(30) NOT NULL DEFAULT 'raw' CHECK (processing_status IN ('raw', 'normalized', 'context_linked', 'retrieval_prepared', 'finalized')),
    parser_name VARCHAR(40),
    parser_version VARCHAR(40),
    dedupe_key VARCHAR(128),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_document_blocks_document_order
    ON document_blocks (document_id, source_unit_index, reading_order);

CREATE INDEX IF NOT EXISTS idx_document_blocks_type
    ON document_blocks (document_id, block_type);

CREATE INDEX IF NOT EXISTS idx_document_blocks_embedding_status
    ON document_blocks (document_id, embedding_status);

CREATE INDEX IF NOT EXISTS idx_document_blocks_dedupe_key
    ON document_blocks (document_id, dedupe_key);

CREATE TABLE IF NOT EXISTS document_block_assets (
    block_asset_id UUID PRIMARY KEY,
    block_id UUID NOT NULL REFERENCES document_blocks(block_id) ON DELETE CASCADE,
    asset_role VARCHAR(40) NOT NULL CHECK (asset_role IN ('diagram_crop', 'page_snapshot', 'table_crop', 'thumbnail')),
    storage_path TEXT NOT NULL DEFAULT '',
    mime_type VARCHAR(128),
    byte_size BIGINT,
    content_hash VARCHAR(128),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_document_block_assets_block_id
    ON document_block_assets (block_id);

CREATE INDEX IF NOT EXISTS idx_document_block_assets_content_hash
    ON document_block_assets (content_hash);

COMMIT;
