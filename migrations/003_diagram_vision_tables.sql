BEGIN;

CREATE TABLE IF NOT EXISTS diagram_block_details (
    block_id UUID PRIMARY KEY
        REFERENCES document_blocks(block_id) ON DELETE CASCADE,
    image_asset_id UUID NULL
        REFERENCES document_block_assets(block_asset_id) ON DELETE SET NULL,
    diagram_kind VARCHAR(40) NOT NULL DEFAULT 'unknown',
    image_region JSONB NOT NULL DEFAULT '{}'::jsonb,
    ocr_text JSONB NOT NULL DEFAULT '[]'::jsonb,
    visual_description TEXT,
    semantic_links JSONB NOT NULL DEFAULT '[]'::jsonb,
    question_answerable_facts JSONB NOT NULL DEFAULT '[]'::jsonb,
    vision_status VARCHAR(30) NOT NULL DEFAULT 'pending_vision_analysis'
        CHECK (vision_status IN ('pending_vision_analysis', 'processing', 'completed', 'failed')),
    vision_confidence DOUBLE PRECISION,
    provider_name VARCHAR(40),
    model_name VARCHAR(80),
    prompt_version VARCHAR(40),
    last_analyzed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_diagram_block_details_status
    ON diagram_block_details (vision_status);

CREATE TABLE IF NOT EXISTS diagram_block_analysis_runs (
    analysis_run_id UUID PRIMARY KEY,
    block_id UUID NOT NULL
        REFERENCES document_blocks(block_id) ON DELETE CASCADE,
    provider_name VARCHAR(40) NOT NULL,
    model_name VARCHAR(80) NOT NULL,
    prompt_version VARCHAR(40) NOT NULL,
    request_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    raw_response JSONB,
    parsed_output JSONB,
    status VARCHAR(20) NOT NULL DEFAULT 'processing'
        CHECK (status IN ('processing', 'completed', 'failed')),
    error_message TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_diagram_block_analysis_runs_block_id
    ON diagram_block_analysis_runs (block_id, started_at DESC);

COMMIT;
