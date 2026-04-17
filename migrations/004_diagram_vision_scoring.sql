BEGIN;

ALTER TABLE diagram_block_details
    ADD COLUMN IF NOT EXISTS vision_gate_score DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS vision_gate_reasons JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE diagram_block_details
    DROP CONSTRAINT IF EXISTS diagram_block_details_vision_status_check;

ALTER TABLE diagram_block_details
    ADD CONSTRAINT diagram_block_details_vision_status_check
    CHECK (vision_status IN ('pending_vision_analysis', 'processing', 'completed', 'failed', 'skipped'));

CREATE INDEX IF NOT EXISTS idx_diagram_block_details_gate_score
    ON diagram_block_details (vision_gate_score);

COMMIT;
