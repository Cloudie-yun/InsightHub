BEGIN;

ALTER TABLE document_blocks
    ADD COLUMN IF NOT EXISTS is_context_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS context_deleted_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS context_deleted_by UUID NULL REFERENCES users(user_id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_document_blocks_context_deleted
    ON document_blocks (document_id, is_context_deleted);

COMMIT;
