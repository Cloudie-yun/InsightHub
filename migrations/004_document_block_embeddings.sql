BEGIN;

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS document_block_embeddings (
    block_id UUID PRIMARY KEY
        REFERENCES document_blocks(block_id) ON DELETE CASCADE,
    model_name VARCHAR(120) NOT NULL,
    embedding vector(1536) NOT NULL,
    embedding_dim INT NOT NULL DEFAULT 1536 CHECK (embedding_dim = 1536),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Keep embedding lifecycle state on document_blocks.embedding_status
-- (ready -> embedded / failed) as the source-of-truth.

CREATE INDEX IF NOT EXISTS idx_document_block_embeddings_embedding_ivfflat
    ON document_block_embeddings USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);

CREATE INDEX IF NOT EXISTS idx_document_block_embeddings_model_name
    ON document_block_embeddings (model_name);

COMMIT;
