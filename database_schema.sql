-- Enable gen_random_uuid() if not already enabled
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Drop trigger function if you want a clean reset (optional)
DROP FUNCTION IF EXISTS update_updated_at_column() CASCADE;

-- ====================
-- USERS TABLE
-- ====================
CREATE TABLE users (
    user_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    username VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,

    auth_provider VARCHAR(20) NOT NULL DEFAULT 'local'
        CHECK (auth_provider IN ('local', 'google')),

    google_sub VARCHAR(255) UNIQUE,
    password_hash TEXT,

    email_verified BOOLEAN NOT NULL DEFAULT false,

    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT provider_min_requirements CHECK (
        (auth_provider = 'local' AND password_hash IS NOT NULL)
        OR
        (auth_provider = 'google' AND google_sub IS NOT NULL)
    )
);

-- ====================
-- USER VERIFICATION TOKENS (multi purpose)
-- ====================
CREATE TABLE user_verification_tokens (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,

    purpose VARCHAR(30) NOT NULL
        CHECK (purpose IN ('password_reset', 'email_verify')),

    token_hash TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    used_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ====================
-- DOCUMENTS TABLE
-- ====================
CREATE TYPE processing_status_enum AS ENUM (
    'pending', 'processing', 'completed', 'failed'
);

CREATE TABLE documents (
    document_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,

    -- File identity
    original_filename   VARCHAR(255) NOT NULL,
    stored_filename     VARCHAR(255) NOT NULL,        -- UUID-based, safe for disk
    storage_path        TEXT NOT NULL,                -- /storage/users/{uid}/documents/
    mime_type           VARCHAR(100) NOT NULL,
    file_extension      VARCHAR(20) NOT NULL,
    file_size_bytes     BIGINT NOT NULL,
    file_hash           VARCHAR(64) NOT NULL,         -- SHA-256 hex

    -- Processing
    processing_status   processing_status_enum NOT NULL DEFAULT 'pending',
    processing_error    TEXT,                         -- nullable, only set on failure

    -- Soft delete
    is_deleted          BOOLEAN NOT NULL DEFAULT false,
    deleted_at          TIMESTAMPTZ,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP

	CONSTRAINT chk_documents_deleted_consistency
	CHECK (
	    (is_deleted = false AND deleted_at IS NULL)
	    OR
	    (is_deleted = true AND deleted_at IS NOT NULL)
	)
);

-- ====================
-- CONVERSATIONS TABLE
-- ====================
CREATE TABLE conversations (
    conversation_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id          UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    title            VARCHAR(255),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ====================
-- CONVERSATIONS DOCUMENTS JUNCTION TABLE
-- ====================
CREATE TABLE conversation_documents (
    conversation_id  UUID NOT NULL REFERENCES conversations(conversation_id) ON DELETE CASCADE,
    document_id      UUID NOT NULL REFERENCES documents(document_id) ON DELETE CASCADE,
    added_at         TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (conversation_id, document_id)
);


-- ====================
-- INDEXES FOR PERFORMANCE
-- ====================
-- Users
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_google_sub ON users(google_sub);
CREATE INDEX idx_users_auth_provider ON users(auth_provider);

-- User Verification Tokens
CREATE INDEX idx_uvt_user_id ON user_verification_tokens(user_id);
CREATE INDEX idx_uvt_purpose ON user_verification_tokens(purpose);
CREATE INDEX idx_uvt_expires_at ON user_verification_tokens(expires_at);
CREATE INDEX idx_uvt_used_at ON user_verification_tokens(used_at);
-- Optional composite index, useful when fetching latest active token for a user
CREATE INDEX idx_uvt_user_purpose_created
ON user_verification_tokens(user_id, purpose, created_at DESC);

-- Documents
CREATE INDEX idx_documents_user_id ON documents(user_id);
CREATE INDEX idx_documents_status  ON documents(processing_status);
CREATE INDEX idx_documents_hash    ON documents(file_hash);
CREATE INDEX idx_documents_active  ON documents(user_id) WHERE is_deleted = false;

-- Conversations
CREATE INDEX idx_conversations_user_id ON conversations(user_id);
-- Conversation Document Join
CREATE INDEX idx_conversation_documents_document_id ON conversation_documents(document_id);

-- ====================
-- AUTO-UPDATE TRIGGER
-- ====================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_users_updated_at
BEFORE UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_documents_updated_at
BEFORE UPDATE ON documents
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_conversations_updated_at
BEFORE UPDATE ON conversations
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();
