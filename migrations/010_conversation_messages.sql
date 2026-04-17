BEGIN;

CREATE TABLE IF NOT EXISTS conversation_messages (
    message_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    conversation_id UUID NOT NULL REFERENCES conversations(conversation_id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    role VARCHAR(20) NOT NULL CHECK (role IN ('user', 'assistant')),
    message_text TEXT NOT NULL,
    selected_document_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    retrieval_payload JSONB,
    model_provider VARCHAR(50),
    model_name VARCHAR(100),
    prompt_version VARCHAR(50),
    reply_to_message_id UUID REFERENCES conversation_messages(message_id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_conversation_messages_conversation_created
    ON conversation_messages (conversation_id, created_at ASC);

CREATE INDEX IF NOT EXISTS idx_conversation_messages_user_id
    ON conversation_messages (user_id);

CREATE INDEX IF NOT EXISTS idx_conversation_messages_reply_to
    ON conversation_messages (reply_to_message_id);

COMMIT;
