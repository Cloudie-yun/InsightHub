CREATE TABLE IF NOT EXISTS study_aids (
    study_aid_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    conversation_id UUID REFERENCES conversations(conversation_id) ON DELETE SET NULL,
    document_id UUID REFERENCES documents(document_id) ON DELETE SET NULL,
    aid_type TEXT NOT NULL CHECK (aid_type IN ('flashcards', 'mindmap')),
    title TEXT NOT NULL DEFAULT '',
    source_requirements TEXT NOT NULL DEFAULT '',
    page_range TEXT NOT NULL DEFAULT '',
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_study_aids_user_type_created
    ON study_aids (user_id, aid_type, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_study_aids_document
    ON study_aids (document_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_study_aids_conversation
    ON study_aids (conversation_id, created_at DESC);
