BEGIN;

CREATE TABLE IF NOT EXISTS user_prompt_profiles (
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    prompt_type VARCHAR(32) NOT NULL CHECK (prompt_type IN ('qna', 'vision')),
    prompt_text TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, prompt_type)
);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'users'
          AND column_name = 'custom_system_prompt'
    ) THEN
        INSERT INTO user_prompt_profiles (user_id, prompt_type, prompt_text)
        SELECT user_id, 'qna', COALESCE(custom_system_prompt, '')
        FROM users
        WHERE COALESCE(custom_system_prompt, '') <> ''
        ON CONFLICT (user_id, prompt_type) DO NOTHING;
    END IF;
END $$;

COMMIT;
