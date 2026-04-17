ALTER TABLE users
    ADD COLUMN IF NOT EXISTS custom_system_prompt TEXT NOT NULL DEFAULT '';

ALTER TABLE users
    DROP CONSTRAINT IF EXISTS provider_min_requirements;

ALTER TABLE users
    ADD CONSTRAINT provider_min_requirements CHECK (
        (auth_provider = 'local' AND password_hash IS NOT NULL)
        OR
        (auth_provider = 'google' AND google_sub IS NOT NULL AND password_hash IS NULL)
    );
