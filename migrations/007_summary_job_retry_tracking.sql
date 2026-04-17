BEGIN;

DO $$
DECLARE
    target_table text;
    has_summary_status boolean;
    has_status boolean;
    has_job_status boolean;
BEGIN
    FOREACH target_table IN ARRAY ARRAY['document_summary_jobs', 'conversation_summary_jobs']
    LOOP
        IF to_regclass(format('public.%I', target_table)) IS NULL THEN
            RAISE NOTICE 'Skipping retry tracking migration for table % because it does not exist.', target_table;
            CONTINUE;
        END IF;

        EXECUTE format(
            'ALTER TABLE %I
                ADD COLUMN IF NOT EXISTS attempt_count INT NOT NULL DEFAULT 0,
                ADD COLUMN IF NOT EXISTS max_attempts INT NOT NULL DEFAULT 5,
                ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ,
                ADD COLUMN IF NOT EXISTS last_error_code TEXT,
                ADD COLUMN IF NOT EXISTS last_error_message TEXT',
            target_table
        );

        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = target_table
              AND column_name = 'summary_status'
        )
        INTO has_summary_status;

        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = target_table
              AND column_name = 'status'
        )
        INTO has_status;

        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = target_table
              AND column_name = 'job_status'
        )
        INTO has_job_status;

        IF NOT has_summary_status THEN
            EXECUTE format('ALTER TABLE %I ADD COLUMN summary_status TEXT', target_table);
        END IF;

        IF has_status THEN
            EXECUTE format(
                'UPDATE %I
                 SET summary_status = COALESCE(NULLIF(BTRIM(summary_status), ''''), NULLIF(BTRIM(status), ''''))
                 WHERE summary_status IS NULL OR BTRIM(summary_status) = ''''',
                target_table
            );
        ELSIF has_job_status THEN
            EXECUTE format(
                'UPDATE %I
                 SET summary_status = COALESCE(NULLIF(BTRIM(summary_status), ''''), NULLIF(BTRIM(job_status), ''''))
                 WHERE summary_status IS NULL OR BTRIM(summary_status) = ''''',
                target_table
            );
        END IF;

        EXECUTE format(
            'UPDATE %I
             SET summary_status = ''pending''
             WHERE summary_status IS NULL OR BTRIM(summary_status) = ''''',
            target_table
        );

        EXECUTE format(
            'UPDATE %I
             SET summary_status = ''pending''
             WHERE summary_status IN (''queued'', ''retrying'')',
            target_table
        );

        EXECUTE format(
            'UPDATE %I
             SET next_attempt_at = CURRENT_TIMESTAMP
             WHERE summary_status IN (''pending'', ''failed'')
               AND next_attempt_at IS NULL',
            target_table
        );

        EXECUTE format('ALTER TABLE %I ALTER COLUMN summary_status SET NOT NULL', target_table);
        EXECUTE format('ALTER TABLE %I ALTER COLUMN summary_status SET DEFAULT ''pending''', target_table);

        EXECUTE format(
            'CREATE INDEX IF NOT EXISTS %I ON %I (summary_status, next_attempt_at)',
            'idx_' || target_table || '_summary_status_next_attempt_at',
            target_table
        );
    END LOOP;
END;
$$;

COMMIT;
