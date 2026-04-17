BEGIN;

DO $$
BEGIN
    IF to_regclass('public.document_summaries') IS NULL THEN
        RAISE NOTICE 'Skipping document summary invariant migration because table public.document_summaries does not exist.';
        RETURN;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'document_summaries'
          AND column_name = 'conversation_id'
    ) THEN
        EXECUTE '
            UPDATE document_summaries ds
            SET conversation_id = canonical.conversation_id
            FROM (
                SELECT cd.document_id, MIN(cd.conversation_id) AS conversation_id
                FROM conversation_documents cd
                GROUP BY cd.document_id
            ) AS canonical
            WHERE ds.document_id = canonical.document_id
              AND ds.conversation_id IS DISTINCT FROM canonical.conversation_id
        ';

        EXECUTE '
            UPDATE document_summaries ds
            SET conversation_id = NULL
            WHERE ds.conversation_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM conversation_documents cd
                  WHERE cd.document_id = ds.document_id
              )
        ';

        EXECUTE '
            CREATE INDEX IF NOT EXISTS idx_document_summaries_conversation_id
                ON document_summaries (conversation_id)
        ';
    ELSE
        RAISE NOTICE 'Skipping conversation_id backfill because column document_summaries.conversation_id does not exist.';
    END IF;

    EXECUTE '
        CREATE OR REPLACE FUNCTION sync_document_summaries_conversation_id()
        RETURNS trigger AS
        $fn$
        DECLARE
            derived_conversation_id UUID;
        BEGIN
            SELECT MIN(cd.conversation_id)
            INTO derived_conversation_id
            FROM conversation_documents cd
            WHERE cd.document_id = NEW.document_id;

            IF EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = ''public''
                  AND table_name = ''document_summaries''
                  AND column_name = ''conversation_id''
            ) THEN
                NEW.conversation_id := derived_conversation_id;
            END IF;

            RETURN NEW;
        END
        $fn$
        LANGUAGE plpgsql
    ';

    EXECUTE 'DROP TRIGGER IF EXISTS trg_sync_document_summaries_conversation_id ON document_summaries';

    EXECUTE '
        CREATE TRIGGER trg_sync_document_summaries_conversation_id
        BEFORE INSERT OR UPDATE OF document_id, conversation_id
        ON document_summaries
        FOR EACH ROW
        EXECUTE FUNCTION sync_document_summaries_conversation_id()
    ';
END;
$$;

COMMIT;
