BEGIN;

ALTER TABLE conversation_messages
    ADD COLUMN IF NOT EXISTS family_id UUID,
    ADD COLUMN IF NOT EXISTS family_version_number INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS branch_parent_message_id UUID REFERENCES conversation_messages(message_id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS is_active_in_family BOOLEAN NOT NULL DEFAULT TRUE;

UPDATE conversation_messages
SET family_id = message_id
WHERE role = 'user'
  AND family_id IS NULL;

UPDATE conversation_messages assistant
SET family_id = COALESCE(assistant.reply_to_message_id, assistant.message_id)
WHERE assistant.role = 'assistant'
  AND assistant.family_id IS NULL;

WITH ordered_messages AS (
    SELECT
        cm.message_id,
        cm.conversation_id,
        cm.role,
        cm.created_at,
        ROW_NUMBER() OVER (
            PARTITION BY cm.conversation_id
            ORDER BY
                cm.created_at ASC,
                COALESCE(cm.reply_to_message_id, cm.message_id) ASC,
                CASE WHEN cm.role = 'user' THEN 0 ELSE 1 END ASC,
                cm.message_id ASC
        ) AS sequence_no
    FROM conversation_messages cm
),
user_parent_links AS (
    SELECT
        u.message_id,
        (
            SELECT a.message_id
            FROM ordered_messages a
            WHERE a.conversation_id = u.conversation_id
              AND a.role = 'assistant'
              AND a.sequence_no < u.sequence_no
            ORDER BY a.sequence_no DESC
            LIMIT 1
        ) AS parent_assistant_message_id
    FROM ordered_messages u
    WHERE u.role = 'user'
)
UPDATE conversation_messages cm
SET branch_parent_message_id = upl.parent_assistant_message_id
FROM user_parent_links upl
WHERE cm.message_id = upl.message_id
  AND cm.role = 'user'
  AND cm.branch_parent_message_id IS NULL;

ALTER TABLE conversation_messages
    ALTER COLUMN family_id SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_conversation_messages_family
    ON conversation_messages (conversation_id, family_id, family_version_number);

CREATE INDEX IF NOT EXISTS idx_conversation_messages_branch_parent
    ON conversation_messages (conversation_id, branch_parent_message_id);

CREATE INDEX IF NOT EXISTS idx_conversation_messages_family_active
    ON conversation_messages (conversation_id, family_id, is_active_in_family);

COMMIT;
