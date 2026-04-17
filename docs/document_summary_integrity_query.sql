-- Periodic integrity query for document summary linkage.
-- Flags rows where document_summaries.conversation_id drifted from the canonical
-- conversation_documents mapping, or where conversation/document ownership mismatches.
SELECT
    ds.document_id,
    ds.conversation_id AS summary_conversation_id,
    canonical.conversation_id AS canonical_conversation_id,
    d.user_id AS document_user_id,
    c.user_id AS conversation_user_id,
    CASE
        WHEN d.document_id IS NULL THEN 'missing_document'
        WHEN ds.conversation_id IS DISTINCT FROM canonical.conversation_id THEN 'conversation_id_mismatch'
        WHEN ds.conversation_id IS NOT NULL AND c.conversation_id IS NULL THEN 'missing_conversation'
        WHEN ds.conversation_id IS NOT NULL AND d.user_id IS DISTINCT FROM c.user_id THEN 'ownership_mismatch'
        ELSE 'ok'
    END AS integrity_status
FROM document_summaries ds
LEFT JOIN documents d
    ON d.document_id = ds.document_id
LEFT JOIN LATERAL (
    SELECT MIN(cd.conversation_id) AS conversation_id
    FROM conversation_documents cd
    WHERE cd.document_id = ds.document_id
) canonical ON TRUE
LEFT JOIN conversations c
    ON c.conversation_id = ds.conversation_id
WHERE d.document_id IS NULL
   OR ds.conversation_id IS DISTINCT FROM canonical.conversation_id
   OR (ds.conversation_id IS NOT NULL AND c.conversation_id IS NULL)
   OR (ds.conversation_id IS NOT NULL AND d.user_id IS DISTINCT FROM c.user_id)
ORDER BY ds.document_id;
