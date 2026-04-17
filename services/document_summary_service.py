from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from psycopg2.extras import Json

from db import get_db_connection
from services.summary_jobs import mark_document_summary_completed


PROCESSING_STATUS_RETRIEVAL_PREPARED = "retrieval_prepared"


class SummaryPreparationService:
    def build_document_summary_input(self, *, cur, document_id: str) -> dict[str, Any]:
        cur.execute(
            """
            SELECT
                db.block_id::text,
                db.source_unit_index,
                db.reading_order,
                COALESCE(NULLIF(BTRIM(db.normalized_content->>'retrieval_text'), ''), db.display_text, '') AS source_text,
                COALESCE(db.source_metadata, '{}'::jsonb) AS source_metadata
            FROM document_blocks db
            WHERE db.document_id = %s
              AND db.processing_status = %s
              AND NULLIF(BTRIM(COALESCE(db.normalized_content->>'retrieval_text', db.display_text, '')), '') IS NOT NULL
            ORDER BY db.source_unit_index ASC NULLS LAST, db.reading_order ASC NULLS LAST, db.block_id ASC
            """,
            (document_id, PROCESSING_STATUS_RETRIEVAL_PREPARED),
        )
        rows = cur.fetchall()

        sources = [
            {
                "block_id": row[0],
                "source_unit_index": row[1],
                "reading_order": row[2],
                "text": row[3] or "",
                "source_metadata": row[4] or {},
            }
            for row in rows
        ]

        return {
            "document_id": str(document_id),
            "source_count": len(sources),
            "sources": sources,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    def build_conversation_summary_input(self, *, cur, conversation_id: str) -> dict[str, Any]:
        cur.execute(
            """
            SELECT
                d.document_id::text,
                d.original_filename,
                COALESCE(ds.summary_text, '') AS summary_text,
                COALESCE(ds.highlights, '[]'::jsonb) AS highlights,
                COALESCE(ds.metadata, '{}'::jsonb) AS metadata,
                COALESCE(ds.updated_at, ds.created_at) AS summary_updated_at,
                cd.added_at
            FROM conversation_documents cd
            JOIN documents d ON d.document_id = cd.document_id
            LEFT JOIN document_summaries ds ON ds.document_id = d.document_id
            WHERE cd.conversation_id = %s
              AND d.is_deleted = FALSE
            ORDER BY cd.added_at DESC, d.created_at DESC
            """,
            (conversation_id,),
        )
        rows = cur.fetchall()

        documents: list[dict[str, Any]] = []
        summary_fragments: list[str] = []
        for row in rows:
            summary_text = (row[2] or "").strip()
            documents.append(
                {
                    "document_id": row[0],
                    "filename": row[1] or "",
                    "summary_text": summary_text,
                    "highlights": row[3] or [],
                    "metadata": row[4] or {},
                    "summary_updated_at": row[5].isoformat() if row[5] else None,
                    "added_at": row[6].isoformat() if row[6] else None,
                }
            )
            if summary_text:
                summary_fragments.append(summary_text)

        return {
            "conversation_id": str(conversation_id),
            "document_count": len(documents),
            "documents": documents,
            "combined_summary_text": "\n\n".join(summary_fragments),
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }


class DocumentSummaryService:
    def __init__(self) -> None:
        self.preparation_service = SummaryPreparationService()

    def get_document_summary_payload(
        self,
        *,
        user_id: str,
        document_id: str,
        conversation_id: str | None = None,
    ) -> dict[str, Any] | None:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                if conversation_id:
                    cur.execute(
                        """
                        SELECT c.conversation_id::text
                        FROM conversations c
                        JOIN conversation_documents cd ON cd.conversation_id = c.conversation_id
                        WHERE c.user_id = %s
                          AND c.conversation_id = %s
                          AND cd.document_id = %s
                        LIMIT 1
                        """,
                        (user_id, conversation_id, document_id),
                    )
                else:
                    cur.execute(
                        """
                        SELECT NULL::text
                        FROM documents d
                        WHERE d.user_id = %s
                          AND d.document_id = %s
                          AND d.is_deleted = FALSE
                        LIMIT 1
                        """,
                        (user_id, document_id),
                    )

                access_row = cur.fetchone()
                if not access_row:
                    return None

                effective_conversation_id = access_row[0] if conversation_id else None

                cur.execute(
                    """
                    SELECT
                        ds.id,
                        ds.document_id::text,
                        ds.conversation_id::text,
                        ds.summary_text,
                        ds.highlights,
                        ds.metadata,
                        ds.source_content_hash,
                        ds.source_content_version,
                        ds.created_at,
                        ds.updated_at
                    FROM document_summaries ds
                    WHERE ds.document_id = %s
                    LIMIT 1
                    """,
                    (document_id,),
                )
                summary_row = cur.fetchone()

                summary_payload = None
                if summary_row:
                    summary_payload = {
                        "id": summary_row[0],
                        "document_id": summary_row[1],
                        "conversation_id": summary_row[2],
                        "summary_text": summary_row[3] or "",
                        "highlights": summary_row[4] or [],
                        "metadata": summary_row[5] or {},
                        "source_content_hash": summary_row[6],
                        "source_content_version": summary_row[7],
                        "created_at": summary_row[8].isoformat() if summary_row[8] else None,
                        "updated_at": summary_row[9].isoformat() if summary_row[9] else None,
                        "sources": [],
                    }
                    cur.execute(
                        """
                        SELECT
                            dss.id,
                            dss.block_id::text,
                            dss.source_rank,
                            dss.source_snippet,
                            dss.source_metadata,
                            dss.created_at
                        FROM document_summary_sources dss
                        WHERE dss.document_summary_id = %s
                        ORDER BY dss.source_rank ASC, dss.id ASC
                        """,
                        (summary_row[0],),
                    )
                    summary_payload["sources"] = [
                        {
                            "id": row[0],
                            "block_id": row[1],
                            "source_rank": row[2],
                            "source_snippet": row[3] or "",
                            "source_metadata": row[4] or {},
                            "created_at": row[5].isoformat() if row[5] else None,
                        }
                        for row in cur.fetchall()
                    ]

                preparation_payload = self.preparation_service.build_document_summary_input(
                    cur=cur,
                    document_id=document_id,
                )

                return {
                    "document_id": str(document_id),
                    "conversation_id": effective_conversation_id,
                    "has_summary": bool(summary_payload),
                    "summary": summary_payload,
                    "preparation": preparation_payload,
                }
        finally:
            conn.close()

    def upsert_document_summary(
        self,
        *,
        document_id: str,
        summary_text: str,
        highlights: list[dict[str, Any]] | list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        sources: list[dict[str, Any]] | None = None,
        conversation_id: str | None = None,
        source_content_hash: str | None = None,
        source_content_version: str | None = None,
    ) -> dict[str, Any]:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                if not conversation_id:
                    cur.execute(
                        """
                        SELECT cd.conversation_id::text
                        FROM conversation_documents cd
                        JOIN documents d ON d.document_id = cd.document_id
                        WHERE cd.document_id = %s
                          AND d.is_deleted = FALSE
                        ORDER BY cd.added_at DESC
                        LIMIT 1
                        """,
                        (document_id,),
                    )
                    row = cur.fetchone()
                    conversation_id = row[0] if row else None

                cur.execute(
                    """
                    INSERT INTO document_summaries (
                        document_id,
                        conversation_id,
                        summary_text,
                        highlights,
                        metadata,
                        source_content_hash,
                        source_content_version,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (document_id)
                    DO UPDATE SET
                        conversation_id = EXCLUDED.conversation_id,
                        summary_text = EXCLUDED.summary_text,
                        highlights = EXCLUDED.highlights,
                        metadata = EXCLUDED.metadata,
                        source_content_hash = EXCLUDED.source_content_hash,
                        source_content_version = EXCLUDED.source_content_version,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING id
                    """,
                    (
                        document_id,
                        conversation_id,
                        (summary_text or "").strip(),
                        Json(highlights or []),
                        Json(metadata or {}),
                        source_content_hash,
                        source_content_version,
                    ),
                )
                summary_id = cur.fetchone()[0]

                cur.execute(
                    "DELETE FROM document_summary_sources WHERE document_summary_id = %s",
                    (summary_id,),
                )
                for idx, source in enumerate(sources or [], start=1):
                    cur.execute(
                        """
                        INSERT INTO document_summary_sources (
                            document_summary_id,
                            document_id,
                            block_id,
                            source_rank,
                            source_snippet,
                            source_metadata
                        )
                        VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                        """,
                        (
                            summary_id,
                            document_id,
                            source.get("block_id"),
                            source.get("source_rank") or idx,
                            source.get("source_snippet") or source.get("text") or "",
                            Json(source.get("source_metadata") or {}),
                        ),
                    )

                mark_document_summary_completed(
                    cur,
                    document_id=document_id,
                    conversation_id=conversation_id,
                    content_hash=source_content_hash,
                    content_version=source_content_version,
                )

                if conversation_id:
                    self.recompute_conversation_summary(cur=cur, conversation_id=conversation_id)

            conn.commit()
            return {
                "document_id": str(document_id),
                "conversation_id": conversation_id,
                "summary_id": summary_id,
            }
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_conversation_summary_payload(self, *, user_id: str, conversation_id: str) -> dict[str, Any] | None:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT c.title
                    FROM conversations c
                    WHERE c.user_id = %s
                      AND c.conversation_id = %s
                    LIMIT 1
                    """,
                    (user_id, conversation_id),
                )
                row = cur.fetchone()
                if not row:
                    return None
                conversation_title = (row[0] or "").strip()

                cur.execute(
                    """
                    SELECT
                        cs.id,
                        cs.title,
                        cs.summary_text,
                        cs.metadata,
                        cs.source_version,
                        cs.created_at,
                        cs.updated_at
                    FROM conversation_summaries cs
                    WHERE cs.conversation_id = %s
                    LIMIT 1
                    """,
                    (conversation_id,),
                )
                summary_row = cur.fetchone()

                summary_payload = None
                if summary_row:
                    summary_payload = {
                        "id": summary_row[0],
                        "title": summary_row[1] or conversation_title,
                        "summary_text": summary_row[2] or "",
                        "metadata": summary_row[3] or {},
                        "source_version": summary_row[4],
                        "created_at": summary_row[5].isoformat() if summary_row[5] else None,
                        "updated_at": summary_row[6].isoformat() if summary_row[6] else None,
                        "sources": [],
                    }
                    cur.execute(
                        """
                        SELECT
                            css.id,
                            css.document_summary_id,
                            css.document_id::text,
                            css.source_rank,
                            css.source_excerpt,
                            css.source_metadata,
                            css.created_at
                        FROM conversation_summary_sources css
                        WHERE css.conversation_summary_id = %s
                        ORDER BY css.source_rank ASC, css.id ASC
                        """,
                        (summary_row[0],),
                    )
                    summary_payload["sources"] = [
                        {
                            "id": source_row[0],
                            "document_summary_id": source_row[1],
                            "document_id": source_row[2],
                            "source_rank": source_row[3],
                            "source_excerpt": source_row[4] or "",
                            "source_metadata": source_row[5] or {},
                            "created_at": source_row[6].isoformat() if source_row[6] else None,
                        }
                        for source_row in cur.fetchall()
                    ]

                preparation_payload = self.preparation_service.build_conversation_summary_input(
                    cur=cur,
                    conversation_id=conversation_id,
                )

                return {
                    "conversation_id": str(conversation_id),
                    "title": conversation_title,
                    "has_summary": bool(summary_payload),
                    "summary": summary_payload,
                    "preparation": preparation_payload,
                }
        finally:
            conn.close()

    def recompute_conversation_summary(self, *, cur, conversation_id: str) -> dict[str, Any]:
        preparation_payload = self.preparation_service.build_conversation_summary_input(
            cur=cur,
            conversation_id=conversation_id,
        )

        document_summaries = [
            item
            for item in preparation_payload.get("documents") or []
            if str(item.get("summary_text") or "").strip()
        ]
        if not document_summaries:
            return {
                "conversation_id": str(conversation_id),
                "summary_id": None,
                "document_count": int(preparation_payload.get("document_count") or 0),
            }

        aggregate_summary = "\n\n".join(item["summary_text"] for item in document_summaries)
        metadata = {
            "document_count": int(preparation_payload.get("document_count") or 0),
            "summarized_document_count": len(document_summaries),
            "recomputed_at": datetime.now(timezone.utc).isoformat(),
        }

        cur.execute(
            """
            SELECT title
            FROM conversations
            WHERE conversation_id = %s
            LIMIT 1
            """,
            (conversation_id,),
        )
        title_row = cur.fetchone()
        conversation_title = (title_row[0] or "").strip() if title_row else ""

        cur.execute(
            """
            INSERT INTO conversation_summaries (
                conversation_id,
                title,
                summary_text,
                metadata,
                source_version,
                updated_at
            )
            VALUES (%s, %s, %s, %s::jsonb, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (conversation_id)
            DO UPDATE SET
                title = EXCLUDED.title,
                summary_text = EXCLUDED.summary_text,
                metadata = EXCLUDED.metadata,
                source_version = EXCLUDED.source_version,
                updated_at = CURRENT_TIMESTAMP
            RETURNING id
            """,
            (
                conversation_id,
                conversation_title,
                aggregate_summary,
                Json(metadata),
                "conversation_summary_v1",
            ),
        )
        summary_id = cur.fetchone()[0]

        cur.execute(
            "DELETE FROM conversation_summary_sources WHERE conversation_summary_id = %s",
            (summary_id,),
        )

        for idx, item in enumerate(document_summaries, start=1):
            cur.execute(
                """
                INSERT INTO conversation_summary_sources (
                    conversation_summary_id,
                    document_summary_id,
                    document_id,
                    source_rank,
                    source_excerpt,
                    source_metadata
                )
                SELECT
                    %s,
                    ds.id,
                    %s,
                    %s,
                    %s,
                    %s::jsonb
                FROM document_summaries ds
                WHERE ds.document_id = %s
                """,
                (
                    summary_id,
                    item.get("document_id"),
                    idx,
                    (item.get("summary_text") or "")[:1200],
                    Json({"filename": item.get("filename") or ""}),
                    item.get("document_id"),
                ),
            )

        return {
            "conversation_id": str(conversation_id),
            "summary_id": summary_id,
            "document_count": int(preparation_payload.get("document_count") or 0),
        }
