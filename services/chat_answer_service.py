from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from psycopg2 import errors as psycopg_errors
from psycopg2.extras import Json

from db import get_db_connection
from services.prompt_profile_service import (
    PROMPT_TYPE_QNA,
    load_prompt_profiles_for_user,
)
from services.retrieval_service import RetrievalService
from services.text_answer_service import (
    PROMPT_VERSION,
    TextAnswerService,
    TextAnswerServiceError,
    build_no_evidence_payload,
)

@dataclass
class ChatAnswerServiceError(Exception):
    code: str
    message: str
    status_code: int = 400
    details: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "status_code": self.status_code,
            "details": self.details or {},
        }


class ChatAnswerService:
    def __init__(self) -> None:
        self.retrieval_service = RetrievalService()
        self.text_answer_service = TextAnswerService()

    def answer_conversation_query(
        self,
        *,
        user_id: str,
        conversation_id: str,
        query: str,
        document_ids: list[str] | None = None,
        k: int | None = None,
        include_filtered: bool = False,
    ) -> dict[str, Any]:
        normalized_query = str(query or "").strip()
        if not normalized_query:
            raise ChatAnswerServiceError(
                code="empty_query",
                message="query is required.",
                status_code=400,
            )

        selected_document_ids = self._normalize_document_ids(document_ids)
        if not selected_document_ids:
            raise ChatAnswerServiceError(
                code="empty_document_scope",
                message="Select at least one document before asking a question.",
                status_code=400,
            )

        retrieval_payload = self.retrieval_service.retrieve_conversation_blocks(
            user_id=user_id,
            conversation_id=conversation_id,
            query=normalized_query,
            k=k,
            document_ids=selected_document_ids,
            include_filtered=include_filtered,
        )
        prompt_profiles = load_prompt_profiles_for_user(user_id)
        answer_payload = self._build_answer_payload(
            query=normalized_query,
            retrieval_payload=retrieval_payload,
            selected_document_ids=selected_document_ids,
            conversation_context=self._load_recent_conversation_context(
                user_id=user_id,
                conversation_id=conversation_id,
            ),
            qna_prompt_override=prompt_profiles.get(PROMPT_TYPE_QNA, ""),
        )
        enriched_retrieval_payload = self._build_persisted_retrieval_payload(
            retrieval_payload=retrieval_payload,
            answer_payload=answer_payload,
        )
        persisted_payload = self._persist_messages(
            user_id=user_id,
            conversation_id=conversation_id,
            query=normalized_query,
            answer_text=answer_payload["answer_text"],
            selected_document_ids=selected_document_ids,
            retrieval_payload=enriched_retrieval_payload,
            model_provider=answer_payload["model_provider"],
            model_name=answer_payload["model_name"],
            prompt_version=answer_payload["prompt_version"],
        )

        return {
            "query": normalized_query,
            "retrieval": enriched_retrieval_payload,
            "messages": persisted_payload,
        }

    def _build_answer_payload(
        self,
        *,
        query: str,
        retrieval_payload: dict[str, Any],
        selected_document_ids: list[str],
        conversation_context: list[dict[str, str]],
        qna_prompt_override: str = "",
    ) -> dict[str, Any]:
        results = retrieval_payload.get("results") if isinstance(retrieval_payload, dict) else []
        if not isinstance(results, list) or not results:
            return build_no_evidence_payload(retrieval_payload=retrieval_payload)

        try:
            return self.text_answer_service.generate_grounded_answer(
                query=query,
                retrieval_payload=retrieval_payload,
                selected_document_ids=selected_document_ids,
                conversation_context=conversation_context,
                user_prompt_override=qna_prompt_override,
            )
        except TextAnswerServiceError as exc:
            raise ChatAnswerServiceError(
                code=exc.code,
                message=exc.message,
                status_code=exc.status_code,
                details=exc.to_dict(),
            ) from exc

    @staticmethod
    def _build_citations(
        *,
        retrieval_payload: dict[str, Any],
        answer_payload: dict[str, Any],
    ) -> list[dict[str, Any]]:
        results = retrieval_payload.get("results") if isinstance(retrieval_payload, dict) else []
        results = results if isinstance(results, list) else []
        result_map = {
            str(result.get("block_id") or "").strip(): result
            for result in results
            if str(result.get("block_id") or "").strip()
        }

        citation_block_ids = [
            str(item).strip()
            for item in (answer_payload.get("citation_block_ids") or [])
            if str(item).strip()
        ]

        if not citation_block_ids and results:
            citation_block_ids = [
                str(result.get("block_id") or "").strip()
                for result in results[:2]
                if str(result.get("block_id") or "").strip()
            ]

        citations: list[dict[str, Any]] = []
        for block_id in citation_block_ids:
            result = result_map.get(block_id)
            if not result:
                continue
            source_metadata = result.get("source_metadata") if isinstance(result.get("source_metadata"), dict) else {}
            page_value = source_metadata.get("page") or source_metadata.get("page_number") or source_metadata.get("page_index")
            page_label = f"p. {page_value}" if page_value not in (None, "") else ""
            citations.append(
                {
                    "block_id": block_id,
                    "document_id": str(result.get("document_id") or ""),
                    "document_name": str(result.get("document_name") or result.get("document_id") or "Source"),
                    "snippet": str(result.get("snippet") or ""),
                    "page_label": page_label,
                    "score": float(result.get("score") or 0.0),
                },
            )
        return citations

    def _build_persisted_retrieval_payload(
        self,
        *,
        retrieval_payload: dict[str, Any],
        answer_payload: dict[str, Any],
    ) -> dict[str, Any]:
        citations = self._build_citations(
            retrieval_payload=retrieval_payload,
            answer_payload=answer_payload,
        )
        enriched_payload = dict(retrieval_payload or {})
        enriched_payload["citations"] = citations
        enriched_payload["grounded_answer"] = {
            "prompt_version": answer_payload.get("prompt_version") or PROMPT_VERSION,
            "prompt_profile": answer_payload.get("prompt_profile") or "default",
            "model_provider": answer_payload.get("model_provider") or "",
            "model_name": answer_payload.get("model_name") or "",
            "confidence": answer_payload.get("confidence") or "insufficient",
            "grounding_status": "grounded" if citations else "insufficient_evidence",
        }
        return enriched_payload

    def _load_recent_conversation_context(
        self,
        *,
        user_id: str,
        conversation_id: str,
        limit: int = 4,
    ) -> list[dict[str, str]]:
        conn = get_db_connection()
        try:
            with conn, conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT cm.role, cm.message_text
                    FROM conversation_messages cm
                    JOIN conversations c ON c.conversation_id = cm.conversation_id
                    WHERE cm.conversation_id = %s
                      AND c.user_id = %s
                    ORDER BY cm.created_at DESC, cm.message_id DESC
                    LIMIT %s
                    """,
                    (conversation_id, user_id, max(0, int(limit))),
                )
                rows = cur.fetchall()
        except psycopg_errors.UndefinedTable:
            return []
        finally:
            conn.close()

        context: list[dict[str, str]] = []
        for row in reversed(rows):
            role = str(row[0] or "").strip().lower()
            content = str(row[1] or "").strip()
            if role not in {"user", "assistant"} or not content:
                continue
            context.append(
                {
                    "role": role,
                    "content": content,
                }
            )
        return context

    def _persist_messages(
        self,
        *,
        user_id: str,
        conversation_id: str,
        query: str,
        answer_text: str,
        selected_document_ids: list[str],
        retrieval_payload: dict[str, Any],
        model_provider: str,
        model_name: str,
        prompt_version: str,
    ) -> dict[str, dict[str, Any]]:
        conn = get_db_connection()
        try:
            with conn, conn.cursor() as cur:
                user_message_id = str(uuid.uuid4())
                assistant_message_id = str(uuid.uuid4())

                cur.execute(
                    """
                    INSERT INTO conversation_messages (
                        message_id,
                        conversation_id,
                        user_id,
                        role,
                        message_text,
                        selected_document_ids,
                        retrieval_payload,
                        model_provider,
                        model_name,
                        prompt_version,
                        reply_to_message_id
                    )
                    VALUES (%s, %s, %s, 'user', %s, %s::jsonb, NULL, NULL, NULL, NULL, NULL)
                    RETURNING
                        message_id,
                        conversation_id,
                        user_id,
                        role,
                        message_text,
                        selected_document_ids,
                        retrieval_payload,
                        model_provider,
                        model_name,
                        prompt_version,
                        reply_to_message_id,
                        created_at
                    """,
                    (
                        user_message_id,
                        conversation_id,
                        user_id,
                        query,
                        Json(selected_document_ids),
                    ),
                )
                user_row = cur.fetchone()

                cur.execute(
                    """
                    INSERT INTO conversation_messages (
                        message_id,
                        conversation_id,
                        user_id,
                        role,
                        message_text,
                        selected_document_ids,
                        retrieval_payload,
                        model_provider,
                        model_name,
                        prompt_version,
                        reply_to_message_id
                    )
                    VALUES (%s, %s, %s, 'assistant', %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s)
                    RETURNING
                        message_id,
                        conversation_id,
                        user_id,
                        role,
                        message_text,
                        selected_document_ids,
                        retrieval_payload,
                        model_provider,
                        model_name,
                        prompt_version,
                        reply_to_message_id,
                        created_at
                    """,
                    (
                        assistant_message_id,
                        conversation_id,
                        user_id,
                        answer_text,
                        Json(selected_document_ids),
                        Json(retrieval_payload),
                        model_provider,
                        model_name,
                        prompt_version,
                        user_message_id,
                    ),
                )
                assistant_row = cur.fetchone()

                cur.execute(
                    """
                    UPDATE conversations
                    SET updated_at = CURRENT_TIMESTAMP
                    WHERE conversation_id = %s
                      AND user_id = %s
                    """,
                    (conversation_id, user_id),
                )

                return {
                    "user": self._serialize_message_row(user_row),
                    "assistant": self._serialize_message_row(assistant_row),
                }
        except psycopg_errors.UndefinedTable as exc:
            raise ChatAnswerServiceError(
                code="conversation_messages_table_missing",
                message="Conversation message storage is not available. Apply migration 010_conversation_messages.sql.",
                status_code=503,
                details={
                    "migration": "migrations/010_conversation_messages.sql",
                },
            ) from exc
        finally:
            conn.close()

    @staticmethod
    def _normalize_document_ids(raw_document_ids: list[str] | None) -> list[str]:
        if raw_document_ids is None:
            return []
        if not isinstance(raw_document_ids, list):
            raise ChatAnswerServiceError(
                code="invalid_document_ids",
                message="document_ids must be an array of document IDs.",
                status_code=400,
            )

        normalized_ids: list[str] = []
        seen_ids: set[str] = set()
        for item in raw_document_ids:
            normalized = str(item or "").strip()
            if not normalized or normalized in seen_ids:
                continue
            seen_ids.add(normalized)
            normalized_ids.append(normalized)
        return normalized_ids

    @staticmethod
    def _serialize_message_row(row) -> dict[str, Any]:
        selected_document_ids = row[5] if isinstance(row[5], list) else []
        retrieval_payload = row[6] if isinstance(row[6], dict) else None
        citations = retrieval_payload.get("citations") if isinstance(retrieval_payload, dict) else []
        citations = citations if isinstance(citations, list) else []
        return {
            "message_id": str(row[0]),
            "conversation_id": str(row[1]),
            "user_id": str(row[2]),
            "role": str(row[3] or ""),
            "message_text": row[4] or "",
            "selected_document_ids": [str(item) for item in selected_document_ids],
            "retrieval_payload": retrieval_payload,
            "citations": citations,
            "model_provider": row[7] or "",
            "model_name": row[8] or "",
            "prompt_version": row[9] or "",
            "reply_to_message_id": str(row[10]) if row[10] else None,
            "created_at": row[11].isoformat() if row[11] else "",
        }
