from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from psycopg2.extras import Json

from db import get_db_connection
from services.retrieval_service import RetrievalService


PROMPT_VERSION = "retrieval_inspection_v1"


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
        summary_text = self._build_retrieval_summary(retrieval_payload)
        persisted_payload = self._persist_messages(
            user_id=user_id,
            conversation_id=conversation_id,
            query=normalized_query,
            summary_text=summary_text,
            selected_document_ids=selected_document_ids,
            retrieval_payload=retrieval_payload,
        )

        return {
            "query": normalized_query,
            "retrieval": retrieval_payload,
            "messages": persisted_payload,
        }

    def _persist_messages(
        self,
        *,
        user_id: str,
        conversation_id: str,
        query: str,
        summary_text: str,
        selected_document_ids: list[str],
        retrieval_payload: dict[str, Any],
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
                    VALUES (%s, %s, %s, 'assistant', %s, %s::jsonb, %s::jsonb, NULL, NULL, %s, %s)
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
                        summary_text,
                        Json(selected_document_ids),
                        Json(retrieval_payload),
                        PROMPT_VERSION,
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
        finally:
            conn.close()

    @staticmethod
    def _build_retrieval_summary(retrieval_payload: dict[str, Any]) -> str:
        returned_count = int(retrieval_payload.get("returned_count") or 0)
        strategy = str(retrieval_payload.get("strategy") or "vector").replace("_", " ")
        filter_summary = retrieval_payload.get("filter_summary") or {}
        excluded_count = int(filter_summary.get("excluded_candidate_count") or 0)
        if returned_count <= 0:
            return f"No useful retrieval results found ({strategy}). {excluded_count} chunk(s) were filtered out."
        return (
            f"Top {returned_count} retrieval result(s) returned via {strategy}. "
            f"{excluded_count} chunk(s) were filtered out by default."
        )

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
        return {
            "message_id": str(row[0]),
            "conversation_id": str(row[1]),
            "user_id": str(row[2]),
            "role": str(row[3] or ""),
            "message_text": row[4] or "",
            "selected_document_ids": [str(item) for item in selected_document_ids],
            "retrieval_payload": retrieval_payload,
            "model_provider": row[7] or "",
            "model_name": row[8] or "",
            "prompt_version": row[9] or "",
            "reply_to_message_id": str(row[10]) if row[10] else None,
            "created_at": row[11].isoformat() if row[11] else "",
        }
