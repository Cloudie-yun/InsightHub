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

        ordered_rows = self._load_conversation_message_rows(
            user_id=user_id,
            conversation_id=conversation_id,
        )
        active_rows = self._resolve_active_branch_rows(ordered_rows)

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
            conversation_context=self._build_conversation_context_from_rows(active_rows[-8:]),
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
            branch_parent_message_id=self._get_active_tail_assistant_id(active_rows),
        )

        return {
            "query": normalized_query,
            "retrieval": enriched_retrieval_payload,
            "messages": persisted_payload,
        }

    def replay_conversation_query(
        self,
        *,
        user_id: str,
        conversation_id: str,
        target_message_id: str,
        mode: str,
        query: str | None = None,
        document_ids: list[str] | None = None,
        k: int | None = None,
        include_filtered: bool = False,
    ) -> dict[str, Any]:
        normalized_mode = str(mode or "").strip().lower()
        if normalized_mode not in {"edit", "regenerate"}:
            raise ChatAnswerServiceError(
                code="invalid_replay_mode",
                message="mode must be 'edit' or 'regenerate'.",
                status_code=400,
            )

        ordered_rows = self._load_conversation_message_rows(
            user_id=user_id,
            conversation_id=conversation_id,
        )
        if not ordered_rows:
            raise ChatAnswerServiceError(
                code="conversation_messages_missing",
                message="No conversation messages were found to replay.",
                status_code=404,
            )

        message_index_by_id = {
            str(row[0]): index
            for index, row in enumerate(ordered_rows)
            if row and row[0]
        }
        target_index = message_index_by_id.get(str(target_message_id or "").strip())
        if target_index is None:
            raise ChatAnswerServiceError(
                code="message_not_found",
                message="The selected message could not be found in this conversation.",
                status_code=404,
            )

        target_row = ordered_rows[target_index]
        if self._rows_support_versioning(ordered_rows):
            active_rows = self._resolve_active_branch_rows(ordered_rows)
            active_index_by_id = {
                str(row[0]): index
                for index, row in enumerate(active_rows)
                if row and row[0]
            }
            if str(target_message_id or "").strip() not in active_index_by_id:
                raise ChatAnswerServiceError(
                    code="message_not_active",
                    message="Only the active branch can be edited or regenerated.",
                    status_code=400,
                )

            target_role = str(target_row[3] or "").strip().lower()
            if normalized_mode == "edit" and target_role != "user":
                raise ChatAnswerServiceError(
                    code="invalid_edit_target",
                    message="Only user messages can be edited.",
                    status_code=400,
                )
            if normalized_mode == "regenerate" and target_role != "assistant":
                raise ChatAnswerServiceError(
                    code="invalid_regenerate_target",
                    message="Only assistant messages can be regenerated.",
                    status_code=400,
                )

            if normalized_mode == "edit":
                active_user_row = target_row
            else:
                reply_to_message_id = str(target_row[10] or "").strip()
                active_user_row = next(
                    (row for row in active_rows if str(row[0] or "") == reply_to_message_id),
                    None,
                )
            if active_user_row is None:
                raise ChatAnswerServiceError(
                    code="family_user_missing",
                    message="The active prompt version for this turn could not be found.",
                    status_code=404,
                )
            active_user_index = active_index_by_id.get(str(active_user_row[0]), 0)
            context_rows = active_rows[:active_user_index]

            if normalized_mode == "edit":
                normalized_query = str(query or "").strip()
                if not normalized_query:
                    raise ChatAnswerServiceError(
                        code="empty_query",
                        message="query is required.",
                        status_code=400,
                    )
                selected_document_ids = self._normalize_document_ids(document_ids)
                if not selected_document_ids:
                    selected_document_ids = self._get_selected_document_ids_from_row(active_user_row)
            else:
                normalized_query = str(active_user_row[4] or "").strip()
                selected_document_ids = self._get_selected_document_ids_from_row(target_row) or self._get_selected_document_ids_from_row(active_user_row)

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
                conversation_context=self._build_conversation_context_from_rows(context_rows),
                qna_prompt_override=prompt_profiles.get(PROMPT_TYPE_QNA, ""),
            )
            enriched_retrieval_payload = self._build_persisted_retrieval_payload(
                retrieval_payload=retrieval_payload,
                answer_payload=answer_payload,
            )
            if normalized_mode == "edit":
                persisted_payload = self._append_user_family_version(
                    user_id=user_id,
                    conversation_id=conversation_id,
                    user_family_id=str(active_user_row[12]),
                    branch_parent_message_id=str(active_user_row[14]) if active_user_row[14] else None,
                    query=normalized_query,
                    answer_text=answer_payload["answer_text"],
                    selected_document_ids=selected_document_ids,
                    retrieval_payload=enriched_retrieval_payload,
                    model_provider=answer_payload["model_provider"],
                    model_name=answer_payload["model_name"],
                    prompt_version=answer_payload["prompt_version"],
                    next_user_version_number=self._get_next_role_family_version_number(
                        ordered_rows,
                        str(active_user_row[12]),
                        "user",
                    ),
                )
            else:
                persisted_payload = self._append_assistant_family_version(
                    user_id=user_id,
                    conversation_id=conversation_id,
                    user_row=active_user_row,
                    assistant_family_id=str(target_row[12]),
                    answer_text=answer_payload["answer_text"],
                    selected_document_ids=selected_document_ids,
                    retrieval_payload=enriched_retrieval_payload,
                    model_provider=answer_payload["model_provider"],
                    model_name=answer_payload["model_name"],
                    prompt_version=answer_payload["prompt_version"],
                    next_assistant_version_number=self._get_next_role_family_version_number(
                        ordered_rows,
                        str(target_row[12]),
                        "assistant",
                    ),
                    user_version_count=self._get_role_family_version_total(
                        ordered_rows,
                        str(active_user_row[12]),
                        "user",
                    ),
                )
            return {
                "mode": normalized_mode,
                "replace_from_message_id": str(target_message_id),
                "query": normalized_query,
                "retrieval": enriched_retrieval_payload,
                "messages": persisted_payload,
            }

        target_role = str(target_row[3] or "").strip().lower()
        if normalized_mode == "edit" and target_role != "user":
            raise ChatAnswerServiceError(
                code="invalid_edit_target",
                message="Only user messages can be edited.",
                status_code=400,
            )
        if normalized_mode == "regenerate" and target_role != "assistant":
            raise ChatAnswerServiceError(
                code="invalid_regenerate_target",
                message="Only assistant messages can be regenerated.",
                status_code=400,
            )

        row_by_id = {
            str(row[0]): row
            for row in ordered_rows
            if row and row[0]
        }

        if normalized_mode == "edit":
            active_user_row = target_row
            active_user_index = target_index
            replace_from_index = target_index
            normalized_query = str(query or "").strip()
            if not normalized_query:
                raise ChatAnswerServiceError(
                    code="empty_query",
                    message="query is required.",
                    status_code=400,
                )
            selected_document_ids = self._normalize_document_ids(document_ids)
            if not selected_document_ids:
                selected_document_ids = self._get_selected_document_ids_from_row(active_user_row)
        else:
            reply_to_message_id = str(target_row[10] or "").strip()
            active_user_row = row_by_id.get(reply_to_message_id)
            if active_user_row is None:
                raise ChatAnswerServiceError(
                    code="reply_target_missing",
                    message="The original user message for this assistant reply could not be found.",
                    status_code=404,
                )
            active_user_index = message_index_by_id.get(str(active_user_row[0]))
            if active_user_index is None:
                raise ChatAnswerServiceError(
                    code="reply_target_missing",
                    message="The original user message for this assistant reply could not be found.",
                    status_code=404,
                )
            replace_from_index = target_index
            normalized_query = str(active_user_row[4] or "").strip()
            selected_document_ids = self._get_selected_document_ids_from_row(target_row) or self._get_selected_document_ids_from_row(active_user_row)

        if not selected_document_ids:
            raise ChatAnswerServiceError(
                code="empty_document_scope",
                message="Select at least one document before asking a question.",
                status_code=400,
            )

        context_rows = ordered_rows[:active_user_index]
        conversation_context = self._build_conversation_context_from_rows(context_rows)

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
            conversation_context=conversation_context,
            qna_prompt_override=prompt_profiles.get(PROMPT_TYPE_QNA, ""),
        )
        enriched_retrieval_payload = self._build_persisted_retrieval_payload(
            retrieval_payload=retrieval_payload,
            answer_payload=answer_payload,
        )

        delete_message_ids = [
            str(row[0])
            for row in ordered_rows[replace_from_index:]
            if row and row[0]
        ]
        persisted_payload = self._replace_message_tail(
            user_id=user_id,
            conversation_id=conversation_id,
            delete_message_ids=delete_message_ids,
            query=normalized_query,
            answer_text=answer_payload["answer_text"],
            selected_document_ids=selected_document_ids,
            retrieval_payload=enriched_retrieval_payload,
            model_provider=answer_payload["model_provider"],
            model_name=answer_payload["model_name"],
            prompt_version=answer_payload["prompt_version"],
            existing_user_row=active_user_row if normalized_mode == "regenerate" else None,
        )

        return {
            "mode": normalized_mode,
            "replace_from_message_id": str(target_message_id),
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
        rows = self._resolve_active_branch_rows(
            self._load_conversation_message_rows(
                user_id=user_id,
                conversation_id=conversation_id,
            )
        )
        return self._build_conversation_context_from_rows(rows[-max(0, int(limit) * 2):])

    def _load_conversation_message_rows(
        self,
        *,
        user_id: str,
        conversation_id: str,
    ) -> list[Any]:
        conn = get_db_connection()
        try:
            with conn, conn.cursor() as cur:
                if self._conversation_messages_support_versioning(cur):
                    cur.execute(
                        """
                        SELECT
                            cm.message_id,
                            cm.conversation_id,
                            cm.user_id,
                            cm.role,
                            cm.message_text,
                            cm.selected_document_ids,
                            cm.retrieval_payload,
                            cm.model_provider,
                            cm.model_name,
                            cm.prompt_version,
                            cm.reply_to_message_id,
                            cm.created_at,
                            cm.family_id,
                            cm.family_version_number,
                            cm.branch_parent_message_id,
                            cm.is_active_in_family
                        FROM conversation_messages cm
                        JOIN conversations c ON c.conversation_id = cm.conversation_id
                        WHERE cm.conversation_id = %s
                          AND c.user_id = %s
                        ORDER BY
                            cm.created_at ASC,
                            cm.family_id ASC NULLS LAST,
                            cm.family_version_number ASC NULLS LAST,
                            CASE WHEN cm.role = 'user' THEN 0 ELSE 1 END ASC,
                            cm.message_id ASC
                        """,
                        (conversation_id, user_id),
                    )
                else:
                    cur.execute(
                        """
                        SELECT
                            cm.message_id,
                            cm.conversation_id,
                            cm.user_id,
                            cm.role,
                            cm.message_text,
                            cm.selected_document_ids,
                            cm.retrieval_payload,
                            cm.model_provider,
                            cm.model_name,
                            cm.prompt_version,
                            cm.reply_to_message_id,
                            cm.created_at
                        FROM conversation_messages cm
                        JOIN conversations c ON c.conversation_id = cm.conversation_id
                        WHERE cm.conversation_id = %s
                          AND c.user_id = %s
                        ORDER BY
                            cm.created_at ASC,
                            COALESCE(cm.reply_to_message_id, cm.message_id) ASC,
                            CASE WHEN cm.role = 'user' THEN 0 ELSE 1 END ASC,
                            cm.message_id ASC
                        """,
                        (conversation_id, user_id),
                    )
                return cur.fetchall()
        except psycopg_errors.UndefinedTable as exc:
            raise ChatAnswerServiceError(
                code="conversation_messages_table_missing",
                message="Conversation message storage is not available. Apply migration 010_conversation_messages.sql.",
                status_code=503,
                details={"migration": "migrations/010_conversation_messages.sql"},
            ) from exc
        finally:
            conn.close()

    @staticmethod
    def _build_conversation_context_from_rows(rows: list[Any]) -> list[dict[str, str]]:
        context: list[dict[str, str]] = []
        for row in rows:
            role = str(row[3] or "").strip().lower()
            content = str(row[4] or "").strip()
            if role not in {"user", "assistant"} or not content:
                continue
            context.append({"role": role, "content": content})
        return context

    @staticmethod
    def _get_selected_document_ids_from_row(row) -> list[str]:
        raw_ids = row[5] if len(row) > 5 and isinstance(row[5], list) else []
        return [str(item).strip() for item in raw_ids if str(item).strip()]

    @staticmethod
    def _rows_support_versioning(rows: list[Any]) -> bool:
        return bool(rows) and len(rows[0]) >= 16

    @staticmethod
    def _resolve_active_branch_rows(rows: list[Any]) -> list[Any]:
        if not rows or not ChatAnswerService._rows_support_versioning(rows):
            return rows

        user_rows_by_parent: dict[str | None, list[Any]] = {}
        assistant_rows_by_user: dict[str, list[Any]] = {}
        for row in rows:
            role = str(row[3] or "").strip().lower()
            if role == "user":
                parent_id = str(row[14]) if row[14] else None
                user_rows_by_parent.setdefault(parent_id, []).append(row)
            elif role == "assistant" and row[10]:
                assistant_rows_by_user.setdefault(str(row[10]), []).append(row)

        active_rows: list[Any] = []
        visited_user_ids: set[str] = set()
        parent_assistant_id: str | None = None

        while True:
            candidates = [
                row
                for row in user_rows_by_parent.get(parent_assistant_id, [])
                if bool(row[15])
            ]
            if not candidates:
                break
            user_row = sorted(
                candidates,
                key=lambda item: (int(item[13] or 1), str(item[11] or ""), str(item[0] or "")),
            )[-1]
            user_message_id = str(user_row[0] or "")
            if not user_message_id or user_message_id in visited_user_ids:
                break
            visited_user_ids.add(user_message_id)
            active_rows.append(user_row)

            assistant_candidates = assistant_rows_by_user.get(user_message_id, [])
            if not assistant_candidates:
                break
            active_assistant_candidates = [
                row for row in assistant_candidates if bool(row[15])
            ]
            assistant_row = sorted(
                active_assistant_candidates or assistant_candidates,
                key=lambda item: (int(item[13] or 1), str(item[11] or ""), str(item[0] or "")),
            )[-1]
            active_rows.append(assistant_row)
            parent_assistant_id = str(assistant_row[0] or "") or None

        return active_rows

    @staticmethod
    def _get_active_tail_assistant_id(rows: list[Any]) -> str | None:
        for row in reversed(rows):
            if str(row[3] or "").strip().lower() == "assistant":
                return str(row[0])
        return None

    @staticmethod
    def _get_next_family_version_number(rows: list[Any], family_id: str) -> int:
        existing = [
            int(row[13] or 1)
            for row in rows
            if len(row) >= 14 and str(row[12] or "") == str(family_id or "")
        ]
        return (max(existing) if existing else 0) + 1

    @staticmethod
    def _get_next_role_family_version_number(rows: list[Any], family_id: str, role: str) -> int:
        normalized_role = str(role or "").strip().lower()
        existing = [
            int(row[13] or 1)
            for row in rows
            if len(row) >= 14
            and str(row[12] or "") == str(family_id or "")
            and str(row[3] or "").strip().lower() == normalized_role
        ]
        return (max(existing) if existing else 0) + 1

    @staticmethod
    def _get_role_family_version_total(rows: list[Any], family_id: str, role: str) -> int:
        normalized_role = str(role or "").strip().lower()
        versions = {
            int(row[13] or 1)
            for row in rows
            if len(row) >= 14
            and str(row[12] or "") == str(family_id or "")
            and str(row[3] or "").strip().lower() == normalized_role
        }
        return max(1, len(versions))

    @staticmethod
    def _conversation_messages_support_versioning(cur) -> bool:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'conversation_messages'
              AND column_name IN ('family_id', 'family_version_number', 'branch_parent_message_id', 'is_active_in_family')
            """,
        )
        available = {str(row[0]) for row in cur.fetchall()}
        return {
            'family_id',
            'family_version_number',
            'branch_parent_message_id',
            'is_active_in_family',
        }.issubset(available)

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
        branch_parent_message_id: str | None = None,
    ) -> dict[str, dict[str, Any]]:
        conn = get_db_connection()
        try:
            with conn, conn.cursor() as cur:
                if self._conversation_messages_support_versioning(cur):
                    return self._persist_versioned_messages(
                        cur,
                        user_id=user_id,
                        conversation_id=conversation_id,
                        query=query,
                        answer_text=answer_text,
                        selected_document_ids=selected_document_ids,
                        retrieval_payload=retrieval_payload,
                        model_provider=model_provider,
                        model_name=model_name,
                        prompt_version=prompt_version,
                        branch_parent_message_id=branch_parent_message_id,
                    )
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

    def _persist_versioned_messages(
        self,
        cur,
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
        branch_parent_message_id: str | None,
    ) -> dict[str, dict[str, Any]]:
        user_family_id = str(uuid.uuid4())
        assistant_family_id = str(uuid.uuid4())
        return self._insert_family_version_rows(
            cur,
            user_id=user_id,
            conversation_id=conversation_id,
            user_family_id=user_family_id,
            user_family_version_number=1,
            assistant_family_id=assistant_family_id,
            assistant_family_version_number=1,
            branch_parent_message_id=branch_parent_message_id,
            query=query,
            answer_text=answer_text,
            selected_document_ids=selected_document_ids,
            retrieval_payload=retrieval_payload,
            model_provider=model_provider,
            model_name=model_name,
            prompt_version=prompt_version,
        )

    def _append_user_family_version(
        self,
        *,
        user_id: str,
        conversation_id: str,
        user_family_id: str,
        branch_parent_message_id: str | None,
        query: str,
        answer_text: str,
        selected_document_ids: list[str],
        retrieval_payload: dict[str, Any],
        model_provider: str,
        model_name: str,
        prompt_version: str,
        next_user_version_number: int,
    ) -> dict[str, dict[str, Any]]:
        conn = get_db_connection()
        try:
            with conn, conn.cursor() as cur:
                if not self._conversation_messages_support_versioning(cur):
                    return self._replace_message_tail(
                        user_id=user_id,
                        conversation_id=conversation_id,
                        delete_message_ids=[],
                        query=query,
                        answer_text=answer_text,
                        selected_document_ids=selected_document_ids,
                        retrieval_payload=retrieval_payload,
                        model_provider=model_provider,
                        model_name=model_name,
                        prompt_version=prompt_version,
                        existing_user_row=None,
                    )
                cur.execute(
                    """
                    UPDATE conversation_messages
                    SET is_active_in_family = FALSE
                    WHERE conversation_id = %s
                      AND user_id = %s
                      AND family_id = %s
                      AND role = 'user'
                    """,
                    (conversation_id, user_id, user_family_id),
                )
                result = self._insert_family_version_rows(
                    cur,
                    user_id=user_id,
                    conversation_id=conversation_id,
                    user_family_id=user_family_id,
                    user_family_version_number=next_user_version_number,
                    assistant_family_id=str(uuid.uuid4()),
                    assistant_family_version_number=1,
                    branch_parent_message_id=branch_parent_message_id,
                    query=query,
                    answer_text=answer_text,
                    selected_document_ids=selected_document_ids,
                    retrieval_payload=retrieval_payload,
                    model_provider=model_provider,
                    model_name=model_name,
                    prompt_version=prompt_version,
                )
                cur.execute(
                    """
                    UPDATE conversations
                    SET updated_at = CURRENT_TIMESTAMP
                    WHERE conversation_id = %s
                      AND user_id = %s
                    """,
                    (conversation_id, user_id),
                )
                return result
        finally:
            conn.close()

    def _append_assistant_family_version(
        self,
        *,
        user_id: str,
        conversation_id: str,
        user_row,
        assistant_family_id: str,
        answer_text: str,
        selected_document_ids: list[str],
        retrieval_payload: dict[str, Any],
        model_provider: str,
        model_name: str,
        prompt_version: str,
        next_assistant_version_number: int,
        user_version_count: int,
    ) -> dict[str, dict[str, Any]]:
        conn = get_db_connection()
        try:
            with conn, conn.cursor() as cur:
                if not self._conversation_messages_support_versioning(cur):
                    return self._replace_message_tail(
                        user_id=user_id,
                        conversation_id=conversation_id,
                        delete_message_ids=[],
                        query=str(user_row[4] or ""),
                        answer_text=answer_text,
                        selected_document_ids=selected_document_ids,
                        retrieval_payload=retrieval_payload,
                        model_provider=model_provider,
                        model_name=model_name,
                        prompt_version=prompt_version,
                        existing_user_row=user_row,
                    )
                cur.execute(
                    """
                    UPDATE conversation_messages
                    SET is_active_in_family = FALSE
                    WHERE conversation_id = %s
                      AND user_id = %s
                      AND family_id = %s
                      AND role = 'assistant'
                    """,
                    (conversation_id, user_id, assistant_family_id),
                )
                assistant_row = self._insert_assistant_version_row(
                    cur,
                    user_id=user_id,
                    conversation_id=conversation_id,
                    user_message_id=str(user_row[0]),
                    assistant_family_id=assistant_family_id,
                    assistant_family_version_number=next_assistant_version_number,
                    answer_text=answer_text,
                    selected_document_ids=selected_document_ids,
                    retrieval_payload=retrieval_payload,
                    model_provider=model_provider,
                    model_name=model_name,
                    prompt_version=prompt_version,
                )
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
                    "user": self._serialize_message_row(
                        user_row,
                        version_count=max(1, int(user_version_count or 1)),
                    ),
                    "assistant": self._serialize_message_row(
                        assistant_row,
                        version_count=next_assistant_version_number,
                    ),
                }
        finally:
            conn.close()

    def _insert_family_version_rows(
        self,
        cur,
        *,
        user_id: str,
        conversation_id: str,
        user_family_id: str,
        user_family_version_number: int,
        assistant_family_id: str,
        assistant_family_version_number: int,
        branch_parent_message_id: str | None,
        query: str,
        answer_text: str,
        selected_document_ids: list[str],
        retrieval_payload: dict[str, Any],
        model_provider: str,
        model_name: str,
        prompt_version: str,
    ) -> dict[str, dict[str, Any]]:
        user_message_id = str(uuid.uuid4())

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
                reply_to_message_id,
                family_id,
                family_version_number,
                branch_parent_message_id,
                is_active_in_family
            )
            VALUES (%s, %s, %s, 'user', %s, %s::jsonb, NULL, NULL, NULL, NULL, NULL, %s, %s, %s, TRUE)
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
                created_at,
                family_id,
                family_version_number,
                branch_parent_message_id,
                is_active_in_family
            """,
            (
                user_message_id,
                conversation_id,
                user_id,
                query,
                Json(selected_document_ids),
                user_family_id,
                user_family_version_number,
                branch_parent_message_id,
            ),
        )
        user_row = cur.fetchone()

        assistant_row = self._insert_assistant_version_row(
            cur,
            user_id=user_id,
            conversation_id=conversation_id,
            user_message_id=user_message_id,
            assistant_family_id=assistant_family_id,
            assistant_family_version_number=assistant_family_version_number,
            answer_text=answer_text,
            selected_document_ids=selected_document_ids,
            retrieval_payload=retrieval_payload,
            model_provider=model_provider,
            model_name=model_name,
            prompt_version=prompt_version,
        )

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
            "user": self._serialize_message_row(user_row, version_count=user_family_version_number),
            "assistant": self._serialize_message_row(assistant_row, version_count=assistant_family_version_number),
        }

    def _insert_assistant_version_row(
        self,
        cur,
        *,
        user_id: str,
        conversation_id: str,
        user_message_id: str,
        assistant_family_id: str,
        assistant_family_version_number: int,
        answer_text: str,
        selected_document_ids: list[str],
        retrieval_payload: dict[str, Any],
        model_provider: str,
        model_name: str,
        prompt_version: str,
    ):
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
                reply_to_message_id,
                family_id,
                family_version_number,
                branch_parent_message_id,
                is_active_in_family
            )
            VALUES (%s, %s, %s, 'assistant', %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s, %s, NULL, TRUE)
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
                created_at,
                family_id,
                family_version_number,
                branch_parent_message_id,
                is_active_in_family
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
                assistant_family_id,
                assistant_family_version_number,
            ),
        )
        return cur.fetchone()

    def select_family_version(
        self,
        *,
        user_id: str,
        conversation_id: str,
        family_id: str,
        role: str = "",
        version_number: int,
    ) -> dict[str, Any]:
        conn = get_db_connection()
        try:
            with conn, conn.cursor() as cur:
                if not self._conversation_messages_support_versioning(cur):
                    raise ChatAnswerServiceError(
                        code="conversation_versioning_unavailable",
                        message="Message version history requires the latest conversation message migration.",
                        status_code=503,
                        details={"migration": "migrations/014_conversation_message_versioning.sql"},
                    )
                normalized_role = str(role or "").strip().lower()
                cur.execute(
                    """
                    SELECT DISTINCT role
                    FROM conversation_messages
                    WHERE conversation_id = %s
                      AND user_id = %s
                      AND family_id = %s
                      AND family_version_number = %s
                    """,
                    (conversation_id, user_id, family_id, version_number),
                )
                version_roles = [
                    str(row[0] or "").strip().lower()
                    for row in cur.fetchall()
                    if str(row[0] or "").strip()
                ]
                if normalized_role not in {"user", "assistant"} or normalized_role not in version_roles:
                    if len(version_roles) == 1:
                        normalized_role = version_roles[0]
                    else:
                        cur.execute(
                            """
                            SELECT DISTINCT role
                            FROM conversation_messages
                            WHERE conversation_id = %s
                              AND user_id = %s
                              AND family_id = %s
                            """,
                            (conversation_id, user_id, family_id),
                        )
                        family_roles = [
                            str(row[0] or "").strip().lower()
                            for row in cur.fetchall()
                            if str(row[0] or "").strip()
                        ]
                        if len(family_roles) == 1:
                            normalized_role = family_roles[0]
                        elif not version_roles:
                            raise ChatAnswerServiceError(
                                code="version_not_found",
                                message="The selected message version does not exist.",
                                status_code=404,
                            )
                        else:
                            raise ChatAnswerServiceError(
                                code="ambiguous_family_role",
                                message="The selected message version could not be matched to a specific role.",
                                status_code=400,
                            )
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM conversation_messages
                    WHERE conversation_id = %s
                      AND user_id = %s
                      AND family_id = %s
                      AND role = %s
                      AND family_version_number = %s
                    """,
                    (conversation_id, user_id, family_id, normalized_role, version_number),
                )
                if int(cur.fetchone()[0] or 0) == 0:
                    raise ChatAnswerServiceError(
                        code="version_not_found",
                        message="The selected message version does not exist.",
                        status_code=404,
                    )
                cur.execute(
                    """
                    UPDATE conversation_messages
                    SET is_active_in_family = CASE WHEN family_version_number = %s THEN TRUE ELSE FALSE END
                    WHERE conversation_id = %s
                      AND user_id = %s
                      AND family_id = %s
                      AND role = %s
                    """,
                    (version_number, conversation_id, user_id, family_id, normalized_role),
                )
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
                    "family_id": family_id,
                    "role": normalized_role,
                    "version_number": version_number,
                }
        finally:
            conn.close()

    def _replace_message_tail(
        self,
        *,
        user_id: str,
        conversation_id: str,
        delete_message_ids: list[str],
        query: str,
        answer_text: str,
        selected_document_ids: list[str],
        retrieval_payload: dict[str, Any],
        model_provider: str,
        model_name: str,
        prompt_version: str,
        existing_user_row=None,
    ) -> dict[str, dict[str, Any]]:
        conn = get_db_connection()
        try:
            with conn, conn.cursor() as cur:
                if delete_message_ids:
                    cur.execute(
                        """
                        DELETE FROM conversation_messages
                        WHERE conversation_id = %s
                          AND user_id = %s
                          AND message_id = ANY(%s::uuid[])
                        """,
                        (conversation_id, user_id, delete_message_ids),
                    )

                if existing_user_row is None:
                    user_message_id = str(uuid.uuid4())
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
                else:
                    user_row = existing_user_row
                    user_message_id = str(existing_user_row[0])

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
                details={"migration": "migrations/010_conversation_messages.sql"},
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
    def _serialize_message_row(row, version_count: int | None = None) -> dict[str, Any]:
        selected_document_ids = row[5] if isinstance(row[5], list) else []
        retrieval_payload = row[6] if isinstance(row[6], dict) else None
        citations = retrieval_payload.get("citations") if isinstance(retrieval_payload, dict) else []
        citations = citations if isinstance(citations, list) else []
        family_id = str(row[12]) if len(row) >= 13 and row[12] else str(row[10] or row[0])
        version_index = int(row[13] or 1) if len(row) >= 14 else 1
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
            "family_id": family_id,
            "version_index": version_index,
            "version_count": int(version_count or version_index or 1),
            "branch_parent_message_id": str(row[14]) if len(row) >= 15 and row[14] else None,
        }
