from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any

from db import get_db_connection
from services.embedding_service import EmbeddingService, EmbeddingServiceError


@dataclass
class RetrievalServiceError(Exception):
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


class RetrievalService:
    def __init__(self) -> None:
        default_k = max(1, int(os.getenv("RETRIEVAL_DEFAULT_K", "5")))
        max_k = max(default_k, int(os.getenv("RETRIEVAL_MAX_K", "20")))
        snippet_max = max(120, int(os.getenv("RETRIEVAL_SNIPPET_MAX_CHARS", "450")))

        self.default_k = default_k
        self.max_k = max_k
        self.snippet_max_chars = snippet_max
        self.embedding_service = EmbeddingService()

    def retrieve_conversation_blocks(
        self,
        *,
        user_id: str,
        conversation_id: str,
        query: str,
        k: int | None = None,
        document_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        normalized_query = str(query or "").strip()
        if not normalized_query:
            raise RetrievalServiceError(
                code="empty_query",
                message="query is required.",
                status_code=400,
            )

        parsed_k = self._parse_k(k)
        requested_document_ids = self._normalize_document_ids(document_ids)

        try:
            query_vector = self.embedding_service.embed_texts([normalized_query])[0]
        except EmbeddingServiceError:
            raise
        except Exception as exc:  # pragma: no cover - defensive wrapper
            raise RetrievalServiceError(
                code="embedding_failed",
                message="Unable to embed retrieval query.",
                status_code=503,
            ) from exc

        query_vector_literal = self._vector_literal(query_vector)

        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT d.document_id::text
                    FROM conversations c
                    JOIN conversation_documents cd ON cd.conversation_id = c.conversation_id
                    JOIN documents d              ON d.document_id       = cd.document_id
                    WHERE c.conversation_id = %s
                      AND c.user_id = %s
                      AND d.is_deleted = FALSE
                    """,
                    (conversation_id, user_id),
                )
                allowed_document_ids = {str(row[0]) for row in cur.fetchall()}

                if requested_document_ids:
                    invalid_document_ids = [
                        document_id
                        for document_id in requested_document_ids
                        if document_id not in allowed_document_ids
                    ]
                    if invalid_document_ids:
                        raise RetrievalServiceError(
                            code="invalid_document_scope",
                            message="One or more document_ids are not part of this conversation.",
                            status_code=400,
                            details={"invalid_document_ids": invalid_document_ids},
                        )
                    scoped_document_ids = requested_document_ids
                else:
                    scoped_document_ids = list(allowed_document_ids)

                if not scoped_document_ids:
                    return {
                        "query": normalized_query,
                        "k": parsed_k,
                        "results": [],
                    }

                cur.execute(
                    """
                    SELECT
                        db.block_id::text,
                        db.document_id::text,
                        GREATEST(0.0, LEAST(1.0, ((1 - (dbe.embedding <=> %s::vector)) + 1.0) / 2.0)) AS score,
                        db.normalized_content->>'retrieval_text' AS retrieval_text,
                        COALESCE(db.source_metadata, '{}'::jsonb) AS source_metadata
                    FROM document_blocks db
                    JOIN document_block_embeddings dbe ON dbe.block_id = db.block_id
                    WHERE db.document_id = ANY(%s::uuid[])
                      AND db.embedding_status = 'embedded'
                      AND NULLIF(BTRIM(db.normalized_content->>'retrieval_text'), '') IS NOT NULL
                    ORDER BY dbe.embedding <=> %s::vector ASC
                    LIMIT %s
                    """,
                    (query_vector_literal, scoped_document_ids, query_vector_literal, parsed_k),
                )
                rows = cur.fetchall()

            if not rows:
                return {
                    "query": normalized_query,
                    "k": parsed_k,
                    "results": [],
                }

            return {
                "query": normalized_query,
                "k": parsed_k,
                "results": [
                    {
                        "block_id": block_id,
                        "document_id": document_id,
                        "score": float(score) if score is not None else 0.0,
                        "snippet": self._truncate_snippet(retrieval_text),
                        "source_metadata": source_metadata or {},
                    }
                    for block_id, document_id, score, retrieval_text, source_metadata in rows
                ],
            }
        finally:
            conn.close()

    def _parse_k(self, value: int | str | None) -> int:
        if value in (None, ""):
            return self.default_k
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise RetrievalServiceError(
                code="invalid_k",
                message="k must be an integer.",
                status_code=400,
            ) from exc
        if parsed < 1:
            raise RetrievalServiceError(
                code="invalid_k",
                message="k must be at least 1.",
                status_code=400,
            )
        if parsed > self.max_k:
            raise RetrievalServiceError(
                code="invalid_k",
                message=f"k must be less than or equal to {self.max_k}.",
                status_code=400,
            )
        return parsed

    @staticmethod
    def _normalize_document_ids(raw_document_ids: list[str] | None) -> list[str]:
        if raw_document_ids is None:
            return []
        if not isinstance(raw_document_ids, list):
            raise RetrievalServiceError(
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
    def _vector_literal(vector: list[float]) -> str:
        return "[" + ",".join(str(float(value)) for value in vector) + "]"

    def _truncate_snippet(self, snippet: str | None) -> str:
        normalized = (snippet or "").strip()
        if len(normalized) <= self.snippet_max_chars:
            return normalized
        return normalized[: self.snippet_max_chars].rstrip() + "…"
