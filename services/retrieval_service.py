from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any

from db import get_db_connection
from services.embedding_service import EmbeddingService, EmbeddingServiceError


FILTERED_SECTION_MARKERS = (
    "abstract",
    "references",
    "bibliography",
    "works cited",
    "literature cited",
)


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
        include_filtered: bool = False,
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
        include_filtered = self._parse_include_filtered(include_filtered)
        scoped_document_ids = self._resolve_scoped_document_ids(
            user_id=user_id,
            conversation_id=conversation_id,
            requested_document_ids=requested_document_ids,
        )

        if not scoped_document_ids:
            return self._build_empty_payload(
                query=normalized_query,
                k=parsed_k,
                strategy="vector",
                include_filtered=include_filtered,
            )

        try:
            query_vector = self.embedding_service.embed_texts([normalized_query])[0]
            payload = self._retrieve_ranked_vector_results(
                query=normalized_query,
                parsed_k=parsed_k,
                scoped_document_ids=scoped_document_ids,
                query_vector=query_vector,
                include_filtered=include_filtered,
            )
            payload["strategy"] = "vector"
            return payload
        except EmbeddingServiceError as exc:
            if not self._should_use_keyword_fallback(exc):
                raise
            payload = self._retrieve_ranked_keyword_results(
                query=normalized_query,
                parsed_k=parsed_k,
                scoped_document_ids=scoped_document_ids,
                include_filtered=include_filtered,
            )
            payload["strategy"] = "keyword_fallback"
            payload["fallback_reason"] = exc.to_dict()
            return payload
        except Exception as exc:  # pragma: no cover - defensive wrapper
            raise RetrievalServiceError(
                code="embedding_failed",
                message="Unable to embed retrieval query.",
                status_code=503,
            ) from exc

    def _resolve_scoped_document_ids(
        self,
        *,
        user_id: str,
        conversation_id: str,
        requested_document_ids: list[str],
    ) -> list[str]:
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
                allowed_document_ids = [str(row[0]) for row in cur.fetchall()]
        finally:
            conn.close()

        allowed_document_id_set = set(allowed_document_ids)
        if requested_document_ids:
            invalid_document_ids = [
                document_id
                for document_id in requested_document_ids
                if document_id not in allowed_document_id_set
            ]
            if invalid_document_ids:
                raise RetrievalServiceError(
                    code="invalid_document_scope",
                    message="One or more document_ids are not part of this conversation.",
                    status_code=400,
                    details={"invalid_document_ids": invalid_document_ids},
                )
            return requested_document_ids

        return allowed_document_ids

    def _retrieve_ranked_vector_results(
        self,
        *,
        query: str,
        parsed_k: int,
        scoped_document_ids: list[str],
        query_vector: list[float],
        include_filtered: bool,
    ) -> dict[str, Any]:
        query_vector_literal = self._vector_literal(query_vector)
        counts = self._collect_candidate_counts(
            scoped_document_ids=scoped_document_ids,
            include_filtered=include_filtered,
            require_embedding=True,
        )
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        db.block_id::text,
                        db.document_id::text,
                        d.original_filename,
                        db.block_type,
                        db.subtype,
                        db.normalized_content,
                        COALESCE(db.source_metadata, '{}'::jsonb) AS source_metadata,
                        db.normalized_content->>'retrieval_text' AS retrieval_text,
                        GREATEST(0.0, LEAST(1.0, ((1 - (dbe.embedding <=> %s::vector)) + 1.0) / 2.0)) AS score
                    FROM document_blocks db
                    JOIN document_block_embeddings dbe ON dbe.block_id = db.block_id
                    JOIN documents d                  ON d.document_id  = db.document_id
                    WHERE db.document_id = ANY(%s::uuid[])
                      AND db.embedding_status = 'embedded'
                      AND d.is_deleted = FALSE
                      AND NULLIF(BTRIM(db.normalized_content->>'retrieval_text'), '') IS NOT NULL
                      AND (
                        %s = TRUE
                        OR NOT (
                            (db.block_type = 'text' AND COALESCE(db.normalized_content->>'text_role', '') = 'note')
                            OR EXISTS (
                                SELECT 1
                                FROM jsonb_array_elements_text(COALESCE(db.normalized_content->'section_path', '[]'::jsonb)) AS sp(value)
                                WHERE lower(sp.value) LIKE 'abstract%%'
                                   OR lower(sp.value) LIKE 'references%%'
                                   OR lower(sp.value) LIKE 'bibliography%%'
                                   OR lower(sp.value) LIKE 'works cited%%'
                                   OR lower(sp.value) LIKE 'literature cited%%'
                            )
                        )
                      )
                    ORDER BY dbe.embedding <=> %s::vector ASC
                    LIMIT %s
                    """,
                    (query_vector_literal, scoped_document_ids, include_filtered, query_vector_literal, parsed_k),
                )
                rows = cur.fetchall()
        finally:
            conn.close()

        return self._build_payload(
            query=query,
            parsed_k=parsed_k,
            include_filtered=include_filtered,
            rows=rows,
            counts=counts,
            strategy="vector",
        )

    def _retrieve_ranked_keyword_results(
        self,
        *,
        query: str,
        parsed_k: int,
        scoped_document_ids: list[str],
        include_filtered: bool,
    ) -> dict[str, Any]:
        counts = self._collect_candidate_counts(
            scoped_document_ids=scoped_document_ids,
            include_filtered=include_filtered,
            require_embedding=False,
        )
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH query_terms AS (
                        SELECT plainto_tsquery('english', %s) AS ts_query
                    )
                    SELECT
                        db.block_id::text,
                        db.document_id::text,
                        d.original_filename,
                        db.block_type,
                        db.subtype,
                        db.normalized_content,
                        COALESCE(db.source_metadata, '{}'::jsonb) AS source_metadata,
                        db.normalized_content->>'retrieval_text' AS retrieval_text,
                        LEAST(
                            1.0,
                            ts_rank_cd(
                                to_tsvector('english', db.normalized_content->>'retrieval_text'),
                                query_terms.ts_query
                            )
                        ) AS score
                    FROM document_blocks db
                    JOIN documents d ON d.document_id = db.document_id
                    CROSS JOIN query_terms
                    WHERE db.document_id = ANY(%s::uuid[])
                      AND d.is_deleted = FALSE
                      AND NULLIF(BTRIM(db.normalized_content->>'retrieval_text'), '') IS NOT NULL
                      AND query_terms.ts_query @@ to_tsvector('english', db.normalized_content->>'retrieval_text')
                      AND (
                        %s = TRUE
                        OR NOT (
                            (db.block_type = 'text' AND COALESCE(db.normalized_content->>'text_role', '') = 'note')
                            OR EXISTS (
                                SELECT 1
                                FROM jsonb_array_elements_text(COALESCE(db.normalized_content->'section_path', '[]'::jsonb)) AS sp(value)
                                WHERE lower(sp.value) LIKE 'abstract%%'
                                   OR lower(sp.value) LIKE 'references%%'
                                   OR lower(sp.value) LIKE 'bibliography%%'
                                   OR lower(sp.value) LIKE 'works cited%%'
                                   OR lower(sp.value) LIKE 'literature cited%%'
                            )
                        )
                      )
                    ORDER BY score DESC, d.original_filename ASC, db.block_id ASC
                    LIMIT %s
                    """,
                    (query, scoped_document_ids, include_filtered, parsed_k),
                )
                rows = cur.fetchall()
        finally:
            conn.close()

        return self._build_payload(
            query=query,
            parsed_k=parsed_k,
            include_filtered=include_filtered,
            rows=rows,
            counts=counts,
            strategy="keyword_fallback",
        )

    def _collect_candidate_counts(
        self,
        *,
        scoped_document_ids: list[str],
        include_filtered: bool,
        require_embedding: bool,
    ) -> dict[str, int | bool]:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                embedding_join = "JOIN document_block_embeddings dbe ON dbe.block_id = db.block_id" if require_embedding else ""
                embedding_predicate = "AND db.embedding_status = 'embedded'" if require_embedding else ""
                cur.execute(
                    f"""
                    SELECT
                        COUNT(*)::int AS total_candidates,
                        COUNT(*) FILTER (
                            WHERE NOT (
                                (db.block_type = 'text' AND COALESCE(db.normalized_content->>'text_role', '') = 'note')
                                OR EXISTS (
                                    SELECT 1
                                    FROM jsonb_array_elements_text(COALESCE(db.normalized_content->'section_path', '[]'::jsonb)) AS sp(value)
                                    WHERE lower(sp.value) LIKE 'abstract%%'
                                       OR lower(sp.value) LIKE 'references%%'
                                       OR lower(sp.value) LIKE 'bibliography%%'
                                       OR lower(sp.value) LIKE 'works cited%%'
                                       OR lower(sp.value) LIKE 'literature cited%%'
                                )
                            )
                        )::int AS included_candidates
                    FROM document_blocks db
                    {embedding_join}
                    JOIN documents d ON d.document_id = db.document_id
                    WHERE db.document_id = ANY(%s::uuid[])
                      AND d.is_deleted = FALSE
                      {embedding_predicate}
                      AND NULLIF(BTRIM(db.normalized_content->>'retrieval_text'), '') IS NOT NULL
                    """,
                    (scoped_document_ids,),
                )
                row = cur.fetchone() or (0, 0)
        finally:
            conn.close()

        total_candidates = int(row[0] or 0)
        included_candidates = int(row[1] or 0)
        excluded_candidates = max(0, total_candidates - included_candidates)
        visible_candidates = total_candidates if include_filtered else included_candidates
        return {
            "total_candidate_count": total_candidates,
            "included_candidate_count": included_candidates,
            "excluded_candidate_count": excluded_candidates,
            "visible_candidate_count": visible_candidates,
            "include_filtered": include_filtered,
        }

    def _build_payload(
        self,
        *,
        query: str,
        parsed_k: int,
        include_filtered: bool,
        rows,
        counts: dict[str, int | bool],
        strategy: str,
    ) -> dict[str, Any]:
        results = [
            self._serialize_result_row(row)
            for row in rows or []
        ]
        return {
            "query": query,
            "k": parsed_k,
            "strategy": strategy,
            "returned_count": len(results),
            "filter_summary": counts,
            "results": results,
            "include_filtered": include_filtered,
        }

    def _build_empty_payload(
        self,
        *,
        query: str,
        k: int,
        strategy: str,
        include_filtered: bool,
    ) -> dict[str, Any]:
        return {
            "query": query,
            "k": k,
            "strategy": strategy,
            "returned_count": 0,
            "filter_summary": {
                "total_candidate_count": 0,
                "included_candidate_count": 0,
                "excluded_candidate_count": 0,
                "visible_candidate_count": 0,
                "include_filtered": include_filtered,
            },
            "results": [],
            "include_filtered": include_filtered,
        }

    def _serialize_result_row(self, row) -> dict[str, Any]:
        block_id, document_id, document_name, block_type, subtype, normalized_content, source_metadata, retrieval_text, score = row
        normalized_content = normalized_content if isinstance(normalized_content, dict) else {}
        source_metadata = source_metadata if isinstance(source_metadata, dict) else {}
        text_role = str(normalized_content.get("text_role") or "").strip().lower()
        section_path = [
            str(item).strip()
            for item in (normalized_content.get("section_path") or [])
            if str(item).strip()
        ]
        filter_reason = self._detect_filter_reason(text_role=text_role, section_path=section_path)
        return {
            "block_id": block_id,
            "document_id": document_id,
            "document_name": document_name or "",
            "score": float(score) if score is not None else 0.0,
            "snippet": self._truncate_snippet(retrieval_text),
            "block_type": str(block_type or "").strip().lower(),
            "subtype": str(subtype or "").strip().lower(),
            "text_role": text_role,
            "section_path": section_path,
            "source_metadata": source_metadata,
            "is_filtered": bool(filter_reason),
            "filter_reason": filter_reason,
            "relevance_reason": self._build_relevance_reason(
                block_type=str(block_type or "").strip().lower(),
                subtype=str(subtype or "").strip().lower(),
                text_role=text_role,
                section_path=section_path,
                filter_reason=filter_reason,
            ),
        }

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
    def _parse_include_filtered(value: bool | str | None) -> bool:
        if isinstance(value, bool):
            return value
        return str(value or "").strip().lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _vector_literal(vector: list[float]) -> str:
        return "[" + ",".join(str(float(value)) for value in vector) + "]"

    def _truncate_snippet(self, snippet: str | None) -> str:
        normalized = (snippet or "").strip()
        if len(normalized) <= self.snippet_max_chars:
            return normalized
        return normalized[: self.snippet_max_chars].rstrip() + "..."

    @staticmethod
    def _detect_filter_reason(*, text_role: str, section_path: list[str]) -> str:
        if text_role == "note":
            return "reference_note"
        normalized_sections = [item.strip().lower() for item in section_path if item]
        for section in normalized_sections:
            for marker in FILTERED_SECTION_MARKERS:
                if section.startswith(marker):
                    return marker.replace(" ", "_")
        return ""

    @staticmethod
    def _build_relevance_reason(
        *,
        block_type: str,
        subtype: str,
        text_role: str,
        section_path: list[str],
        filter_reason: str,
    ) -> str:
        if filter_reason == "reference_note":
            return "filtered reference note"
        if filter_reason:
            return f"filtered {filter_reason.replace('_', ' ')}"
        if block_type == "table":
            return "matched table context"
        if block_type == "diagram":
            return "matched diagram caption or nearby context"
        if text_role == "caption":
            return "matched caption text"
        if text_role == "heading":
            return "matched section heading"
        if section_path:
            return "matched body paragraph within section"
        if subtype:
            return f"matched {subtype}"
        return "matched body paragraph"

    @staticmethod
    def _should_use_keyword_fallback(exc: EmbeddingServiceError) -> bool:
        if not exc.retryable:
            return False
        return exc.code in {
            "provider_local_error",
            "provider_connection_error",
            "provider_timeout",
            "provider_http_error",
        }
