from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any

from db import get_db_connection
from services.embedding_service import EmbeddingService, EmbeddingServiceError
from services.qdrant_index_service import QdrantIndexService, QdrantServiceError
from services.reranker_service import RerankerService, RerankerServiceError


logger = logging.getLogger(__name__)

FILTERED_SECTION_MARKERS = (
    "abstract",
    "references",
    "bibliography",
    "works cited",
    "literature cited",
)

BACKEND_POSTGRES_HYBRID = "postgres_hybrid"
BACKEND_QDRANT_RRF_RERANK = "qdrant_rrf_rerank"


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
        storage_dimension = max(1, int(os.getenv("EMBEDDING_STORAGE_DIMENSION", "1536")))
        candidate_multiplier = max(1, int(os.getenv("RETRIEVAL_HYBRID_CANDIDATE_MULTIPLIER", "3")))
        candidate_floor = max(1, int(os.getenv("RETRIEVAL_HYBRID_CANDIDATE_FLOOR", "15")))
        candidate_cap = max(candidate_floor, int(os.getenv("RETRIEVAL_HYBRID_CANDIDATE_CAP", "50")))
        vector_weight = float(os.getenv("RETRIEVAL_HYBRID_VECTOR_WEIGHT", "0.7"))
        keyword_weight = float(os.getenv("RETRIEVAL_HYBRID_KEYWORD_WEIGHT", "0.3"))
        total_weight = vector_weight + keyword_weight

        self.default_k = default_k
        self.max_k = max_k
        self.snippet_max_chars = snippet_max
        self.storage_dimension = storage_dimension
        self.candidate_multiplier = candidate_multiplier
        self.candidate_floor = candidate_floor
        self.candidate_cap = candidate_cap
        self.vector_weight = (vector_weight / total_weight) if total_weight > 0 else 0.7
        self.keyword_weight = (keyword_weight / total_weight) if total_weight > 0 else 0.3
        self.embedding_service = EmbeddingService()
        self.qdrant_service = QdrantIndexService()
        self.reranker_service = RerankerService()
        self.backend = str(os.getenv("RETRIEVAL_BACKEND") or BACKEND_POSTGRES_HYBRID).strip() or BACKEND_POSTGRES_HYBRID
        self.query_normalization_enabled = self._is_truthy_env(os.getenv("QUERY_NORMALIZATION_ENABLED", "1"))
        self.dense_top_n = max(1, int(os.getenv("RETRIEVAL_DENSE_TOP_N", "50")))
        self.sparse_top_n = max(1, int(os.getenv("RETRIEVAL_SPARSE_TOP_N", "50")))
        self.fusion_top_n = max(1, int(os.getenv("RETRIEVAL_FUSION_TOP_N", "100")))
        self.final_k_default = max(1, int(os.getenv("RETRIEVAL_FINAL_K", str(default_k))))
        self.rrf_k = max(1, int(os.getenv("RETRIEVAL_RRF_K", "60")))
        self.single_document_top_k = max(1, int(os.getenv("RETRIEVAL_SINGLE_DOCUMENT_TOP_K", "10")))
        self.multi_document_top_k_per_doc = max(1, int(os.getenv("RETRIEVAL_MULTI_DOCUMENT_TOP_K_PER_DOC", "5")))

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
        normalized_query = self._normalize_query(query)
        if not normalized_query:
            raise RetrievalServiceError(code="empty_query", message="query is required.", status_code=400)

        requested_k = self._parse_k(k)
        requested_document_ids = self._normalize_document_ids(document_ids)
        include_filtered = self._parse_include_filtered(include_filtered)
        scoped_document_ids = self._resolve_scoped_document_ids(
            user_id=user_id,
            conversation_id=conversation_id,
            requested_document_ids=requested_document_ids,
        )
        result_limits = self._resolve_result_limits(
            requested_k=requested_k,
            scoped_document_ids=scoped_document_ids,
        )
        final_k = int(result_limits["target_k"])

        if not scoped_document_ids:
            return self._build_empty_payload(query=normalized_query, k=final_k, strategy=self.backend, include_filtered=include_filtered)

        counts = self._collect_candidate_counts(
            scoped_document_ids=scoped_document_ids,
            include_filtered=include_filtered,
            require_embedding=False,
        )
        normalized_query_vector = self._embed_query_or_keyword_fallback(
            query=normalized_query,
            parsed_k=final_k,
            scoped_document_ids=scoped_document_ids,
            include_filtered=include_filtered,
        )
        if isinstance(normalized_query_vector, dict):
            normalized_query_vector["requested_k"] = requested_k
            return normalized_query_vector

        lexical_query = normalized_query.lower()
        if self.backend == BACKEND_QDRANT_RRF_RERANK:
            try:
                payload = self._retrieve_qdrant_rrf_rerank(
                    query=normalized_query,
                    lexical_query=lexical_query,
                    query_vector=normalized_query_vector,
                    parsed_k=final_k,
                    per_document_limit=result_limits["per_document_limit"],
                    scoped_document_ids=scoped_document_ids,
                    include_filtered=include_filtered,
                    counts=counts,
                )
                payload["requested_k"] = requested_k
                return payload
            except QdrantServiceError as exc:
                logger.warning("Advanced retrieval backend unavailable; falling back to postgres_hybrid.")
                fallback_payload = self._retrieve_postgres_hybrid(
                    query=normalized_query,
                    lexical_query=lexical_query,
                    query_vector=normalized_query_vector,
                    parsed_k=final_k,
                    per_document_limit=result_limits["per_document_limit"],
                    scoped_document_ids=scoped_document_ids,
                    include_filtered=include_filtered,
                    counts=counts,
                )
                fallback_payload["fallback_reason"] = exc.to_dict()
                fallback_payload["requested_k"] = requested_k
                return fallback_payload

        payload = self._retrieve_postgres_hybrid(
            query=normalized_query,
            lexical_query=lexical_query,
            query_vector=normalized_query_vector,
            parsed_k=final_k,
            per_document_limit=result_limits["per_document_limit"],
            scoped_document_ids=scoped_document_ids,
            include_filtered=include_filtered,
            counts=counts,
        )
        payload["requested_k"] = requested_k
        return payload

    def _embed_query_or_keyword_fallback(
        self,
        *,
        query: str,
        parsed_k: int,
        scoped_document_ids: list[str],
        include_filtered: bool,
    ) -> list[float] | dict[str, Any]:
        try:
            query_vectors = self.embedding_service.embed_texts([query])
        except EmbeddingServiceError as exc:
            return self._build_keyword_fallback_payload(
                query=query,
                parsed_k=parsed_k,
                scoped_document_ids=scoped_document_ids,
                include_filtered=include_filtered,
                fallback_reason=exc.to_dict(),
            )
        except Exception as exc:  # pragma: no cover
            return self._build_keyword_fallback_payload(
                query=query,
                parsed_k=parsed_k,
                scoped_document_ids=scoped_document_ids,
                include_filtered=include_filtered,
                fallback_reason={
                    "code": "embedding_failed",
                    "message": "Unable to embed retrieval query.",
                    "status_code": 503,
                    "details": {"exception_type": type(exc).__name__},
                },
            )

        if not query_vectors or not isinstance(query_vectors[0], list) or not query_vectors[0]:
            return self._build_keyword_fallback_payload(
                query=query,
                parsed_k=parsed_k,
                scoped_document_ids=scoped_document_ids,
                include_filtered=include_filtered,
                fallback_reason={
                    "code": "embedding_missing_vector",
                    "message": "Embedding provider returned no query vector.",
                    "status_code": 503,
                    "details": {},
                },
            )
        return self._normalize_query_vector_for_storage(query_vectors[0])

    def _retrieve_qdrant_rrf_rerank(
        self,
        *,
        query: str,
        lexical_query: str,
        query_vector: list[float],
        parsed_k: int,
        per_document_limit: int | None,
        scoped_document_ids: list[str],
        include_filtered: bool,
        counts: dict[str, int | bool],
    ) -> dict[str, Any]:
        self.qdrant_service.ensure_collection(vector_size=self.storage_dimension)
        dense_rows = self._fetch_ranked_qdrant_candidates(
            query_vector=query_vector,
            scoped_document_ids=scoped_document_ids,
            limit=self.dense_top_n,
        )
        sparse_rows = self._fetch_ranked_keyword_candidates(
            query=lexical_query,
            scoped_document_ids=scoped_document_ids,
            limit=self.sparse_top_n,
        )
        fused_rows = self._build_rrf_candidates(
            dense_rows=dense_rows,
            sparse_rows=sparse_rows,
            include_filtered=include_filtered,
            fusion_limit=self.fusion_top_n,
        )
        reranker_fallback = None
        try:
            reranked_rows, reranker_summary = self._rerank_candidates(query=query, rows=fused_rows, parsed_k=parsed_k)
        except RerankerServiceError as exc:
            reranker_fallback = exc.to_dict()
            reranked_rows = [
                {
                    **row,
                    "rerank_score": float(row.get("rrf_score") or 0.0),
                    "score": float(row.get("rrf_score") or 0.0),
                }
                for row in fused_rows[:parsed_k]
            ]
            reranker_summary = {"reranker_applied": False, "reranker_fallback": True}
        reranked_rows = self._apply_result_limits(
            rows=reranked_rows,
            target_k=parsed_k,
            per_document_limit=per_document_limit,
        )
        payload = self._build_payload(
            query=query,
            parsed_k=parsed_k,
            include_filtered=include_filtered,
            rows=reranked_rows,
            counts=counts,
            strategy=BACKEND_QDRANT_RRF_RERANK,
            candidate_summary={
                "dense_candidate_count": len(dense_rows),
                "sparse_candidate_count": len(sparse_rows),
                "fused_candidate_count": len(fused_rows),
                "reranked_candidate_count": len(reranked_rows),
                **reranker_summary,
            },
        )
        if reranker_fallback:
            payload["reranker_fallback_reason"] = reranker_fallback
        return payload

    def _retrieve_postgres_hybrid(
        self,
        *,
        query: str,
        lexical_query: str,
        query_vector: list[float],
        parsed_k: int,
        per_document_limit: int | None,
        scoped_document_ids: list[str],
        include_filtered: bool,
        counts: dict[str, int | bool],
    ) -> dict[str, Any]:
        candidate_limit = self._candidate_pool_limit(parsed_k)
        vector_rows = self._fetch_ranked_vector_candidates(
            scoped_document_ids=scoped_document_ids,
            query_vector=query_vector,
            limit=candidate_limit,
        )
        keyword_rows = self._fetch_ranked_keyword_candidates(
            query=lexical_query,
            scoped_document_ids=scoped_document_ids,
            limit=candidate_limit,
        )
        fused_rows = self._build_postgres_hybrid_ranked_results(
            vector_rows=vector_rows,
            keyword_rows=keyword_rows,
            parsed_k=parsed_k,
            include_filtered=include_filtered,
        )
        fused_rows = self._apply_result_limits(
            rows=fused_rows,
            target_k=parsed_k,
            per_document_limit=per_document_limit,
        )
        return self._build_payload(
            query=query,
            parsed_k=parsed_k,
            include_filtered=include_filtered,
            rows=fused_rows,
            counts=counts,
            strategy=BACKEND_POSTGRES_HYBRID,
            candidate_summary={
                "vector_candidate_count": len(vector_rows),
                "keyword_candidate_count": len(keyword_rows),
                "fused_candidate_count": len(fused_rows),
            },
        )

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

    def _fetch_ranked_qdrant_candidates(
        self,
        *,
        query_vector: list[float],
        scoped_document_ids: list[str],
        limit: int,
    ) -> list[dict[str, Any]]:
        hits = self.qdrant_service.search(
            query_vector=query_vector,
            document_ids=scoped_document_ids,
            limit=limit,
        )
        rows: list[dict[str, Any]] = []
        for index, hit in enumerate(hits, start=1):
            payload = hit.get("payload") if isinstance(hit, dict) else {}
            payload = payload if isinstance(payload, dict) else {}
            rows.append(
                {
                    "block_id": str(payload.get("block_id") or hit.get("id") or ""),
                    "document_id": str(payload.get("document_id") or ""),
                    "document_name": payload.get("document_name") or "",
                    "block_type": str(payload.get("block_type") or "").strip().lower(),
                    "subtype": str(payload.get("subtype") or "").strip().lower(),
                    "normalized_content": {
                        "text_role": str(payload.get("text_role") or "").strip().lower(),
                        "section_path": payload.get("section_path") if isinstance(payload.get("section_path"), list) else [],
                    },
                    "source_metadata": payload.get("source_metadata") if isinstance(payload.get("source_metadata"), dict) else {},
                    "retrieval_text": payload.get("retrieval_text") or "",
                    "dense_score": max(0.0, min(1.0, float(hit.get("score") or 0.0))),
                    "dense_rank": index,
                    "match_sources": ["dense"],
                }
            )
        return rows

    def _fetch_ranked_vector_candidates(
        self,
        *,
        scoped_document_ids: list[str],
        query_vector: list[float],
        limit: int,
    ) -> list[dict[str, Any]]:
        query_vector_literal = self._vector_literal(query_vector)
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
                        GREATEST(0.0, LEAST(1.0, ((1 - (dbe.embedding <=> %s::vector)) + 1.0) / 2.0)) AS vector_score
                    FROM document_blocks db
                    JOIN document_block_embeddings dbe ON dbe.block_id = db.block_id
                    JOIN documents d                  ON d.document_id  = db.document_id
                    WHERE db.document_id = ANY(%s::uuid[])
                      AND db.embedding_status = 'embedded'
                      AND d.is_deleted = FALSE
                      AND NULLIF(BTRIM(db.normalized_content->>'retrieval_text'), '') IS NOT NULL
                    ORDER BY dbe.embedding <=> %s::vector ASC
                    LIMIT %s
                    """,
                    (query_vector_literal, scoped_document_ids, query_vector_literal, limit),
                )
                rows = cur.fetchall()
        finally:
            conn.close()

        results: list[dict[str, Any]] = []
        for index, row in enumerate(rows or [], start=1):
            payload = self._candidate_row_from_tuple(row, match_source="vector")
            payload["vector_rank"] = index
            results.append(payload)
        return results

    def _fetch_ranked_keyword_candidates(
        self,
        *,
        query: str,
        scoped_document_ids: list[str],
        limit: int,
    ) -> list[dict[str, Any]]:
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
                        ) AS keyword_score
                    FROM document_blocks db
                    JOIN documents d ON d.document_id = db.document_id
                    CROSS JOIN query_terms
                    WHERE db.document_id = ANY(%s::uuid[])
                      AND d.is_deleted = FALSE
                      AND NULLIF(BTRIM(db.normalized_content->>'retrieval_text'), '') IS NOT NULL
                      AND query_terms.ts_query @@ to_tsvector('english', db.normalized_content->>'retrieval_text')
                    ORDER BY keyword_score DESC, d.original_filename ASC, db.block_id ASC
                    LIMIT %s
                    """,
                    (query, scoped_document_ids, limit),
                )
                rows = cur.fetchall()
        finally:
            conn.close()

        results: list[dict[str, Any]] = []
        for index, row in enumerate(rows or [], start=1):
            payload = self._candidate_row_from_tuple(row, match_source="keyword")
            payload["sparse_rank"] = index
            results.append(payload)
        return results

    def _build_keyword_fallback_payload(
        self,
        *,
        query: str,
        parsed_k: int,
        scoped_document_ids: list[str],
        include_filtered: bool,
        fallback_reason: dict[str, Any],
    ) -> dict[str, Any]:
        counts = self._collect_candidate_counts(
            scoped_document_ids=scoped_document_ids,
            include_filtered=include_filtered,
            require_embedding=False,
        )
        result_limits = self._resolve_result_limits(
            requested_k=parsed_k,
            scoped_document_ids=scoped_document_ids,
        )
        keyword_rows = self._fetch_ranked_keyword_candidates(
            query=query.lower(),
            scoped_document_ids=scoped_document_ids,
            limit=self._candidate_pool_limit(parsed_k),
        )
        ranked_rows = self._build_keyword_only_ranked_results(
            keyword_rows=keyword_rows,
            parsed_k=parsed_k,
            include_filtered=include_filtered,
        )
        ranked_rows = self._apply_result_limits(
            rows=ranked_rows,
            target_k=int(result_limits["target_k"]),
            per_document_limit=result_limits["per_document_limit"],
        )
        payload = self._build_payload(
            query=query,
            parsed_k=int(result_limits["target_k"]),
            include_filtered=include_filtered,
            rows=ranked_rows,
            counts=counts,
            strategy="keyword_fallback",
            candidate_summary={"keyword_candidate_count": len(keyword_rows)},
        )
        payload["fallback_reason"] = fallback_reason
        return payload

    def _resolve_result_limits(
        self,
        *,
        requested_k: int,
        scoped_document_ids: list[str],
    ) -> dict[str, int | None]:
        document_count = len(scoped_document_ids or [])
        if document_count <= 0:
            return {
                "target_k": requested_k,
                "per_document_limit": None,
            }
        if document_count == 1:
            target_k = max(requested_k, self.single_document_top_k)
            return {
                "target_k": target_k,
                "per_document_limit": target_k,
            }

        per_document_limit = self.multi_document_top_k_per_doc
        target_k = max(requested_k, document_count * per_document_limit)
        return {
            "target_k": target_k,
            "per_document_limit": per_document_limit,
        }

    @staticmethod
    def _apply_result_limits(
        *,
        rows: list[dict[str, Any]],
        target_k: int,
        per_document_limit: int | None,
    ) -> list[dict[str, Any]]:
        if not rows:
            return []
        if not per_document_limit or per_document_limit < 1:
            return rows[:target_k]

        limited_rows: list[dict[str, Any]] = []
        document_counts: dict[str, int] = {}
        for row in rows:
            document_id = str(row.get("document_id") or "").strip()
            if document_id:
                current_count = document_counts.get(document_id, 0)
                if current_count >= per_document_limit:
                    continue
                document_counts[document_id] = current_count + 1
            limited_rows.append(row)
            if len(limited_rows) >= target_k:
                break
        return limited_rows

    def _build_rrf_candidates(
        self,
        *,
        dense_rows: list[dict[str, Any]],
        sparse_rows: list[dict[str, Any]],
        include_filtered: bool,
        fusion_limit: int,
    ) -> list[dict[str, Any]]:
        merged_by_block_id: dict[str, dict[str, Any]] = {}

        for row in dense_rows:
            block_id = str(row.get("block_id") or "").strip()
            if not block_id:
                continue
            merged_by_block_id[block_id] = {
                **row,
                "sparse_score": 0.0,
                "sparse_rank": None,
                "rrf_score": self._rrf_component(row.get("dense_rank")),
            }

        for row in sparse_rows:
            block_id = str(row.get("block_id") or "").strip()
            if not block_id:
                continue
            existing = merged_by_block_id.get(block_id)
            sparse_score = float(row.get("keyword_score") or 0.0)
            sparse_rank = row.get("sparse_rank")
            if existing is None:
                merged_by_block_id[block_id] = {
                    **row,
                    "dense_score": 0.0,
                    "dense_rank": None,
                    "sparse_score": sparse_score,
                    "sparse_rank": sparse_rank,
                    "rrf_score": self._rrf_component(sparse_rank),
                    "match_sources": ["sparse"],
                }
                continue

            existing["sparse_score"] = sparse_score
            existing["sparse_rank"] = sparse_rank
            existing["rrf_score"] = float(existing.get("rrf_score") or 0.0) + self._rrf_component(sparse_rank)
            match_sources = list(existing.get("match_sources") or [])
            if "sparse" not in match_sources:
                match_sources.append("sparse")
            existing["match_sources"] = match_sources

        fused_rows: list[dict[str, Any]] = []
        for row in merged_by_block_id.values():
            normalized_content = row.get("normalized_content") if isinstance(row.get("normalized_content"), dict) else {}
            text_role = str(normalized_content.get("text_role") or "").strip().lower()
            section_path = [str(item).strip() for item in (normalized_content.get("section_path") or []) if str(item).strip()]
            filter_reason = self._detect_filter_reason(text_role=text_role, section_path=section_path)
            row["filter_reason"] = filter_reason
            if filter_reason and not include_filtered:
                continue
            row["score"] = float(row.get("rrf_score") or 0.0)
            row["match_sources"] = sorted({str(source).strip() for source in (row.get("match_sources") or []) if str(source).strip()})
            fused_rows.append(row)

        fused_rows.sort(
            key=lambda row: (
                -float(row.get("rrf_score") or 0.0),
                -float(row.get("dense_score") or 0.0),
                -float(row.get("sparse_score") or row.get("keyword_score") or 0.0),
                str(row.get("block_id") or ""),
            )
        )
        return fused_rows[:fusion_limit]

    def _rerank_candidates(
        self,
        *,
        query: str,
        rows: list[dict[str, Any]],
        parsed_k: int,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        if not rows:
            return [], {"reranker_applied": False}
        if not self.reranker_service.enabled:
            ranked = [dict(row, rerank_score=float(row.get("rrf_score") or 0.0)) for row in rows[:parsed_k]]
            return ranked, {"reranker_applied": False}

        texts = [str(row.get("retrieval_text") or "") for row in rows]
        scores = self.reranker_service.score_pairs(query=query, texts=texts)
        if len(scores) != len(rows):
            raise RerankerServiceError(
                code="reranker_count_mismatch",
                message="Reranker returned an unexpected number of scores.",
                status_code=503,
                details={"row_count": len(rows), "score_count": len(scores)},
            )

        reranked_rows = [{**row, "rerank_score": float(score), "score": float(score)} for row, score in zip(rows, scores)]
        reranked_rows.sort(
            key=lambda row: (
                -float(row.get("rerank_score") or 0.0),
                -float(row.get("rrf_score") or 0.0),
                str(row.get("block_id") or ""),
            )
        )
        return reranked_rows[:parsed_k], {"reranker_applied": True}

    def _build_postgres_hybrid_ranked_results(
        self,
        *,
        vector_rows: list[dict[str, Any]],
        keyword_rows: list[dict[str, Any]],
        parsed_k: int,
        include_filtered: bool,
    ) -> list[dict[str, Any]]:
        merged_by_block_id: dict[str, dict[str, Any]] = {}

        for row in vector_rows:
            block_id = str(row.get("block_id") or "").strip()
            if not block_id:
                continue
            merged_by_block_id[block_id] = {
                **row,
                "vector_score": float(row.get("vector_score") or 0.0),
                "keyword_score": 0.0,
                "match_sources": ["vector"],
            }

        for row in keyword_rows:
            block_id = str(row.get("block_id") or "").strip()
            if not block_id:
                continue
            existing = merged_by_block_id.get(block_id)
            keyword_score = float(row.get("keyword_score") or 0.0)
            if existing is None:
                merged_by_block_id[block_id] = {
                    **row,
                    "vector_score": 0.0,
                    "keyword_score": keyword_score,
                    "match_sources": ["keyword"],
                }
                continue

            existing["keyword_score"] = keyword_score
            match_sources = list(existing.get("match_sources") or [])
            if "keyword" not in match_sources:
                match_sources.append("keyword")
            existing["match_sources"] = match_sources

        filtered_rows: list[dict[str, Any]] = []
        for row in merged_by_block_id.values():
            normalized_content = row.get("normalized_content") if isinstance(row.get("normalized_content"), dict) else {}
            text_role = str(normalized_content.get("text_role") or "").strip().lower()
            section_path = [str(item).strip() for item in (normalized_content.get("section_path") or []) if str(item).strip()]
            filter_reason = self._detect_filter_reason(text_role=text_role, section_path=section_path)
            row["filter_reason"] = filter_reason
            if filter_reason and not include_filtered:
                continue
            row["score"] = self._fused_score(
                vector_score=float(row.get("vector_score") or 0.0),
                keyword_score=float(row.get("keyword_score") or 0.0),
            )
            row["match_sources"] = sorted({str(source).strip() for source in (row.get("match_sources") or []) if str(source).strip()})
            filtered_rows.append(row)

        filtered_rows.sort(
            key=lambda row: (
                -float(row.get("score") or 0.0),
                -float(row.get("vector_score") or 0.0),
                -float(row.get("keyword_score") or 0.0),
                str(row.get("block_id") or ""),
            )
        )
        return filtered_rows[:parsed_k]

    def _build_keyword_only_ranked_results(
        self,
        *,
        keyword_rows: list[dict[str, Any]],
        parsed_k: int,
        include_filtered: bool,
    ) -> list[dict[str, Any]]:
        ranked_rows: list[dict[str, Any]] = []
        for row in keyword_rows:
            normalized_content = row.get("normalized_content") if isinstance(row.get("normalized_content"), dict) else {}
            text_role = str(normalized_content.get("text_role") or "").strip().lower()
            section_path = [str(item).strip() for item in (normalized_content.get("section_path") or []) if str(item).strip()]
            filter_reason = self._detect_filter_reason(text_role=text_role, section_path=section_path)
            if filter_reason and not include_filtered:
                continue
            row["filter_reason"] = filter_reason
            row["score"] = float(row.get("keyword_score") or 0.0)
            ranked_rows.append(row)

        ranked_rows.sort(
            key=lambda row: (
                -float(row.get("keyword_score") or 0.0),
                str(row.get("document_name") or ""),
                str(row.get("block_id") or ""),
            )
        )
        return ranked_rows[:parsed_k]

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
        rows: list[dict[str, Any]],
        counts: dict[str, int | bool],
        strategy: str,
        candidate_summary: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        results = [self._serialize_result_row(row) for row in rows or []]
        return {
            "query": query,
            "k": parsed_k,
            "strategy": strategy,
            "returned_count": len(results),
            "filter_summary": counts,
            "candidate_summary": candidate_summary or {},
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
            "candidate_summary": {},
            "results": [],
            "include_filtered": include_filtered,
        }

    def _serialize_result_row(self, row: dict[str, Any]) -> dict[str, Any]:
        normalized_content = row.get("normalized_content") if isinstance(row.get("normalized_content"), dict) else {}
        source_metadata = row.get("source_metadata") if isinstance(row.get("source_metadata"), dict) else {}
        text_role = str(normalized_content.get("text_role") or "").strip().lower()
        section_path = [str(item).strip() for item in (normalized_content.get("section_path") or []) if str(item).strip()]
        filter_reason = str(row.get("filter_reason") or self._detect_filter_reason(text_role=text_role, section_path=section_path))
        payload = {
            "block_id": row.get("block_id"),
            "document_id": row.get("document_id"),
            "document_name": row.get("document_name") or "",
            "score": float(row.get("score") or 0.0),
            "snippet": self._truncate_snippet(row.get("retrieval_text")),
            "block_type": str(row.get("block_type") or "").strip().lower(),
            "subtype": str(row.get("subtype") or "").strip().lower(),
            "text_role": text_role,
            "section_path": section_path,
            "source_metadata": source_metadata,
            "is_filtered": bool(filter_reason),
            "filter_reason": filter_reason,
            "relevance_reason": self._build_relevance_reason(
                block_type=str(row.get("block_type") or "").strip().lower(),
                subtype=str(row.get("subtype") or "").strip().lower(),
                text_role=text_role,
                section_path=section_path,
                filter_reason=filter_reason,
            ),
        }

        for key in ("vector_score", "keyword_score", "dense_score", "sparse_score", "rrf_score", "rerank_score"):
            if row.get(key) is not None:
                payload[key] = float(row.get(key) or 0.0)
        for key in ("vector_rank", "dense_rank", "sparse_rank"):
            if row.get(key) is not None:
                payload[key] = int(row.get(key))
        if isinstance(row.get("match_sources"), list):
            payload["match_sources"] = [str(source) for source in row.get("match_sources") if str(source).strip()]
        return payload

    def _parse_k(self, value: int | str | None) -> int:
        if value in (None, ""):
            return self.default_k
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise RetrievalServiceError(code="invalid_k", message="k must be an integer.", status_code=400) from exc
        if parsed < 1:
            raise RetrievalServiceError(code="invalid_k", message="k must be at least 1.", status_code=400)
        if parsed > self.max_k:
            raise RetrievalServiceError(code="invalid_k", message=f"k must be less than or equal to {self.max_k}.", status_code=400)
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

    def _normalize_query(self, query: str | None) -> str:
        normalized = str(query or "").strip()
        if not self.query_normalization_enabled:
            return normalized
        return " ".join(normalized.split())

    @staticmethod
    def _vector_literal(vector: list[float]) -> str:
        return "[" + ",".join(str(float(value)) for value in vector) + "]"

    def _candidate_pool_limit(self, parsed_k: int) -> int:
        return min(self.candidate_cap, max(parsed_k * self.candidate_multiplier, self.candidate_floor))

    def _fused_score(self, *, vector_score: float, keyword_score: float) -> float:
        return max(
            0.0,
            min(
                1.0,
                (self.vector_weight * float(vector_score or 0.0))
                + (self.keyword_weight * float(keyword_score or 0.0)),
            ),
        )

    @staticmethod
    def _candidate_row_from_tuple(row, *, match_source: str) -> dict[str, Any]:
        (
            block_id,
            document_id,
            document_name,
            block_type,
            subtype,
            normalized_content,
            source_metadata,
            retrieval_text,
            source_score,
        ) = row
        payload = {
            "block_id": str(block_id or ""),
            "document_id": str(document_id or ""),
            "document_name": document_name or "",
            "block_type": str(block_type or "").strip().lower(),
            "subtype": str(subtype or "").strip().lower(),
            "normalized_content": normalized_content if isinstance(normalized_content, dict) else {},
            "source_metadata": source_metadata if isinstance(source_metadata, dict) else {},
            "retrieval_text": retrieval_text or "",
            "match_sources": [match_source],
            "score": float(source_score or 0.0),
        }
        if match_source == "vector":
            payload["vector_score"] = float(source_score or 0.0)
        else:
            payload["keyword_score"] = float(source_score or 0.0)
        return payload

    def _normalize_query_vector_for_storage(self, vector: list[float]) -> list[float]:
        current_dimension = len(vector)
        if current_dimension == self.storage_dimension:
            return [float(value) for value in vector]
        if current_dimension < self.storage_dimension:
            padded = [float(value) for value in vector]
            padded.extend([0.0] * (self.storage_dimension - current_dimension))
            return padded
        raise RetrievalServiceError(
            code="query_embedding_dimension_mismatch",
            message=(
                f"Query embedding dimension {current_dimension} exceeds storage dimension "
                f"{self.storage_dimension}."
            ),
            status_code=500,
            details={
                "query_embedding_dimension": current_dimension,
                "storage_dimension": self.storage_dimension,
            },
        )

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

    def _rrf_component(self, rank_value: int | None) -> float:
        if rank_value in (None, 0):
            return 0.0
        return 1.0 / (self.rrf_k + int(rank_value))

    @staticmethod
    def _is_truthy_env(value: str | None) -> bool:
        return str(value or "").strip().lower() in {"1", "true", "yes", "on"}
