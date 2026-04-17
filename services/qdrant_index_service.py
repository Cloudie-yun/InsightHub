from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any

import httpx


logger = logging.getLogger(__name__)


@dataclass
class QdrantServiceError(Exception):
    code: str
    message: str
    status_code: int = 503
    details: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "status_code": self.status_code,
            "details": self.details or {},
        }


class QdrantIndexService:
    def __init__(self) -> None:
        self.base_url = str(os.getenv("QDRANT_URL") or "").strip().rstrip("/")
        self.api_key = str(os.getenv("QDRANT_API_KEY") or "").strip()
        self.collection = str(os.getenv("QDRANT_COLLECTION") or "document_blocks").strip() or "document_blocks"
        self.timeout_seconds = max(1.0, float(os.getenv("QDRANT_TIMEOUT_SECONDS", "10")))

    @property
    def enabled(self) -> bool:
        return bool(self.base_url)

    def ensure_collection(self, *, vector_size: int) -> None:
        self._require_enabled()
        collection_path = f"/collections/{self.collection}"
        try:
            response = self._request("GET", collection_path)
            existing_size = (
                response.get("result", {})
                .get("config", {})
                .get("params", {})
                .get("vectors", {})
                .get("size")
            )
            if int(existing_size or 0) == int(vector_size):
                return
            raise QdrantServiceError(
                code="collection_dimension_mismatch",
                message="Existing Qdrant collection vector size does not match configured embedding storage dimension.",
                status_code=503,
                details={
                    "collection": self.collection,
                    "existing_size": existing_size,
                    "expected_size": vector_size,
                },
            )
        except QdrantServiceError as exc:
            if exc.status_code != 404:
                raise

        payload = {
            "vectors": {
                "size": int(vector_size),
                "distance": str(os.getenv("QDRANT_DISTANCE", "Cosine")).strip() or "Cosine",
            },
        }
        self._request("PUT", collection_path, json=payload)

    def upsert_points(self, *, points: list[dict[str, Any]]) -> None:
        self._require_enabled()
        if not points:
            return
        self._request(
            "PUT",
            f"/collections/{self.collection}/points",
            json={"points": points, "wait": False},
        )

    def delete_points(self, *, point_ids: list[str]) -> None:
        self._require_enabled()
        normalized_ids = [str(point_id).strip() for point_id in point_ids if str(point_id).strip()]
        if not normalized_ids:
            return
        self._request(
            "POST",
            f"/collections/{self.collection}/points/delete",
            json={"points": normalized_ids, "wait": False},
        )

    def search(
        self,
        *,
        query_vector: list[float],
        document_ids: list[str],
        limit: int,
        with_vectors: bool = False,
    ) -> list[dict[str, Any]]:
        self._require_enabled()
        must_filters: list[dict[str, Any]] = []
        normalized_document_ids = [str(item).strip() for item in document_ids if str(item).strip()]
        if normalized_document_ids:
            must_filters.append(
                {
                    "key": "document_id",
                    "match": {"any": normalized_document_ids},
                }
            )

        payload = {
            "vector": [float(value) for value in query_vector],
            "limit": int(limit),
            "with_payload": True,
            "with_vector": bool(with_vectors),
            "filter": {"must": must_filters} if must_filters else None,
        }
        result = self._request(
            "POST",
            f"/collections/{self.collection}/points/search",
            json=payload,
        )
        return result.get("result") or []

    def _request(self, method: str, path: str, *, json: dict[str, Any] | None = None) -> dict[str, Any]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["api-key"] = self.api_key

        try:
            with httpx.Client(base_url=self.base_url, timeout=self.timeout_seconds, headers=headers) as client:
                response = client.request(method=method, url=path, json=json)
        except httpx.HTTPError as exc:
            raise QdrantServiceError(
                code="qdrant_unreachable",
                message="Could not reach Qdrant.",
                status_code=503,
                details={"exception_type": type(exc).__name__},
            ) from exc

        if response.status_code >= 400:
            raise QdrantServiceError(
                code="qdrant_http_error",
                message="Qdrant request failed.",
                status_code=404 if response.status_code == 404 else 503,
                details={
                    "http_status": response.status_code,
                    "response_text": response.text[:1000],
                    "path": path,
                },
            )

        if not response.content:
            return {}

        try:
            parsed = response.json()
        except ValueError as exc:
            raise QdrantServiceError(
                code="qdrant_invalid_response",
                message="Qdrant returned invalid JSON.",
                status_code=503,
                details={"path": path},
            ) from exc

        if not isinstance(parsed, dict):
            raise QdrantServiceError(
                code="qdrant_invalid_response",
                message="Qdrant returned an unexpected payload.",
                status_code=503,
                details={"path": path},
            )
        return parsed

    def _require_enabled(self) -> None:
        if self.enabled:
            return
        raise QdrantServiceError(
            code="qdrant_not_configured",
            message="Qdrant is not configured. Set QDRANT_URL to enable dense retrieval.",
            status_code=503,
            details={},
        )


def build_qdrant_payload(
    *,
    block_id: str,
    document_id: str,
    document_name: str,
    block_type: str,
    subtype: str,
    normalized_content: dict[str, Any],
    source_metadata: dict[str, Any],
    retrieval_text: str,
) -> dict[str, Any]:
    normalized = normalized_content if isinstance(normalized_content, dict) else {}
    text_role = str(normalized.get("text_role") or "").strip().lower()
    section_path = [
        str(item).strip()
        for item in (normalized.get("section_path") or [])
        if str(item).strip()
    ]
    is_filtered_candidate = bool(
        text_role == "note"
        or any(
            section.lower().startswith(
                ("abstract", "references", "bibliography", "works cited", "literature cited")
            )
            for section in section_path
        )
    )
    return {
        "block_id": str(block_id or ""),
        "document_id": str(document_id or ""),
        "document_name": document_name or "",
        "block_type": str(block_type or "").strip().lower(),
        "subtype": str(subtype or "").strip().lower(),
        "retrieval_text": (retrieval_text or "").strip(),
        "text_role": text_role,
        "section_path": section_path,
        "source_metadata": source_metadata if isinstance(source_metadata, dict) else {},
        "is_filtered_candidate": is_filtered_candidate,
    }
