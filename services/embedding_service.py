from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any
from urllib import error, request


OPENAI_EMBEDDINGS_URL = "https://api.openai.com/v1/embeddings"
GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta"


@dataclass
class EmbeddingServiceError(Exception):
    code: str
    message: str
    retryable: bool
    provider: str
    status_code: int | None = None
    details: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "retryable": self.retryable,
            "provider": self.provider,
            "status_code": self.status_code,
            "details": self.details or {},
        }


class EmbeddingService:
    def __init__(self) -> None:
        self.provider = (os.environ.get("EMBEDDING_PROVIDER") or "gemini").strip().lower()
        default_model = "text-embedding-004" if self.provider == "gemini" else "text-embedding-3-small"
        self.model = (os.environ.get("EMBEDDING_MODEL") or default_model).strip()
        self.batch_size = self._parse_positive_int(os.environ.get("EMBEDDING_BATCH_SIZE"), default=32)
        self.expected_dimension = self._parse_positive_int(os.environ.get("EMBEDDING_DIMENSION"), default=None)
        self.api_key = self._resolve_api_key()

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []

        normalized_texts = [self._normalize_text(text, idx) for idx, text in enumerate(texts)]

        if self.provider not in {"openai", "gemini"}:
            raise EmbeddingServiceError(
                code="unsupported_provider",
                message=f"Embedding provider '{self.provider}' is not supported.",
                retryable=False,
                provider=self.provider,
                details={"configured_provider": self.provider},
            )

        if not self.api_key:
            expected_key_hint = "GEMINI_API_KEY" if self.provider == "gemini" else "OPENAI_API_KEY"
            raise EmbeddingServiceError(
                code="missing_api_key",
                message=f"Embedding API key is missing. Set EMBEDDING_API_KEY or {expected_key_hint}.",
                retryable=False,
                provider=self.provider,
            )

        vectors: list[list[float]] = []
        for start in range(0, len(normalized_texts), self.batch_size):
            batch = normalized_texts[start : start + self.batch_size]
            if self.provider == "openai":
                batch_vectors = self._embed_openai_batch(batch)
            else:
                batch_vectors = self._embed_gemini_batch(batch)
            vectors.extend(batch_vectors)

        if len(vectors) != len(normalized_texts):
            raise EmbeddingServiceError(
                code="embedding_count_mismatch",
                message="Embedding provider returned an unexpected number of vectors.",
                retryable=True,
                provider=self.provider,
                details={
                    "input_count": len(normalized_texts),
                    "vector_count": len(vectors),
                },
            )

        self._validate_vectors(vectors)
        return vectors

    def _embed_openai_batch(self, batch: list[str]) -> list[list[float]]:
        payload = {
            "model": self.model,
            "input": batch,
        }
        encoded_payload = json.dumps(payload).encode("utf-8")
        req = request.Request(
            OPENAI_EMBEDDINGS_URL,
            data=encoded_payload,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )

        try:
            with request.urlopen(req, timeout=45) as resp:
                status = getattr(resp, "status", 200)
                body = resp.read().decode("utf-8")
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="ignore") if hasattr(exc, "read") else ""
            retryable = exc.code >= 500 or exc.code == 429
            raise EmbeddingServiceError(
                code="provider_http_error",
                message="Embedding provider request failed.",
                retryable=retryable,
                provider=self.provider,
                status_code=exc.code,
                details={"response_body": body[:1000]},
            ) from exc
        except error.URLError as exc:
            raise EmbeddingServiceError(
                code="provider_connection_error",
                message="Could not connect to embedding provider.",
                retryable=True,
                provider=self.provider,
                details={"reason": str(exc.reason)},
            ) from exc
        except TimeoutError as exc:
            raise EmbeddingServiceError(
                code="provider_timeout",
                message="Embedding provider request timed out.",
                retryable=True,
                provider=self.provider,
            ) from exc

        if status >= 400:
            raise EmbeddingServiceError(
                code="provider_http_error",
                message="Embedding provider request failed.",
                retryable=status >= 500 or status == 429,
                provider=self.provider,
                status_code=status,
                details={"response_body": body[:1000]},
            )

        try:
            parsed = json.loads(body)
        except json.JSONDecodeError as exc:
            raise EmbeddingServiceError(
                code="invalid_provider_response",
                message="Embedding provider returned invalid JSON.",
                retryable=True,
                provider=self.provider,
                status_code=status,
            ) from exc

        data = parsed.get("data")
        if not isinstance(data, list):
            raise EmbeddingServiceError(
                code="invalid_provider_response",
                message="Embedding provider response did not include a valid data array.",
                retryable=True,
                provider=self.provider,
                status_code=status,
                details={"response_keys": list(parsed.keys())},
            )

        indexed_embeddings: list[tuple[int, list[float]]] = []
        for row in data:
            if not isinstance(row, dict):
                continue
            row_index = row.get("index")
            row_embedding = row.get("embedding")
            if isinstance(row_index, int) and isinstance(row_embedding, list):
                indexed_embeddings.append((row_index, row_embedding))

        indexed_embeddings.sort(key=lambda item: item[0])
        return [embedding for _, embedding in indexed_embeddings]

    def _embed_gemini_batch(self, batch: list[str]) -> list[list[float]]:
        model_ref = self.model if self.model.startswith("models/") else f"models/{self.model}"
        payload = {
            "requests": [
                {
                    "model": model_ref,
                    "content": {
                        "parts": [{"text": text}],
                    },
                }
                for text in batch
            ]
        }
        url = f"{GEMINI_API_BASE}/{model_ref}:batchEmbedContents?key={self.api_key}"
        raw_response = self._post_json(url=url, payload=payload, provider="gemini")

        embeddings = raw_response.get("embeddings")
        if not isinstance(embeddings, list):
            raise EmbeddingServiceError(
                code="invalid_provider_response",
                message="Gemini response did not include a valid embeddings array.",
                retryable=True,
                provider="gemini",
                details={"response_keys": list(raw_response.keys())},
            )

        vectors: list[list[float]] = []
        for idx, item in enumerate(embeddings):
            values = None
            if isinstance(item, dict):
                candidate = item.get("values")
                if isinstance(candidate, list):
                    values = candidate
            if values is None:
                raise EmbeddingServiceError(
                    code="invalid_provider_response",
                    message="Gemini embedding row is missing numeric values.",
                    retryable=True,
                    provider="gemini",
                    details={"index": idx},
                )
            vectors.append(values)

        return vectors

    def _post_json(self, *, url: str, payload: dict[str, Any], provider: str) -> dict[str, Any]:
        req = request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with request.urlopen(req, timeout=45) as resp:
                status = getattr(resp, "status", 200)
                body = resp.read().decode("utf-8")
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="ignore") if hasattr(exc, "read") else ""
            retryable = exc.code >= 500 or exc.code == 429
            raise EmbeddingServiceError(
                code="provider_http_error",
                message="Embedding provider request failed.",
                retryable=retryable,
                provider=provider,
                status_code=exc.code,
                details={"response_body": body[:1000]},
            ) from exc
        except error.URLError as exc:
            raise EmbeddingServiceError(
                code="provider_connection_error",
                message="Could not connect to embedding provider.",
                retryable=True,
                provider=provider,
                details={"reason": str(exc.reason)},
            ) from exc
        except TimeoutError as exc:
            raise EmbeddingServiceError(
                code="provider_timeout",
                message="Embedding provider request timed out.",
                retryable=True,
                provider=provider,
            ) from exc

        if status >= 400:
            raise EmbeddingServiceError(
                code="provider_http_error",
                message="Embedding provider request failed.",
                retryable=status >= 500 or status == 429,
                provider=provider,
                status_code=status,
                details={"response_body": body[:1000]},
            )

        try:
            parsed = json.loads(body)
        except json.JSONDecodeError as exc:
            raise EmbeddingServiceError(
                code="invalid_provider_response",
                message="Embedding provider returned invalid JSON.",
                retryable=True,
                provider=provider,
                status_code=status,
            ) from exc

        if not isinstance(parsed, dict):
            raise EmbeddingServiceError(
                code="invalid_provider_response",
                message="Embedding provider returned a non-object JSON response.",
                retryable=True,
                provider=provider,
            )
        return parsed

    def _validate_vectors(self, vectors: list[list[float]]) -> None:
        inferred_dimension: int | None = None

        for idx, vector in enumerate(vectors):
            if not isinstance(vector, list) or not vector:
                raise EmbeddingServiceError(
                    code="invalid_vector",
                    message="Embedding provider returned an empty vector.",
                    retryable=True,
                    provider=self.provider,
                    details={"index": idx},
                )

            if any(not isinstance(value, (float, int)) for value in vector):
                raise EmbeddingServiceError(
                    code="invalid_vector",
                    message="Embedding provider returned a vector with non-numeric values.",
                    retryable=True,
                    provider=self.provider,
                    details={"index": idx},
                )

            vector_dimension = len(vector)
            if inferred_dimension is None:
                inferred_dimension = vector_dimension
            elif vector_dimension != inferred_dimension:
                raise EmbeddingServiceError(
                    code="dimension_mismatch",
                    message="Embedding provider returned vectors with inconsistent dimensions.",
                    retryable=True,
                    provider=self.provider,
                    details={
                        "expected_dimension": inferred_dimension,
                        "actual_dimension": vector_dimension,
                        "index": idx,
                    },
                )

            if self.expected_dimension and vector_dimension != self.expected_dimension:
                raise EmbeddingServiceError(
                    code="dimension_mismatch",
                    message="Embedding dimension did not match EMBEDDING_DIMENSION.",
                    retryable=False,
                    provider=self.provider,
                    details={
                        "expected_dimension": self.expected_dimension,
                        "actual_dimension": vector_dimension,
                        "index": idx,
                    },
                )

    @staticmethod
    def _normalize_text(text: str, index: int) -> str:
        if not isinstance(text, str):
            raise EmbeddingServiceError(
                code="invalid_input",
                message="All embedding inputs must be strings.",
                retryable=False,
                provider="local",
                details={"index": index, "value_type": type(text).__name__},
            )

        normalized = text.strip()
        if not normalized:
            raise EmbeddingServiceError(
                code="empty_input",
                message="Embedding input text cannot be empty.",
                retryable=False,
                provider="local",
                details={"index": index},
            )
        return normalized

    @staticmethod
    def _parse_positive_int(raw_value: str | None, *, default: int | None) -> int | None:
        if raw_value is None:
            return default

        try:
            parsed = int(str(raw_value).strip())
        except (TypeError, ValueError):
            return default

        return parsed if parsed > 0 else default

    def _resolve_api_key(self) -> str:
        configured_key = (os.environ.get("EMBEDDING_API_KEY") or "").strip()
        if configured_key:
            return configured_key

        if self.provider == "gemini":
            return (os.environ.get("GEMINI_API_KEY") or "").strip()

        return (os.environ.get("OPENAI_API_KEY") or "").strip()
