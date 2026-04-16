from __future__ import annotations

import email.utils
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib import error, request
from sentence_transformers import SentenceTransformer

from services.quota_router import (
    TASK_TYPE_EMBEDDING,
    QuotaRouterError,
    classify_quota_error,
    extract_response_headers,
    get_quota_project_id,
    pick_available_model,
    record_model_success,
    record_quota_failure,
)

OPENAI_EMBEDDINGS_URL = "https://api.openai.com/v1/embeddings"
GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta"


@dataclass
class EmbeddingServiceError(Exception):
    code: str
    message: str
    retryable: bool
    provider: str
    status_code: int | None = None
    retry_after_seconds: float | None = None
    details: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "retryable": self.retryable,
            "provider": self.provider,
            "status_code": self.status_code,
            "retry_after_seconds": self.retry_after_seconds,
            "details": self.details or {},
        }


class EmbeddingService:
    _hf_model_instance = None
    _hf_model_name = None

    def __init__(self) -> None:
        self.provider = (os.environ.get("EMBEDDING_PROVIDER") or "gemini").strip().lower()

        if self.provider == "gemini":
            default_model = "gemini-embedding-001"
            default_dimension = 1536
        elif self.provider == "openai":
            default_model = "text-embedding-3-small"
            default_dimension = 1536
        elif self.provider == "huggingface":
            default_model = "sentence-transformers/all-MiniLM-L6-v2"
            default_dimension = 384
        else:
            default_model = ""
            default_dimension = 1536

        self.model = (os.environ.get("EMBEDDING_MODEL") or default_model).strip()
        self.current_model_name = self.model
        self.batch_size = self._parse_positive_int(os.environ.get("EMBEDDING_BATCH_SIZE"), default=32)
        self.expected_dimension = self._parse_positive_int(
            os.environ.get("EMBEDDING_DIMENSION"),
            default=default_dimension,
        )
        self.api_key = self._resolve_api_key()

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []

        normalized_texts = [self._normalize_text(text, idx) for idx, text in enumerate(texts)]

        if self.provider not in {"openai", "gemini", "huggingface"}:
            raise EmbeddingServiceError(
                code="unsupported_provider",
                message=f"Embedding provider '{self.provider}' is not supported.",
                retryable=False,
                provider=self.provider,
                details={"configured_provider": self.provider},
            )
        if self.provider in {"openai", "gemini"} and not self.api_key:
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
            elif self.provider == "gemini":
                batch_vectors = self._embed_gemini_batch(batch)
            else:
                batch_vectors = self._embed_huggingface_batch(batch)
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
            response_headers = self._serialize_headers(getattr(exc, "headers", None))
            retry_after_seconds = self._parse_retry_after(response_headers.get("Retry-After"))
            raise EmbeddingServiceError(
                code="provider_http_error",
                message="Embedding provider request failed.",
                retryable=retryable,
                provider=self.provider,
                status_code=exc.code,
                retry_after_seconds=retry_after_seconds,
                details={
                    "response_body": body[:1000],
                    "response_headers": response_headers,
                },
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
            response_headers = self._serialize_headers(getattr(resp, "headers", None))
            raise EmbeddingServiceError(
                code="provider_http_error",
                message="Embedding provider request failed.",
                retryable=status >= 500 or status == 429,
                provider=self.provider,
                status_code=status,
                retry_after_seconds=self._parse_retry_after(response_headers.get("Retry-After")),
                details={
                    "response_body": body[:1000],
                    "response_headers": response_headers,
                },
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
        project_id = get_quota_project_id()
        fallback_errors: list[EmbeddingServiceError] = []
        attempted_models: set[str] = set()
        raw_response: dict[str, Any] | None = None

        while raw_response is None:
            try:
                selected_model = pick_available_model(
                    TASK_TYPE_EMBEDDING,
                    project_id=project_id,
                    fallback_model=self.model,
                )
            except QuotaRouterError as exc:
                latest_error = fallback_errors[-1] if fallback_errors else None
                raise EmbeddingServiceError(
                    code="quota_router_unavailable",
                    message=str(exc),
                    retryable=True,
                    provider="gemini",
                    status_code=latest_error.status_code if latest_error else None,
                    retry_after_seconds=latest_error.retry_after_seconds if latest_error else None,
                    details={
                        "attempted_models": sorted(attempted_models),
                        "last_error": latest_error.to_dict() if latest_error else None,
                    },
                ) from exc

            attempted_models.add(selected_model)
            self.current_model_name = selected_model
            model_ref = selected_model if selected_model.startswith("models/") else f"models/{selected_model}"
            requests_payload = [
                {
                    "model": model_ref,
                    "content": {
                        "parts": [{"text": text}],
                    },
                }
                for text in batch
            ]
            if self.expected_dimension and model_ref != "models/embedding-001":
                for request_payload in requests_payload:
                    request_payload["outputDimensionality"] = int(self.expected_dimension)

            payload = {"requests": [request_payload for request_payload in requests_payload]}
            url = f"{GEMINI_API_BASE}/{model_ref}:batchEmbedContents?key={self.api_key}"

            try:
                raw_response = self._post_json(url=url, payload=payload, provider="gemini")
            except EmbeddingServiceError as exc:
                quota_error_code = classify_quota_error(
                    status_code=exc.status_code,
                    message=exc.message,
                    details=exc.details,
                )
                if not quota_error_code:
                    raise

                record_quota_failure(
                    project_id=project_id,
                    model_name=selected_model,
                    error_code=quota_error_code,
                    retry_after_seconds=exc.retry_after_seconds,
                    response_headers=extract_response_headers(exc.details),
                )
                fallback_errors.append(exc)

        record_model_success(
            project_id=project_id,
            model_name=self.current_model_name,
            request_count=1,
            token_count=sum(len(text.split()) for text in batch),
        )

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
    
    def _embed_huggingface_batch(self, batch: list[str]) -> list[list[float]]:
        try:
            if (
                EmbeddingService._hf_model_instance is None
                or EmbeddingService._hf_model_name != self.model
            ):
                EmbeddingService._hf_model_instance = SentenceTransformer(self.model)
                EmbeddingService._hf_model_name = self.model

            model = EmbeddingService._hf_model_instance
            vectors = model.encode(
                batch,
                convert_to_numpy=True,
                normalize_embeddings=True,
                show_progress_bar=False,
            )

            return [vec.astype(float).tolist() for vec in vectors]

        except Exception as exc:
            raise EmbeddingServiceError(
                code="provider_local_error",
                message=f"Hugging Face embedding failed: {exc}",
                retryable=True,
                provider="huggingface",
                details={"model": self.model},
            ) from exc

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
            response_headers = self._serialize_headers(getattr(exc, "headers", None))
            retry_after_seconds = self._parse_retry_after(response_headers.get("Retry-After"))
            raise EmbeddingServiceError(
                code="provider_http_error",
                message="Embedding provider request failed.",
                retryable=retryable,
                provider=provider,
                status_code=exc.code,
                retry_after_seconds=retry_after_seconds,
                details={
                    "response_body": body[:1000],
                    "response_headers": response_headers,
                },
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
            response_headers = self._serialize_headers(getattr(resp, "headers", None))
            raise EmbeddingServiceError(
                code="provider_http_error",
                message="Embedding provider request failed.",
                retryable=status >= 500 or status == 429,
                provider=provider,
                status_code=status,
                retry_after_seconds=self._parse_retry_after(response_headers.get("Retry-After")),
                details={
                    "response_body": body[:1000],
                    "response_headers": response_headers,
                },
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

    def get_effective_model_name(self) -> str:
        return (self.current_model_name or self.model).strip()

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

    @staticmethod
    def _serialize_headers(headers: Any) -> dict[str, str]:
        if not headers:
            return {}

        items: list[tuple[str, str]] = []
        if hasattr(headers, "items"):
            try:
                items = list(headers.items())
            except Exception:
                items = []

        selected_headers: dict[str, str] = {}
        allowed_names = {
            "retry-after",
            "x-ratelimit-limit-requests",
            "x-ratelimit-remaining-requests",
            "x-ratelimit-reset-requests",
            "x-ratelimit-limit-tokens",
            "x-ratelimit-remaining-tokens",
            "x-ratelimit-reset-tokens",
        }
        for key, value in items:
            normalized_key = str(key or "").strip()
            if not normalized_key or normalized_key.lower() not in allowed_names:
                continue
            selected_headers[normalized_key] = str(value or "").strip()[:200]
        return selected_headers

    @staticmethod
    def _parse_retry_after(value: str | None) -> float | None:
        raw_value = str(value or "").strip()
        if not raw_value:
            return None

        try:
            seconds = float(raw_value)
            return max(0.0, seconds)
        except (TypeError, ValueError):
            pass

        try:
            parsed_dt = email.utils.parsedate_to_datetime(raw_value)
        except (TypeError, ValueError, IndexError, OverflowError):
            return None

        if parsed_dt is None:
            return None
        if parsed_dt.tzinfo is None:
            parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)

        delay_seconds = (parsed_dt - datetime.now(timezone.utc)).total_seconds()
        return max(0.0, delay_seconds)
