from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

try:
    from sentence_transformers import CrossEncoder
except Exception:  # pragma: no cover - import guard for optional dependency shape
    CrossEncoder = None


@dataclass
class RerankerServiceError(Exception):
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


class RerankerService:
    _model_instance = None
    _model_name = None

    def __init__(self) -> None:
        self.enabled = str(os.getenv("RERANKER_ENABLED", "1")).strip().lower() in {"1", "true", "yes", "on"}
        self.model_name = str(os.getenv("RERANKER_MODEL") or "BAAI/bge-reranker-base").strip()
        self.batch_size = max(1, int(os.getenv("RERANKER_BATCH_SIZE", "16")))

    def score_pairs(self, *, query: str, texts: list[str]) -> list[float]:
        if not self.enabled or not texts:
            return []
        model = self._load_model()
        try:
            predictions = model.predict(
                [(query, text) for text in texts],
                batch_size=self.batch_size,
                show_progress_bar=False,
            )
        except Exception as exc:
            raise RerankerServiceError(
                code="reranker_inference_failed",
                message="Cross-encoder reranking failed.",
                status_code=503,
                details={"exception_type": type(exc).__name__},
            ) from exc

        return [float(value) for value in predictions]

    def _load_model(self):
        if not self.enabled:
            raise RerankerServiceError(
                code="reranker_disabled",
                message="Reranker is disabled.",
                status_code=503,
                details={},
            )
        if CrossEncoder is None:
            raise RerankerServiceError(
                code="reranker_import_failed",
                message="sentence-transformers CrossEncoder is unavailable.",
                status_code=503,
                details={},
            )
        if (
            RerankerService._model_instance is None
            or RerankerService._model_name != self.model_name
        ):
            try:
                RerankerService._model_instance = CrossEncoder(self.model_name)
                RerankerService._model_name = self.model_name
            except Exception as exc:
                raise RerankerServiceError(
                    code="reranker_model_load_failed",
                    message="Could not load the configured reranker model.",
                    status_code=503,
                    details={
                        "model_name": self.model_name,
                        "exception_type": type(exc).__name__,
                    },
                ) from exc
        return RerankerService._model_instance
