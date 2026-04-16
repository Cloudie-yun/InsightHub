from __future__ import annotations

import argparse
import hashlib
import logging
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from psycopg2 import errors as psycopg_errors
from psycopg2.extras import Json

from db import get_db_connection
from services.embedding_service import EmbeddingService, EmbeddingServiceError
from services.extracted_content import (
    EMBEDDING_STATUS_EMBEDDED,
    EMBEDDING_STATUS_FAILED,
    EMBEDDING_STATUS_READY,
    EMBEDDING_STATUS_RETRYING,
)

def _read_int_env(name: str, default: int) -> int:
    raw_value = str(os.environ.get(name, "") or "").strip()
    if not raw_value:
        return default
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        return default

DEFAULT_BATCH_SIZE = 64
DEFAULT_LIMIT = 256
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_STORAGE_DIMENSION = _read_int_env("EMBEDDING_STORAGE_DIMENSION", 1536)
WORKER_VERSION = "embedding_worker_v1"

logger = logging.getLogger(__name__)


def _read_float_env(name: str, default: float) -> float:
    raw_value = str(os.environ.get(name, "") or "").strip()
    if not raw_value:
        return default
    try:
        return float(raw_value)
    except (TypeError, ValueError):
        return default


DEFAULT_RETRY_BACKOFF_SECONDS = _read_float_env("EMBEDDING_RETRY_BASE_SECONDS", 5.0)
DEFAULT_RETRY_MAX_SECONDS = _read_float_env("EMBEDDING_RETRY_MAX_SECONDS", 300.0)
DEFAULT_RETRY_JITTER_SECONDS = _read_float_env("EMBEDDING_RETRY_JITTER_SECONDS", 2.0)
DEFAULT_POLL_SECONDS = _read_float_env("EMBEDDING_RETRY_POLL_SECONDS", 2.0)


@dataclass
class PendingBlock:
    block_id: str
    retrieval_text: str
    source_metadata: dict[str, Any]
    existing_model_name: str | None
    existing_embedding_dim: int | None
    attempt_count: int


class EmbeddingWorker:
    def __init__(
        self,
        *,
        batch_size: int = DEFAULT_BATCH_SIZE,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        retry_backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS,
        retry_max_seconds: float = DEFAULT_RETRY_MAX_SECONDS,
        retry_jitter_seconds: float = DEFAULT_RETRY_JITTER_SECONDS,
        storage_dimension: int = DEFAULT_STORAGE_DIMENSION,
        service: EmbeddingService | None = None,
    ) -> None:
        self.batch_size = max(1, min(batch_size, 512))
        self.max_attempts = max(1, min(max_attempts, 10))
        self.retry_backoff_seconds = max(0.0, retry_backoff_seconds)
        self.retry_max_seconds = max(self.retry_backoff_seconds, retry_max_seconds)
        self.retry_jitter_seconds = max(0.0, retry_jitter_seconds)
        self.storage_dimension = max(1, int(storage_dimension or DEFAULT_STORAGE_DIMENSION))
        self.service = service or EmbeddingService()
        self.service.batch_size = max(1, min(self.service.batch_size, self.batch_size))
        self._embedding_runs_enabled = True

    def run_once(self, *, limit: int = DEFAULT_LIMIT) -> dict[str, int]:
        stats = {
            "selected": 0,
            "embedded": 0,
            "failed": 0,
            "skipped_idempotent": 0,
        }

        conn = get_db_connection()
        try:
            with conn:
                with conn.cursor() as cur:
                    rows = self._fetch_pending_blocks(cur, limit=limit)
                    stats["selected"] = len(rows)
                    if not rows:
                        return stats

                    to_embed: list[PendingBlock] = []
                    for row in rows:
                        if self._is_idempotent_match(row):
                            self._mark_embedded(cur, row=row, reused_existing=True)
                            stats["skipped_idempotent"] += 1
                        else:
                            to_embed.append(row)

                    if not to_embed:
                        return stats

                    for start in range(0, len(to_embed), self.batch_size):
                        chunk = to_embed[start : start + self.batch_size]
                        vectors = self._embed_with_retries(cur, rows=chunk)
                        if vectors is None:
                            stats["failed"] += len(chunk)
                            continue

                        for row, vector in zip(chunk, vectors):
                            self._upsert_embedding(cur, row=row, vector=vector)
                            self._mark_embedded(
                                cur,
                                row=row,
                                reused_existing=False,
                                attempt_count=row.attempt_count + 1,
                            )
                            stats["embedded"] += 1

            return stats
        finally:
            conn.close()

    def _embed_with_retries(self, cur, *, rows: list[PendingBlock]) -> list[list[float]] | None:
        texts = [row.retrieval_text for row in rows]
        batch_attempt = max((row.attempt_count for row in rows), default=0) + 1
        started_at = datetime.now(timezone.utc)
        batch_started = time.perf_counter()
        try:
            vectors = self.service.embed_texts(texts)
            latency_ms = (time.perf_counter() - batch_started) * 1000.0
            logger.info(
                "embedding batch success model=%s batch_size=%s latency_ms=%.2f attempt=%s",
                self._current_model_name(),
                len(rows),
                latency_ms,
                batch_attempt,
            )
            completed_at = datetime.now(timezone.utc)
            for row in rows:
                self._record_run(
                    cur,
                    row=row,
                    status="embedded",
                    error_message=None,
                    started_at=started_at,
                    completed_at=completed_at,
                )
            return vectors
        except EmbeddingServiceError as exc:
            latency_ms = (time.perf_counter() - batch_started) * 1000.0
            is_retryable = bool(exc.retryable)
            logger.exception(
                "Embedding batch failed model=%s batch_size=%s latency_ms=%.2f attempt=%s retryable=%s",
                self._current_model_name(),
                len(rows),
                latency_ms,
                batch_attempt,
                is_retryable,
            )
            completed_at = datetime.now(timezone.utc)
            for row in rows:
                next_attempt_count = row.attempt_count + 1
                has_attempts_left = next_attempt_count < self.max_attempts
                status = "retrying" if (is_retryable and has_attempts_left) else "failed"
                self._record_run(
                    cur,
                    row=row,
                    status=status,
                    error_message=exc.message,
                    started_at=started_at,
                    completed_at=completed_at,
                )
                if is_retryable and has_attempts_left:
                    self._mark_retrying(
                        cur,
                        row=row,
                        error=exc,
                        attempt_count=next_attempt_count,
                    )
                else:
                    self._mark_failed(
                        cur,
                        row=row,
                        error_payload=exc.to_dict(),
                        attempt_count=next_attempt_count,
                    )
            return None

    def _fetch_pending_blocks(self, cur, *, limit: int) -> list[PendingBlock]:
        cur.execute(
            """
            SELECT
                db.block_id::text,
                db.normalized_content->>'retrieval_text' AS retrieval_text,
                COALESCE(db.source_metadata, '{}'::jsonb) AS source_metadata,
                dbe.model_name,
                dbe.embedding_dim,
                COALESCE((db.source_metadata->'embedding'->>'attempt_count')::int, 0) AS attempt_count
            FROM document_blocks db
            LEFT JOIN document_block_embeddings dbe
              ON dbe.block_id = db.block_id
            WHERE db.embedding_status IN (%s, %s)
              AND (
                    NULLIF(BTRIM(COALESCE(db.source_metadata->'embedding'->>'next_attempt_at', '')), '') IS NULL
                    OR (db.source_metadata->'embedding'->>'next_attempt_at')::timestamptz <= CURRENT_TIMESTAMP
                )
              AND NULLIF(BTRIM(db.normalized_content->>'retrieval_text'), '') IS NOT NULL
            ORDER BY db.updated_at ASC, db.created_at ASC
            LIMIT %s
            FOR UPDATE OF db SKIP LOCKED
            """,
            (EMBEDDING_STATUS_READY, EMBEDDING_STATUS_RETRYING, limit),
        )

        items: list[PendingBlock] = []
        for block_id, retrieval_text, source_metadata, model_name, embedding_dim, attempt_count in cur.fetchall():
            items.append(
                PendingBlock(
                    block_id=block_id,
                    retrieval_text=(retrieval_text or "").strip(),
                    source_metadata=source_metadata or {},
                    existing_model_name=model_name,
                    existing_embedding_dim=embedding_dim,
                    attempt_count=max(0, int(attempt_count or 0)),
                )
            )
        return items

    def _is_idempotent_match(self, row: PendingBlock) -> bool:
        embedding_meta = row.source_metadata.get("embedding") if isinstance(row.source_metadata, dict) else None
        if not isinstance(embedding_meta, dict):
            return False

        existing_hash = str(embedding_meta.get("content_hash") or "")
        existing_version_hash = str(embedding_meta.get("model_version_hash") or "")
        has_same_hash = existing_hash and existing_hash == self._content_hash(row.retrieval_text)
        has_same_model_version = existing_version_hash and existing_version_hash == self._current_model_version_hash()
        has_existing_vector = row.existing_model_name == self._current_model_name()

        return bool(has_same_hash and has_same_model_version and has_existing_vector)

    def _upsert_embedding(self, cur, *, row: PendingBlock, vector: list[float]) -> None:
        source_dimension = len(vector)
        storage_vector = self._normalize_vector_for_storage(vector)
        cur.execute(
            """
            INSERT INTO document_block_embeddings (
                block_id,
                model_name,
                embedding,
                embedding_dim,
                created_at,
                updated_at
            )
            VALUES (%s, %s, %s::vector, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (block_id)
            DO UPDATE SET
                model_name = EXCLUDED.model_name,
                embedding = EXCLUDED.embedding,
                embedding_dim = EXCLUDED.embedding_dim,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                row.block_id,
                self._current_model_name(),
                self._vector_literal(storage_vector),
                len(storage_vector),
            ),
        )
        row.source_metadata = {
            **(row.source_metadata or {}),
            "embedding": {
                **((row.source_metadata or {}).get("embedding") or {}),
                "source_dimension": source_dimension,
                "storage_dimension": len(storage_vector),
            },
        }

    def _mark_embedded(self, cur, *, row: PendingBlock, reused_existing: bool, attempt_count: int | None = None) -> None:
        effective_attempt_count = row.attempt_count if attempt_count is None else max(0, attempt_count)
        payload = {
            "worker_version": WORKER_VERSION,
            "provider": self.service.provider,
            "model_name": self._current_model_name(),
            "model_signature": self._build_model_signature(),
            "model_version_hash": self._current_model_version_hash(),
            "content_hash": self._content_hash(row.retrieval_text),
            "attempt_count": effective_attempt_count,
            "source_dimension": self._embedding_source_dimension(row),
            "storage_dimension": self.storage_dimension,
            "last_attempt_at": datetime.now(timezone.utc).isoformat(),
            "next_attempt_at": None,
            "retry_after_seconds": None,
            "last_status": EMBEDDING_STATUS_EMBEDDED,
            "last_error": None,
            "reused_existing": reused_existing,
            "error": None,
        }
        cur.execute(
            """
            UPDATE document_blocks
            SET
                embedding_status = %s,
                source_metadata = jsonb_set(
                    COALESCE(source_metadata, '{}'::jsonb),
                    '{embedding}',
                    %s::jsonb,
                    true
                ),
                updated_at = CURRENT_TIMESTAMP
            WHERE block_id = %s
            """,
            (
                EMBEDDING_STATUS_EMBEDDED,
                Json(payload),
                row.block_id,
            ),
        )

    def _mark_retrying(
        self,
        cur,
        *,
        row: PendingBlock,
        error: EmbeddingServiceError,
        attempt_count: int,
    ) -> None:
        retry_after_seconds = self._compute_retry_delay(error, attempt_count=attempt_count)
        next_attempt_at = datetime.now(timezone.utc).timestamp() + retry_after_seconds
        payload = {
            "worker_version": WORKER_VERSION,
            "provider": self.service.provider,
            "model_name": self._current_model_name(),
            "model_signature": self._build_model_signature(),
            "model_version_hash": self._current_model_version_hash(),
            "content_hash": self._content_hash(row.retrieval_text),
            "attempt_count": attempt_count,
            "source_dimension": self._embedding_source_dimension(row),
            "storage_dimension": self.storage_dimension,
            "last_attempt_at": datetime.now(timezone.utc).isoformat(),
            "next_attempt_at": datetime.fromtimestamp(next_attempt_at, tz=timezone.utc).isoformat(),
            "retry_after_seconds": retry_after_seconds,
            "last_status": EMBEDDING_STATUS_RETRYING,
            "last_error": error.to_dict(),
            "error": error.to_dict(),
        }
        cur.execute(
            """
            UPDATE document_blocks
            SET
                embedding_status = %s,
                source_metadata = jsonb_set(
                    COALESCE(source_metadata, '{}'::jsonb),
                    '{embedding}',
                    %s::jsonb,
                    true
                ),
                updated_at = CURRENT_TIMESTAMP
            WHERE block_id = %s
            """,
            (
                EMBEDDING_STATUS_READY,
                Json(payload),
                row.block_id,
            ),
        )

    def _mark_failed(self, cur, *, row: PendingBlock, error_payload: dict[str, Any], attempt_count: int) -> None:
        payload = {
            "worker_version": WORKER_VERSION,
            "provider": self.service.provider,
            "model_name": self._current_model_name(),
            "model_signature": self._build_model_signature(),
            "model_version_hash": self._current_model_version_hash(),
            "content_hash": self._content_hash(row.retrieval_text),
            "attempt_count": attempt_count,
            "source_dimension": self._embedding_source_dimension(row),
            "storage_dimension": self.storage_dimension,
            "last_attempt_at": datetime.now(timezone.utc).isoformat(),
            "next_attempt_at": None,
            "retry_after_seconds": error_payload.get("retry_after_seconds"),
            "last_status": EMBEDDING_STATUS_FAILED,
            "last_error": error_payload,
            "error": error_payload,
        }
        cur.execute(
            """
            UPDATE document_blocks
            SET
                embedding_status = %s,
                source_metadata = jsonb_set(
                    COALESCE(source_metadata, '{}'::jsonb),
                    '{embedding}',
                    %s::jsonb,
                    true
                ),
                updated_at = CURRENT_TIMESTAMP
            WHERE block_id = %s
            """,
            (
                EMBEDDING_STATUS_FAILED,
                Json(payload),
                row.block_id,
            ),
        )

    def _compute_retry_delay(self, error: EmbeddingServiceError, *, attempt_count: int) -> float:
        if error.retry_after_seconds is not None:
            return max(0.0, float(error.retry_after_seconds))

        exponential_delay = self.retry_backoff_seconds * (2 ** max(0, attempt_count - 1))
        bounded_delay = min(self.retry_max_seconds, exponential_delay)
        jitter = random.uniform(0.0, self.retry_jitter_seconds) if self.retry_jitter_seconds > 0 else 0.0
        return max(0.0, bounded_delay + jitter)

    def _record_run(
        self,
        cur,
        *,
        row: PendingBlock,
        status: str,
        error_message: str | None,
        started_at: datetime,
        completed_at: datetime,
    ) -> None:
        if not self._embedding_runs_enabled:
            return

        try:
            cur.execute(
                """
                INSERT INTO embedding_runs (
                    block_id,
                    status,
                    error_message,
                    started_at,
                    completed_at,
                    model_name
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    row.block_id,
                    status,
                    error_message,
                    started_at,
                    completed_at,
                    self._current_model_name(),
                ),
            )
        except psycopg_errors.UndefinedTable:
            self._embedding_runs_enabled = False
            logger.warning("embedding_runs table not found; run tracking is disabled until migration is applied.")

    def _build_model_signature(self) -> str:
        expected_dimension = self.service.expected_dimension or "auto"
        return f"{self.service.provider}:{self._current_model_name()}:{expected_dimension}:{WORKER_VERSION}"

    def _current_model_name(self) -> str:
        return self.service.get_effective_model_name()

    def _current_model_version_hash(self) -> str:
        return hashlib.sha256(self._build_model_signature().encode("utf-8")).hexdigest()

    @staticmethod
    def _content_hash(text: str) -> str:
        return hashlib.sha256((text or "").strip().encode("utf-8")).hexdigest()

    @staticmethod
    def _vector_literal(vector: list[float]) -> str:
        return "[" + ",".join(str(float(value)) for value in vector) + "]"

    def _normalize_vector_for_storage(self, vector: list[float]) -> list[float]:
        current_dimension = len(vector)
        if current_dimension == self.storage_dimension:
            return [float(value) for value in vector]
        if current_dimension < self.storage_dimension:
            padded = [float(value) for value in vector]
            padded.extend([0.0] * (self.storage_dimension - current_dimension))
            return padded
        raise ValueError(
            f"Embedding vector dimension {current_dimension} exceeds storage dimension {self.storage_dimension}."
        )

    @staticmethod
    def _embedding_source_dimension(row: PendingBlock) -> int | None:
        embedding_meta = row.source_metadata.get("embedding") if isinstance(row.source_metadata, dict) else None
        if not isinstance(embedding_meta, dict):
            return None
        value = embedding_meta.get("source_dimension")
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None


def run_worker(
    *,
    batch_size: int,
    limit: int,
    loop: bool,
    sleep_seconds: float,
    max_attempts: int,
    retry_backoff_seconds: float,
    retry_max_seconds: float,
    retry_jitter_seconds: float,
) -> int:
    worker = EmbeddingWorker(
        batch_size=batch_size,
        max_attempts=max_attempts,
        retry_backoff_seconds=retry_backoff_seconds,
        retry_max_seconds=retry_max_seconds,
        retry_jitter_seconds=retry_jitter_seconds,
    )

    while True:
        stats = worker.run_once(limit=limit)
        logger.info(
            "embedding-worker selected=%s embedded=%s failed=%s skipped_idempotent=%s",
            stats["selected"],
            stats["embedded"],
            stats["failed"],
            stats["skipped_idempotent"],
        )

        if not loop or stats["selected"] == 0:
            return 0

        time.sleep(max(0.1, sleep_seconds))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Embed pending document blocks and persist vectors.")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Rows embedded per provider call.")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Max pending rows to claim per loop.")
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=DEFAULT_MAX_ATTEMPTS,
        help="Max embedding attempts for retryable provider failures.",
    )
    parser.add_argument(
        "--retry-backoff-seconds",
        type=float,
        default=DEFAULT_RETRY_BACKOFF_SECONDS,
        help="Exponential backoff base for retryable failures.",
    )
    parser.add_argument(
        "--retry-max-seconds",
        type=float,
        default=DEFAULT_RETRY_MAX_SECONDS,
        help="Max cooldown for retryable failures when Retry-After is unavailable.",
    )
    parser.add_argument(
        "--retry-jitter-seconds",
        type=float,
        default=DEFAULT_RETRY_JITTER_SECONDS,
        help="Random jitter added to computed retry cooldowns.",
    )
    parser.add_argument("--loop", action="store_true", help="Keep polling for new ready blocks.")
    parser.add_argument("--sleep-seconds", type=float, default=DEFAULT_POLL_SECONDS, help="Sleep interval when --loop is enabled.")
    parser.add_argument("--log-level", default="INFO", help="Logging level (e.g. DEBUG, INFO).")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    return run_worker(
        batch_size=args.batch_size,
        limit=args.limit,
        loop=args.loop,
        sleep_seconds=args.sleep_seconds,
        max_attempts=args.max_attempts,
        retry_backoff_seconds=args.retry_backoff_seconds,
        retry_max_seconds=args.retry_max_seconds,
        retry_jitter_seconds=args.retry_jitter_seconds,
    )


if __name__ == "__main__":
    raise SystemExit(main())
