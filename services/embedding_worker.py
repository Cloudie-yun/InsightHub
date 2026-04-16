from __future__ import annotations

import argparse
import hashlib
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from psycopg2 import errors as psycopg_errors
from psycopg2.extras import Json

from db import get_db_connection
from services.embedding_service import EmbeddingService, EmbeddingServiceError
from services.extracted_content import EMBEDDING_STATUS_EMBEDDED, EMBEDDING_STATUS_FAILED, EMBEDDING_STATUS_READY


DEFAULT_BATCH_SIZE = 64
DEFAULT_LIMIT = 256
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_RETRY_BACKOFF_SECONDS = 1.0
WORKER_VERSION = "embedding_worker_v1"

logger = logging.getLogger(__name__)


@dataclass
class PendingBlock:
    block_id: str
    retrieval_text: str
    source_metadata: dict[str, Any]
    existing_model_name: str | None
    existing_embedding_dim: int | None


class EmbeddingWorker:
    def __init__(
        self,
        *,
        batch_size: int = DEFAULT_BATCH_SIZE,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        retry_backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS,
        service: EmbeddingService | None = None,
    ) -> None:
        self.batch_size = max(1, min(batch_size, 512))
        self.max_attempts = max(1, min(max_attempts, 10))
        self.retry_backoff_seconds = max(0.0, retry_backoff_seconds)
        self.service = service or EmbeddingService()
        self.service.batch_size = max(1, min(self.service.batch_size, self.batch_size))
        self.model_signature = self._build_model_signature()
        self.model_version_hash = hashlib.sha256(self.model_signature.encode("utf-8")).hexdigest()
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
                            self._mark_embedded(cur, row=row, reused_existing=False)
                            stats["embedded"] += 1

            return stats
        finally:
            conn.close()

    def _embed_with_retries(self, cur, *, rows: list[PendingBlock]) -> list[list[float]] | None:
        texts = [row.retrieval_text for row in rows]

        for attempt in range(1, self.max_attempts + 1):
            started_at = datetime.now(timezone.utc)
            batch_started = time.perf_counter()
            try:
                vectors = self.service.embed_texts(texts)
                latency_ms = (time.perf_counter() - batch_started) * 1000.0
                logger.info(
                    "embedding batch success model=%s batch_size=%s latency_ms=%.2f attempt=%s/%s",
                    self.service.model,
                    len(rows),
                    latency_ms,
                    attempt,
                    self.max_attempts,
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
                has_attempts_left = attempt < self.max_attempts
                status = "retrying" if (is_retryable and has_attempts_left) else "failed"
                logger.exception(
                    "Embedding batch failed model=%s batch_size=%s latency_ms=%.2f attempt=%s/%s retryable=%s",
                    self.service.model,
                    len(rows),
                    latency_ms,
                    attempt,
                    self.max_attempts,
                    is_retryable,
                )
                completed_at = datetime.now(timezone.utc)
                for row in rows:
                    self._record_run(
                        cur,
                        row=row,
                        status=status,
                        error_message=exc.message,
                        started_at=started_at,
                        completed_at=completed_at,
                    )

                if not is_retryable or not has_attempts_left:
                    for row in rows:
                        self._mark_failed(cur, row=row, error_payload=exc.to_dict())
                    return None

                sleep_seconds = self.retry_backoff_seconds * attempt
                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)

        return None

    def _fetch_pending_blocks(self, cur, *, limit: int) -> list[PendingBlock]:
        cur.execute(
            """
            SELECT
                db.block_id::text,
                db.normalized_content->>'retrieval_text' AS retrieval_text,
                COALESCE(db.source_metadata, '{}'::jsonb) AS source_metadata,
                dbe.model_name,
                dbe.embedding_dim
            FROM document_blocks db
            LEFT JOIN document_block_embeddings dbe
              ON dbe.block_id = db.block_id
            WHERE db.embedding_status = %s
              AND NULLIF(BTRIM(db.normalized_content->>'retrieval_text'), '') IS NOT NULL
            ORDER BY db.updated_at ASC, db.created_at ASC
            LIMIT %s
            FOR UPDATE OF db SKIP LOCKED
            """,
            (EMBEDDING_STATUS_READY, limit),
        )

        items: list[PendingBlock] = []
        for block_id, retrieval_text, source_metadata, model_name, embedding_dim in cur.fetchall():
            items.append(
                PendingBlock(
                    block_id=block_id,
                    retrieval_text=(retrieval_text or "").strip(),
                    source_metadata=source_metadata or {},
                    existing_model_name=model_name,
                    existing_embedding_dim=embedding_dim,
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
        has_same_model_version = existing_version_hash and existing_version_hash == self.model_version_hash
        has_existing_vector = row.existing_model_name == self.service.model

        return bool(has_same_hash and has_same_model_version and has_existing_vector)

    def _upsert_embedding(self, cur, *, row: PendingBlock, vector: list[float]) -> None:
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
                self.service.model,
                self._vector_literal(vector),
                len(vector),
            ),
        )

    def _mark_embedded(self, cur, *, row: PendingBlock, reused_existing: bool) -> None:
        payload = {
            "worker_version": WORKER_VERSION,
            "provider": self.service.provider,
            "model_name": self.service.model,
            "model_signature": self.model_signature,
            "model_version_hash": self.model_version_hash,
            "content_hash": self._content_hash(row.retrieval_text),
            "last_status": EMBEDDING_STATUS_EMBEDDED,
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

    def _mark_failed(self, cur, *, row: PendingBlock, error_payload: dict[str, Any]) -> None:
        payload = {
            "worker_version": WORKER_VERSION,
            "provider": self.service.provider,
            "model_name": self.service.model,
            "model_signature": self.model_signature,
            "model_version_hash": self.model_version_hash,
            "content_hash": self._content_hash(row.retrieval_text),
            "last_status": EMBEDDING_STATUS_FAILED,
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
                    self.service.model,
                ),
            )
        except psycopg_errors.UndefinedTable:
            self._embedding_runs_enabled = False
            logger.warning("embedding_runs table not found; run tracking is disabled until migration is applied.")

    def _build_model_signature(self) -> str:
        expected_dimension = self.service.expected_dimension or "auto"
        return f"{self.service.provider}:{self.service.model}:{expected_dimension}:{WORKER_VERSION}"

    @staticmethod
    def _content_hash(text: str) -> str:
        return hashlib.sha256((text or "").strip().encode("utf-8")).hexdigest()

    @staticmethod
    def _vector_literal(vector: list[float]) -> str:
        return "[" + ",".join(str(float(value)) for value in vector) + "]"


def run_worker(
    *,
    batch_size: int,
    limit: int,
    loop: bool,
    sleep_seconds: float,
    max_attempts: int,
    retry_backoff_seconds: float,
) -> int:
    worker = EmbeddingWorker(
        batch_size=batch_size,
        max_attempts=max_attempts,
        retry_backoff_seconds=retry_backoff_seconds,
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
        help="Linear backoff base for retryable failures (attempt * backoff).",
    )
    parser.add_argument("--loop", action="store_true", help="Keep polling for new ready blocks.")
    parser.add_argument("--sleep-seconds", type=float, default=2.0, help="Sleep interval when --loop is enabled.")
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
    )


if __name__ == "__main__":
    raise SystemExit(main())
