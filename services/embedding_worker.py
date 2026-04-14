from __future__ import annotations

import argparse
import hashlib
import logging
import time
from dataclasses import dataclass
from typing import Any

from psycopg2.extras import Json

from db import get_db_connection
from services.embedding_service import EmbeddingService, EmbeddingServiceError
from services.extracted_content import EMBEDDING_STATUS_EMBEDDED, EMBEDDING_STATUS_FAILED, EMBEDDING_STATUS_READY


DEFAULT_BATCH_SIZE = 64
DEFAULT_LIMIT = 256
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
    def __init__(self, *, batch_size: int = DEFAULT_BATCH_SIZE, service: EmbeddingService | None = None) -> None:
        self.batch_size = max(1, min(batch_size, 512))
        self.service = service or EmbeddingService()
        self.model_signature = self._build_model_signature()
        self.model_version_hash = hashlib.sha256(self.model_signature.encode("utf-8")).hexdigest()

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

                    texts = [row.retrieval_text for row in to_embed]
                    try:
                        vectors = self.service.embed_texts(texts)
                    except EmbeddingServiceError as exc:
                        logger.exception("Embedding provider failed for %s blocks.", len(to_embed))
                        for row in to_embed:
                            self._mark_failed(cur, row=row, error_payload=exc.to_dict())
                            stats["failed"] += 1
                        return stats

                    for row, vector in zip(to_embed, vectors):
                        self._upsert_embedding(cur, row=row, vector=vector)
                        self._mark_embedded(cur, row=row, reused_existing=False)
                        stats["embedded"] += 1

            return stats
        finally:
            conn.close()

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

    def _build_model_signature(self) -> str:
        expected_dimension = self.service.expected_dimension or "auto"
        return f"{self.service.provider}:{self.service.model}:{expected_dimension}:{WORKER_VERSION}"

    @staticmethod
    def _content_hash(text: str) -> str:
        return hashlib.sha256((text or "").strip().encode("utf-8")).hexdigest()

    @staticmethod
    def _vector_literal(vector: list[float]) -> str:
        return "[" + ",".join(str(float(value)) for value in vector) + "]"


def run_worker(*, batch_size: int, limit: int, loop: bool, sleep_seconds: float) -> int:
    worker = EmbeddingWorker(batch_size=batch_size)

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
    )


if __name__ == "__main__":
    raise SystemExit(main())
