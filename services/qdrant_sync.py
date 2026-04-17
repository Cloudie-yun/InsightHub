from __future__ import annotations

import argparse
import logging

from db import get_db_connection
from services.qdrant_index_service import QdrantIndexService, QdrantServiceError, build_qdrant_payload


logger = logging.getLogger(__name__)


def _parse_vector_literal(value: str) -> list[float]:
    raw = str(value or "").strip()
    if raw.startswith("[") and raw.endswith("]"):
        raw = raw[1:-1]
    if not raw:
        return []
    return [float(part) for part in raw.split(",") if str(part).strip()]


def sync_qdrant(*, batch_size: int, document_id: str | None = None) -> int:
    service = QdrantIndexService()
    if not service.enabled:
        raise QdrantServiceError(
            code="qdrant_not_configured",
            message="Set QDRANT_URL before running qdrant sync.",
            status_code=503,
            details={},
        )

    conn = get_db_connection()
    synced = 0
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(MAX(dbe.embedding_dim), 0)::int
                FROM document_block_embeddings dbe
                """
            )
            dimension = int((cur.fetchone() or [0])[0] or 0)
            if dimension <= 0:
                return 0
            service.ensure_collection(vector_size=dimension)

            params = []
            scope_sql = ""
            if document_id:
                scope_sql = "AND db.document_id = %s::uuid"
                params.append(document_id)

            cur.execute(
                f"""
                SELECT
                    db.block_id::text,
                    db.document_id::text,
                    d.original_filename,
                    db.block_type,
                    db.subtype,
                    db.normalized_content,
                    COALESCE(db.source_metadata, '{{}}'::jsonb) AS source_metadata,
                    db.normalized_content->>'retrieval_text' AS retrieval_text,
                    dbe.embedding::text AS embedding_literal
                FROM document_blocks db
                JOIN document_block_embeddings dbe ON dbe.block_id = db.block_id
                JOIN documents d ON d.document_id = db.document_id
                WHERE db.embedding_status = 'embedded'
                  AND d.is_deleted = FALSE
                  AND NULLIF(BTRIM(db.normalized_content->>'retrieval_text'), '') IS NOT NULL
                  {scope_sql}
                ORDER BY db.updated_at ASC, db.created_at ASC
                """,
                params,
            )

            points: list[dict] = []
            for row in cur.fetchall():
                (
                    block_id,
                    doc_id,
                    document_name,
                    block_type,
                    subtype,
                    normalized_content,
                    source_metadata,
                    retrieval_text,
                    embedding_literal,
                ) = row
                vector = _parse_vector_literal(embedding_literal)
                if not vector:
                    continue
                payload = build_qdrant_payload(
                    block_id=str(block_id or ""),
                    document_id=str(doc_id or ""),
                    document_name=document_name or "",
                    block_type=str(block_type or "").strip().lower(),
                    subtype=str(subtype or "").strip().lower(),
                    normalized_content=normalized_content if isinstance(normalized_content, dict) else {},
                    source_metadata=source_metadata if isinstance(source_metadata, dict) else {},
                    retrieval_text=retrieval_text or "",
                )
                points.append({"id": str(block_id or ""), "vector": vector, "payload": payload})
                if len(points) >= batch_size:
                    service.upsert_points(points=points)
                    synced += len(points)
                    points = []

            if points:
                service.upsert_points(points=points)
                synced += len(points)
        return synced
    finally:
        conn.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync embedded document blocks into Qdrant.")
    parser.add_argument("--batch-size", type=int, default=128)
    parser.add_argument("--document-id")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    synced = sync_qdrant(batch_size=max(1, int(args.batch_size)), document_id=args.document_id)
    logger.info("qdrant-sync upserted=%s", synced)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
