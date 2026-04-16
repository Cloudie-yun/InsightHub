from __future__ import annotations

import argparse
import json

from psycopg2 import errors as psycopg_errors

from db import get_db_connection
from services.extracted_content import (
    EMBEDDING_STATUS_EMBEDDED,
    EMBEDDING_STATUS_FAILED,
    EMBEDDING_STATUS_READY,
    EMBEDDING_STATUS_RETRYING,
)


def read_diagnostics(*, failure_limit: int) -> dict:
    conn = get_db_connection()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) FILTER (
                        WHERE embedding_status = %s
                          AND COALESCE(source_metadata->'embedding'->>'last_status', '') <> %s
                    ) AS ready_count,
                    COUNT(*) FILTER (
                        WHERE embedding_status = %s
                          AND COALESCE(source_metadata->'embedding'->>'last_status', '') = %s
                    ) AS retrying_count,
                    COUNT(*) FILTER (WHERE embedding_status = %s) AS embedded_count,
                    COUNT(*) FILTER (WHERE embedding_status = %s) AS failed_count
                FROM document_blocks
                """,
                (
                    EMBEDDING_STATUS_READY,
                    EMBEDDING_STATUS_RETRYING,
                    EMBEDDING_STATUS_READY,
                    EMBEDDING_STATUS_RETRYING,
                    EMBEDDING_STATUS_EMBEDDED,
                    EMBEDDING_STATUS_FAILED,
                ),
            )
            ready_count, retrying_count, embedded_count, failed_count = cur.fetchone()

            try:
                cur.execute(
                    """
                    SELECT
                        er.block_id::text,
                        er.status,
                        er.model_name,
                        er.error_message,
                        er.completed_at,
                        db.source_metadata->'embedding'->>'next_attempt_at' AS next_attempt_at,
                        COALESCE((db.source_metadata->'embedding'->>'attempt_count')::int, 0) AS attempt_count
                    FROM embedding_runs er
                    JOIN document_blocks db
                      ON db.block_id = er.block_id
                    WHERE er.status IN ('retrying', 'failed')
                    ORDER BY er.completed_at DESC NULLS LAST
                    LIMIT %s
                    """,
                    (max(1, failure_limit),),
                )
                recent_failures = [
                    {
                        "block_id": row[0],
                        "status": row[1],
                        "model_name": row[2],
                        "error_message": row[3],
                        "completed_at": row[4].isoformat() if row[4] else None,
                        "next_attempt_at": row[5],
                        "attempt_count": int(row[6] or 0),
                    }
                    for row in cur.fetchall()
                ]
            except psycopg_errors.UndefinedTable:
                recent_failures = []

        return {
            "ready_count": int(ready_count or 0),
            "pending_count": int((ready_count or 0) + (retrying_count or 0)),
            "retrying_count": int(retrying_count or 0),
            "embedded_count": int(embedded_count or 0),
            "failed_count": int(failed_count or 0),
            "recent_failures": recent_failures,
        }
    finally:
        conn.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect embedding pipeline status and recent failures.")
    parser.add_argument("--failure-limit", type=int, default=10, help="Max number of recent failed runs to include.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    payload = read_diagnostics(failure_limit=args.failure_limit)
    if args.pretty:
        print(json.dumps(payload, indent=2))
    else:
        print(json.dumps(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
