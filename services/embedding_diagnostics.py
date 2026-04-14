from __future__ import annotations

import argparse
import json

from psycopg2 import errors as psycopg_errors

from db import get_db_connection
from services.extracted_content import EMBEDDING_STATUS_EMBEDDED, EMBEDDING_STATUS_FAILED, EMBEDDING_STATUS_READY


def read_diagnostics(*, failure_limit: int) -> dict:
    conn = get_db_connection()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE embedding_status = %s) AS pending_count,
                    COUNT(*) FILTER (WHERE embedding_status = %s) AS embedded_count,
                    COUNT(*) FILTER (WHERE embedding_status = %s) AS failed_count
                FROM document_blocks
                """,
                (EMBEDDING_STATUS_READY, EMBEDDING_STATUS_EMBEDDED, EMBEDDING_STATUS_FAILED),
            )
            pending_count, embedded_count, failed_count = cur.fetchone()

            try:
                cur.execute(
                    """
                    SELECT
                        er.block_id::text,
                        er.model_name,
                        er.error_message,
                        er.completed_at
                    FROM embedding_runs er
                    WHERE er.status = 'failed'
                    ORDER BY er.completed_at DESC NULLS LAST
                    LIMIT %s
                    """,
                    (max(1, failure_limit),),
                )
                recent_failures = [
                    {
                        "block_id": row[0],
                        "model_name": row[1],
                        "error_message": row[2],
                        "completed_at": row[3].isoformat() if row[3] else None,
                    }
                    for row in cur.fetchall()
                ]
            except psycopg_errors.UndefinedTable:
                recent_failures = []

        return {
            "pending_count": int(pending_count or 0),
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
