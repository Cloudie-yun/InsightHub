from __future__ import annotations

import argparse
import html
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from db import get_db_connection
from services.retrieval_service import RetrievalService, RetrievalServiceError


def _parse_document_ids(raw: str | None) -> list[str]:
    if not raw:
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for value in raw.split(","):
        document_id = value.strip()
        if not document_id or document_id in seen:
            continue
        seen.add(document_id)
        normalized.append(document_id)
    return normalized


def _resolve_user_id(conversation_id: str) -> str:
    conn = get_db_connection()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT user_id::text
                FROM conversations
                WHERE conversation_id = %s
                """,
                (conversation_id,),
            )
            row = cur.fetchone()
            if not row:
                raise RetrievalServiceError(
                    code="conversation_not_found",
                    message="Conversation not found.",
                    status_code=404,
                    details={"conversation_id": conversation_id},
                )
            return str(row[0])
    finally:
        conn.close()


def _collect_scope_diagnostics(*, conversation_id: str, document_ids: list[str]) -> dict[str, int]:
    conn = get_db_connection()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT d.document_id::text
                FROM conversations c
                JOIN conversation_documents cd ON cd.conversation_id = c.conversation_id
                JOIN documents d              ON d.document_id       = cd.document_id
                WHERE c.conversation_id = %s
                  AND d.is_deleted = FALSE
                """,
                (conversation_id,),
            )
            allowed_document_ids = [str(row[0]) for row in cur.fetchall()]
            allowed_document_id_set = set(allowed_document_ids)

            if document_ids:
                scoped_document_ids = [doc_id for doc_id in document_ids if doc_id in allowed_document_id_set]
            else:
                scoped_document_ids = allowed_document_ids

            if not scoped_document_ids:
                return {
                    "conversation_document_count": len(allowed_document_ids),
                    "scoped_document_count": 0,
                    "scoped_block_count": 0,
                    "eligible_candidate_count": 0,
                }

            cur.execute(
                """
                SELECT
                    COUNT(*)::int AS scoped_block_count,
                    COUNT(*) FILTER (
                        WHERE db.embedding_status = 'embedded'
                          AND dbe.block_id IS NOT NULL
                          AND NULLIF(BTRIM(db.normalized_content->>'retrieval_text'), '') IS NOT NULL
                    )::int AS eligible_candidate_count
                FROM document_blocks db
                LEFT JOIN document_block_embeddings dbe ON dbe.block_id = db.block_id
                WHERE db.document_id = ANY(%s::uuid[])
                """,
                (scoped_document_ids,),
            )
            block_counts = cur.fetchone() or (0, 0)

            return {
                "conversation_document_count": len(allowed_document_ids),
                "scoped_document_count": len(scoped_document_ids),
                "scoped_block_count": int(block_counts[0] or 0),
                "eligible_candidate_count": int(block_counts[1] or 0),
            }
    finally:
        conn.close()


def run_diagnostics(
    *,
    query: str,
    conversation_id: str,
    document_ids: list[str],
    k: int | None,
) -> dict[str, Any]:
    retrieval_service = RetrievalService()
    user_id = _resolve_user_id(conversation_id)

    scope_started_at = time.perf_counter()
    scope_diagnostics = _collect_scope_diagnostics(
        conversation_id=conversation_id,
        document_ids=document_ids,
    )
    scope_duration_ms = round((time.perf_counter() - scope_started_at) * 1000.0, 2)

    retrieval_started_at = time.perf_counter()
    retrieval_payload = retrieval_service.retrieve_conversation_blocks(
        user_id=user_id,
        conversation_id=conversation_id,
        query=query,
        k=k,
        document_ids=document_ids,
    )
    retrieval_duration_ms = round((time.perf_counter() - retrieval_started_at) * 1000.0, 2)

    results = retrieval_payload.get("results", [])
    return {
        "query": retrieval_payload.get("query", query),
        "conversation_id": conversation_id,
        "document_ids": document_ids,
        "k": retrieval_payload.get("k", k),
        "timing_ms": {
            "scope_diagnostics": scope_duration_ms,
            "retrieval": retrieval_duration_ms,
            "total": round(scope_duration_ms + retrieval_duration_ms, 2),
        },
        "candidate_counts": scope_diagnostics,
        "returned_count": len(results),
        "results": results,
    }


def _render_html(payload: dict[str, Any]) -> str:
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    rows = []
    for index, result in enumerate(payload.get("results", []), start=1):
        snippet = html.escape(str(result.get("snippet") or ""))
        rows.append(
            "<tr>"
            f"<td>{index}</td>"
            f"<td><code>{html.escape(str(result.get('score', 0.0))[:10])}</code></td>"
            f"<td><code>{html.escape(str(result.get('document_id', '')))}</code></td>"
            f"<td><code>{html.escape(str(result.get('block_id', '')))}</code></td>"
            f"<td>{snippet}</td>"
            "</tr>"
        )

    if not rows:
        rows.append('<tr><td colspan="5">No retrieval results found.</td></tr>')

    stats = payload.get("candidate_counts", {})
    timing = payload.get("timing_ms", {})

    return f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <title>Retrieval Diagnostics</title>
  <style>
    body {{ font-family: Inter, Arial, sans-serif; margin: 24px; color: #111827; }}
    .grid {{ display: grid; grid-template-columns: repeat(3, minmax(180px, 1fr)); gap: 12px; margin: 16px 0 24px; }}
    .card {{ border: 1px solid #e5e7eb; border-radius: 10px; padding: 12px; background: #f9fafb; }}
    .label {{ font-size: 12px; color: #6b7280; text-transform: uppercase; letter-spacing: 0.03em; }}
    .value {{ font-size: 20px; font-weight: 600; margin-top: 6px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #e5e7eb; padding: 8px; vertical-align: top; text-align: left; }}
    th {{ background: #f3f4f6; }}
    code {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; }}
    .meta {{ color: #6b7280; font-size: 12px; margin-bottom: 8px; }}
  </style>
</head>
<body>
  <h1>Retrieval Diagnostics</h1>
  <div class=\"meta\">Generated at {generated_at}</div>
  <p><strong>Conversation:</strong> <code>{html.escape(str(payload.get("conversation_id", "")))}</code></p>
  <p><strong>Query:</strong> {html.escape(str(payload.get("query", "")))}</p>
  <p><strong>Document scope:</strong> <code>{html.escape(", ".join(payload.get("document_ids") or ["all conversation documents"]))}</code></p>

  <div class=\"grid\">
    <div class=\"card\"><div class=\"label\">K Requested</div><div class=\"value\">{payload.get("k", "n/a")}</div></div>
    <div class=\"card\"><div class=\"label\">Returned Hits</div><div class=\"value\">{payload.get("returned_count", 0)}</div></div>
    <div class=\"card\"><div class=\"label\">Eligible Candidates</div><div class=\"value\">{stats.get("eligible_candidate_count", 0)}</div></div>
    <div class=\"card\"><div class=\"label\">Scoped Blocks</div><div class=\"value\">{stats.get("scoped_block_count", 0)}</div></div>
    <div class=\"card\"><div class=\"label\">Scoped Docs</div><div class=\"value\">{stats.get("scoped_document_count", 0)}</div></div>
    <div class=\"card\"><div class=\"label\">Conversation Docs</div><div class=\"value\">{stats.get("conversation_document_count", 0)}</div></div>
    <div class=\"card\"><div class=\"label\">Scope Query (ms)</div><div class=\"value\">{timing.get("scope_diagnostics", 0)}</div></div>
    <div class=\"card\"><div class=\"label\">Retrieval (ms)</div><div class=\"value\">{timing.get("retrieval", 0)}</div></div>
    <div class=\"card\"><div class=\"label\">Total (ms)</div><div class=\"value\">{timing.get("total", 0)}</div></div>
  </div>

  <h2>Top Hits</h2>
  <table>
    <thead>
      <tr>
        <th>#</th>
        <th>Score</th>
        <th>Document ID</th>
        <th>Block ID</th>
        <th>Snippet</th>
      </tr>
    </thead>
    <tbody>
      {''.join(rows)}
    </tbody>
  </table>
</body>
</html>
"""


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run retrieval for a conversation and print ranked hits with diagnostics.",
    )
    parser.add_argument("query", help="Natural language query to run against the retrieval index.")
    parser.add_argument(
        "--conversation-id",
        required=True,
        help="Conversation UUID to query.",
    )
    parser.add_argument(
        "--document-ids",
        help="Comma-separated document UUIDs to scope retrieval.",
    )
    parser.add_argument(
        "-k",
        type=int,
        help="Top-k results to request. Defaults to RETRIEVAL_DEFAULT_K.",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Write a styled HTML report to /tmp and print its path.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    document_ids = _parse_document_ids(args.document_ids)
    payload = run_diagnostics(
        query=args.query,
        conversation_id=args.conversation_id,
        document_ids=document_ids,
        k=args.k,
    )

    if args.pretty:
        output_path = Path("/tmp") / f"retrieval_diagnostics_{int(time.time())}.html"
        output_path.write_text(_render_html(payload), encoding="utf-8")
        print(json.dumps({"report_path": os.fspath(output_path), "summary": payload}, indent=2))
    else:
        print(json.dumps(payload, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
