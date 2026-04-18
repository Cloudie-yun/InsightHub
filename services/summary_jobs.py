from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from psycopg2.extras import Json

DOCUMENT_SUMMARY_INPUT_VERSION = "document_summary_input_v1"
CONVERSATION_SUMMARY_INPUT_VERSION = "conversation_summary_input_v1"
PROCESSING_STATUS_RETRIEVAL_PREPARED = "retrieval_prepared"
JOB_STATUS_QUEUED = "queued"
JOB_STATUS_COMPLETED = "completed"
JOB_STATUS_RETRYING = "retrying"


def _relation_exists(cur, relation_name: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (relation_name,))
    row = cur.fetchone()
    return bool(row and row[0])


def _get_table_columns(cur, table_name: str) -> set[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s
        """,
        (table_name,),
    )
    return {str(row[0]) for row in cur.fetchall()}


def _pick_column(columns: set[str], *candidates: str) -> str | None:
    for name in candidates:
        if name in columns:
            return name
    return None


def _normalize_block_text(block: dict[str, Any]) -> str:
    normalized_content = block.get("normalized_content") or {}
    retrieval_text = str(normalized_content.get("retrieval_text") or "").strip()
    if retrieval_text:
        return retrieval_text
    return str(block.get("display_text") or "").strip()


def _eligible_summary_blocks(document_blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    eligible: list[dict[str, Any]] = []
    for block in document_blocks:
        processing_status = str(block.get("processing_status") or "").strip().lower()
        if processing_status != PROCESSING_STATUS_RETRIEVAL_PREPARED:
            continue
        block_text = _normalize_block_text(block)
        if not block_text:
            continue
        eligible.append(
            {
                "block_id": str(block.get("block_id") or ""),
                "source_unit_index": block.get("source_unit_index"),
                "reading_order": block.get("reading_order"),
                "text": block_text,
            }
        )

    eligible.sort(
        key=lambda item: (
            item.get("source_unit_index") if item.get("source_unit_index") is not None else 10**9,
            item.get("reading_order") if item.get("reading_order") is not None else 10**9,
            item.get("block_id") or "",
        )
    )
    return eligible


def _document_summary_content_hash(
    *,
    document_id: str,
    parser_version: str | None,
    blocks: list[dict[str, Any]],
) -> str:
    canonical_payload = {
        "version": DOCUMENT_SUMMARY_INPUT_VERSION,
        "document_id": str(document_id),
        "parser_version": str(parser_version or ""),
        "blocks": blocks,
    }
    encoded = json.dumps(canonical_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def enqueue_document_summary_job(
    cur,
    *,
    document_id: str,
    conversation_id: str | None,
    parser_version: str | None,
    document_blocks: list[dict[str, Any]],
) -> dict[str, Any]:
    if not _relation_exists(cur, "document_summary_jobs"):
        return {"enqueued": False, "reason": "table_missing"}

    eligible_blocks = _eligible_summary_blocks(document_blocks)
    if not eligible_blocks:
        return {"enqueued": False, "reason": "no_eligible_blocks"}

    content_hash = _document_summary_content_hash(
        document_id=str(document_id),
        parser_version=parser_version,
        blocks=eligible_blocks,
    )
    content_version = DOCUMENT_SUMMARY_INPUT_VERSION

    columns = _get_table_columns(cur, "document_summary_jobs")
    document_id_col = _pick_column(columns, "document_id")
    content_hash_col = _pick_column(columns, "content_hash", "source_content_hash", "input_hash")
    content_version_col = _pick_column(columns, "content_version", "summary_version", "source_version")
    conversation_id_col = _pick_column(columns, "conversation_id")
    status_col = _pick_column(columns, "status", "job_status")
    metadata_col = _pick_column(columns, "metadata")
    payload_col = _pick_column(columns, "payload")
    block_count_col = _pick_column(columns, "block_count")

    if not document_id_col:
        return {"enqueued": False, "reason": "schema_missing_document_id"}

    dedupe_conditions = [f"{document_id_col} = %s"]
    dedupe_params: list[Any] = [document_id]
    if content_hash_col:
        dedupe_conditions.append(f"{content_hash_col} = %s")
        dedupe_params.append(content_hash)
    if content_version_col:
        dedupe_conditions.append(f"{content_version_col} = %s")
        dedupe_params.append(content_version)
    if status_col:
        dedupe_conditions.append(f"{status_col} IN (%s, %s, %s, %s)")
        dedupe_params.extend([JOB_STATUS_QUEUED, "processing", JOB_STATUS_RETRYING, JOB_STATUS_COMPLETED])

    cur.execute(
        f"SELECT 1 FROM document_summary_jobs WHERE {' AND '.join(dedupe_conditions)} LIMIT 1",
        tuple(dedupe_params),
    )
    if cur.fetchone():
        return {
            "enqueued": False,
            "reason": "duplicate",
            "content_hash": content_hash,
            "content_version": content_version,
        }

    now = datetime.now(timezone.utc)
    insert_columns = [document_id_col]
    insert_values: list[Any] = [document_id]

    if conversation_id_col and conversation_id:
        insert_columns.append(conversation_id_col)
        insert_values.append(conversation_id)
    if content_hash_col:
        insert_columns.append(content_hash_col)
        insert_values.append(content_hash)
    if content_version_col:
        insert_columns.append(content_version_col)
        insert_values.append(content_version)
    if status_col:
        insert_columns.append(status_col)
        insert_values.append(JOB_STATUS_QUEUED)
    if block_count_col:
        insert_columns.append(block_count_col)
        insert_values.append(len(eligible_blocks))
    if metadata_col:
        insert_columns.append(metadata_col)
        insert_values.append(
            Json(
                {
                    "enqueued_at": now.isoformat(),
                    "parser_version": parser_version,
                    "eligible_block_count": len(eligible_blocks),
                }
            )
        )
    if payload_col:
        insert_columns.append(payload_col)
        insert_values.append(
            Json(
                {
                    "document_id": str(document_id),
                    "conversation_id": str(conversation_id) if conversation_id else None,
                    "content_hash": content_hash,
                    "content_version": content_version,
                    "blocks": eligible_blocks,
                }
            )
        )

    placeholders = ", ".join(["%s"] * len(insert_columns))
    cur.execute(
        f"INSERT INTO document_summary_jobs ({', '.join(insert_columns)}) VALUES ({placeholders})",
        tuple(insert_values),
    )

    return {
        "enqueued": True,
        "content_hash": content_hash,
        "content_version": content_version,
        "block_count": len(eligible_blocks),
    }


def enqueue_conversation_summary_recompute(
    cur,
    *,
    conversation_id: str,
    source_document_id: str | None = None,
    trigger: str = "document_summary_completed",
) -> bool:
    if not conversation_id or not _relation_exists(cur, "conversation_summary_jobs"):
        return False

    columns = _get_table_columns(cur, "conversation_summary_jobs")
    conversation_id_col = _pick_column(columns, "conversation_id")
    status_col = _pick_column(columns, "status", "job_status")
    trigger_col = _pick_column(columns, "trigger", "reason")
    source_document_col = _pick_column(columns, "source_document_id", "document_id")

    if not conversation_id_col:
        return False

    dedupe_conditions = [f"{conversation_id_col} = %s"]
    dedupe_params: list[Any] = [conversation_id]
    if status_col:
        dedupe_conditions.append(f"{status_col} IN (%s, %s, %s)")
        dedupe_params.extend([JOB_STATUS_QUEUED, "processing", JOB_STATUS_RETRYING])

    cur.execute(
        f"SELECT 1 FROM conversation_summary_jobs WHERE {' AND '.join(dedupe_conditions)} LIMIT 1",
        tuple(dedupe_params),
    )
    if cur.fetchone():
        return False

    insert_columns = [conversation_id_col]
    insert_values: list[Any] = [conversation_id]

    if status_col:
        insert_columns.append(status_col)
        insert_values.append(JOB_STATUS_QUEUED)
    if trigger_col:
        insert_columns.append(trigger_col)
        insert_values.append(trigger)
    if source_document_col and source_document_id:
        insert_columns.append(source_document_col)
        insert_values.append(source_document_id)

    placeholders = ", ".join(["%s"] * len(insert_columns))
    cur.execute(
        f"INSERT INTO conversation_summary_jobs ({', '.join(insert_columns)}) VALUES ({placeholders})",
        tuple(insert_values),
    )
    return True


def mark_document_summary_completed(
    cur,
    *,
    document_id: str,
    conversation_id: str | None,
    content_hash: str | None = None,
    content_version: str | None = None,
) -> bool:
    if not _relation_exists(cur, "document_summary_jobs"):
        return False

    columns = _get_table_columns(cur, "document_summary_jobs")
    document_id_col = _pick_column(columns, "document_id")
    status_col = _pick_column(columns, "status", "job_status")
    content_hash_col = _pick_column(columns, "content_hash", "source_content_hash", "input_hash")
    content_version_col = _pick_column(columns, "content_version", "summary_version", "source_version")

    if not document_id_col or not status_col:
        return False

    update_conditions = [f"{document_id_col} = %s"]
    params: list[Any] = [document_id, JOB_STATUS_COMPLETED]
    if content_hash_col and content_hash:
        update_conditions.append(f"{content_hash_col} = %s")
        params.append(content_hash)
    if content_version_col and content_version:
        update_conditions.append(f"{content_version_col} = %s")
        params.append(content_version)

    cur.execute(
        f"UPDATE document_summary_jobs SET {status_col} = %s WHERE {' AND '.join(update_conditions)}",
        tuple(params),
    )

    if conversation_id:
        enqueue_conversation_summary_recompute(
            cur,
            conversation_id=conversation_id,
            source_document_id=document_id,
            trigger="document_summary_completed",
        )
    return True
