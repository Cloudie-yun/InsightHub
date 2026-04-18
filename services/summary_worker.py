from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from db import get_db_connection
from services.summary_jobs import enqueue_conversation_summary_recompute
from services.summary_service import (
    CONVERSATION_TITLE_PROMPT_VERSION,
    GeminiSummaryService,
    SummaryServiceError,
)


WORKER_VERSION = "summary_worker_v1"
JOB_STATUS_QUEUED = "queued"
JOB_STATUS_PROCESSING = "processing"
JOB_STATUS_RETRYING = "retrying"
JOB_STATUS_COMPLETED = "completed"
JOB_STATUS_FAILED = "failed"
DOCUMENT_SUMMARY_SOURCE_VERSION = "document_summary_source_v1"
CONVERSATION_SUMMARY_SOURCE_VERSION = "conversation_summary_source_v1"
CONVERSATION_SUMMARY_INTRO_PROMPT_VERSION = "conversation_summary_intro_v1"
DEFAULT_LIMIT = int(os.environ.get("SUMMARY_WORKER_LIMIT", "8"))
DEFAULT_MAX_ATTEMPTS = int(os.environ.get("SUMMARY_WORKER_MAX_ATTEMPTS", "4"))
DEFAULT_RETRY_BACKOFF_SECONDS = float(os.environ.get("SUMMARY_RETRY_BACKOFF_SECONDS", "10"))
DEFAULT_RETRY_MAX_SECONDS = float(os.environ.get("SUMMARY_RETRY_MAX_SECONDS", "300"))
DEFAULT_RETRY_JITTER_SECONDS = float(os.environ.get("SUMMARY_RETRY_JITTER_SECONDS", "3"))
DEFAULT_POLL_SECONDS = float(os.environ.get("SUMMARY_RETRY_POLL_SECONDS", "20"))

logger = logging.getLogger(__name__)


def _relation_exists(cur, relation_name: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (relation_name,))
    row = cur.fetchone()
    return bool(row and row[0])


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Unsupported value for JSON encoding: {type(value)!r}")


def _json_hash(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=_json_default).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").replace("\r", " ").replace("\n", " ").split()).strip()


def _build_conversation_summary_intro_text(*, document_count: int, summary: dict[str, Any]) -> str:
    summary_text = str(summary.get("summary_text") or "").strip()
    key_points = summary.get("key_points") or []
    lead = (
        f"Your {document_count} document{'s are' if document_count != 1 else ' is'} processed and ready for Q&A."
        if document_count
        else "Your documents are processed and ready for Q&A."
    )
    lines = [lead]
    if summary_text:
        lines.extend(["", f"Conversation summary: {summary_text}"])
    normalized_points = []
    for item in key_points:
        point = _normalize_text(item)
        if point:
            normalized_points.append(point)
        if len(normalized_points) >= 3:
            break
    if normalized_points:
        lines.extend(["", "Key points:"])
        lines.extend(f"- {point}" for point in normalized_points)
    return "\n".join(lines).strip()


@dataclass
class PendingDocumentSummaryJob:
    job_id: str
    document_id: str
    conversation_id: str | None
    content_hash: str
    content_version: str
    attempt_count: int
    payload: dict[str, Any]


@dataclass
class PendingConversationSummaryJob:
    job_id: str
    conversation_id: str
    source_document_id: str | None
    attempt_count: int


class SummaryNotReadyError(RuntimeError):
    pass


class SummaryWorker:
    def __init__(
        self,
        *,
        limit: int = DEFAULT_LIMIT,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        retry_backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS,
        retry_max_seconds: float = DEFAULT_RETRY_MAX_SECONDS,
        retry_jitter_seconds: float = DEFAULT_RETRY_JITTER_SECONDS,
        service: GeminiSummaryService | None = None,
    ) -> None:
        self.limit = max(1, int(limit or DEFAULT_LIMIT))
        self.max_attempts = max(1, int(max_attempts or DEFAULT_MAX_ATTEMPTS))
        self.retry_backoff_seconds = max(0.0, float(retry_backoff_seconds or DEFAULT_RETRY_BACKOFF_SECONDS))
        self.retry_max_seconds = max(self.retry_backoff_seconds, float(retry_max_seconds or DEFAULT_RETRY_MAX_SECONDS))
        self.retry_jitter_seconds = max(0.0, float(retry_jitter_seconds or DEFAULT_RETRY_JITTER_SECONDS))
        self.service = service or GeminiSummaryService()

    @staticmethod
    def _should_preserve_attempt_budget(*, error_code: str | None = None, message: str = "") -> bool:
        normalized_code = str(error_code or "").strip().lower()
        normalized_message = str(message or "").strip().lower()
        if normalized_code == "summary_model_unavailable":
            return True
        return "no compatible model is currently available for task_type=text" in normalized_message

    def run_once(self) -> dict[str, int]:
        stats = {
            "document_jobs_selected": 0,
            "document_jobs_completed": 0,
            "document_jobs_failed": 0,
            "conversation_jobs_selected": 0,
            "conversation_jobs_completed": 0,
            "conversation_jobs_failed": 0,
        }

        conn = get_db_connection()
        try:
            with conn:
                with conn.cursor() as cur:
                    if not self._schema_ready(cur):
                        return stats

                    document_jobs = self._fetch_document_jobs(cur, limit=self.limit)
                    stats["document_jobs_selected"] = len(document_jobs)
                    for job in document_jobs:
                        if self._process_document_job(cur, job):
                            stats["document_jobs_completed"] += 1
                        else:
                            stats["document_jobs_failed"] += 1

                    conversation_jobs = self._fetch_conversation_jobs(cur, limit=self.limit)
                    stats["conversation_jobs_selected"] = len(conversation_jobs)
                    for job in conversation_jobs:
                        if self._process_conversation_job(cur, job):
                            stats["conversation_jobs_completed"] += 1
                        else:
                            stats["conversation_jobs_failed"] += 1

            return stats
        finally:
            conn.close()

    def _schema_ready(self, cur) -> bool:
        required_relations = (
            "document_summaries",
            "conversation_summaries",
            "document_summary_jobs",
            "conversation_summary_jobs",
        )
        return all(_relation_exists(cur, name) for name in required_relations)

    def _fetch_document_jobs(self, cur, *, limit: int) -> list[PendingDocumentSummaryJob]:
        cur.execute(
            """
            SELECT
                job_id::text,
                document_id::text,
                conversation_id::text,
                content_hash,
                content_version,
                attempt_count,
                COALESCE(payload, '{}'::jsonb)
            FROM document_summary_jobs
            WHERE status IN (%s, %s)
              AND (next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP)
            ORDER BY created_at ASC
            LIMIT %s
            FOR UPDATE SKIP LOCKED
            """,
            (JOB_STATUS_QUEUED, JOB_STATUS_RETRYING, limit),
        )
        rows = cur.fetchall()
        jobs: list[PendingDocumentSummaryJob] = []
        for row in rows:
            jobs.append(
                PendingDocumentSummaryJob(
                    job_id=str(row[0]),
                    document_id=str(row[1]),
                    conversation_id=str(row[2]) if row[2] else None,
                    content_hash=str(row[3] or ""),
                    content_version=str(row[4] or ""),
                    attempt_count=max(0, int(row[5] or 0)),
                    payload=row[6] if isinstance(row[6], dict) else {},
                )
            )
        return jobs

    def _fetch_conversation_jobs(self, cur, *, limit: int) -> list[PendingConversationSummaryJob]:
        cur.execute(
            """
            SELECT
                job_id::text,
                conversation_id::text,
                source_document_id::text,
                attempt_count
            FROM conversation_summary_jobs
            WHERE status IN (%s, %s)
              AND (next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP)
            ORDER BY created_at ASC
            LIMIT %s
            FOR UPDATE SKIP LOCKED
            """,
            (JOB_STATUS_QUEUED, JOB_STATUS_RETRYING, limit),
        )
        rows = cur.fetchall()
        jobs: list[PendingConversationSummaryJob] = []
        for row in rows:
            jobs.append(
                PendingConversationSummaryJob(
                    job_id=str(row[0]),
                    conversation_id=str(row[1]),
                    source_document_id=str(row[2]) if row[2] else None,
                    attempt_count=max(0, int(row[3] or 0)),
                )
            )
        return jobs

    def _process_document_job(self, cur, job: PendingDocumentSummaryJob) -> bool:
        self._mark_job_processing(cur, table_name="document_summary_jobs", job_id=job.job_id)
        try:
            document_name = self._fetch_document_name(cur, document_id=job.document_id)
            blocks = job.payload.get("blocks") or []
            summary = self.service.summarize_document(
                document_name=document_name,
                blocks=blocks,
            )
            self._upsert_document_summary(
                cur,
                job=job,
                document_name=document_name,
                summary=summary,
            )
            cur.execute(
                """
                UPDATE document_summary_jobs
                SET
                    status = %s,
                    completed_at = CURRENT_TIMESTAMP,
                    error_message = NULL,
                    next_attempt_at = NULL
                WHERE job_id = %s::uuid
                """,
                (JOB_STATUS_COMPLETED, job.job_id),
            )
            if job.conversation_id:
                enqueue_conversation_summary_recompute(
                    cur,
                    conversation_id=job.conversation_id,
                    source_document_id=job.document_id,
                    trigger="document_summary_completed",
                )
            return True
        except SummaryServiceError as exc:
            self._handle_job_failure(
                cur,
                table_name="document_summary_jobs",
                job_id=job.job_id,
                attempt_count=job.attempt_count,
                message=exc.message,
                retryable=exc.retryable,
                preserve_attempt_budget=self._should_preserve_attempt_budget(
                    error_code=exc.code,
                    message=exc.message,
                ),
                retry_after_seconds=exc.retry_after_seconds,
                summary_target="document",
                target_id=job.document_id,
            )
            self._upsert_failed_document_summary(cur, job=job, message=exc.message)
            return False
        except Exception as exc:
            self._handle_job_failure(
                cur,
                table_name="document_summary_jobs",
                job_id=job.job_id,
                attempt_count=job.attempt_count,
                message=str(exc),
                retryable=True,
                preserve_attempt_budget=self._should_preserve_attempt_budget(message=str(exc)),
                summary_target="document",
                target_id=job.document_id,
            )
            self._upsert_failed_document_summary(cur, job=job, message=str(exc))
            return False

    def _process_conversation_job(self, cur, job: PendingConversationSummaryJob) -> bool:
        self._mark_job_processing(cur, table_name="conversation_summary_jobs", job_id=job.job_id)
        try:
            conversation_documents = self._load_conversation_summary_inputs(cur, conversation_id=job.conversation_id)
            summary = self.service.summarize_conversation(
                conversation_id=job.conversation_id,
                documents=conversation_documents,
            )
            source_payload = {
                "version": CONVERSATION_SUMMARY_SOURCE_VERSION,
                "documents": [
                    {
                        "document_id": item["document_id"],
                        "source_content_hash": item["source_content_hash"],
                        "source_version": item["source_version"],
                    }
                    for item in conversation_documents
                ],
            }
            source_content_hash = _json_hash(source_payload)
            self._upsert_conversation_summary(
                cur,
                job=job,
                summary=summary,
                source_content_hash=source_content_hash,
                document_count=len(conversation_documents),
            )
            cur.execute(
                """
                UPDATE conversation_summary_jobs
                SET
                    status = %s,
                    completed_at = CURRENT_TIMESTAMP,
                    error_message = NULL,
                    next_attempt_at = NULL,
                    content_hash = %s,
                    content_version = %s
                WHERE job_id = %s::uuid
                """,
                (
                    JOB_STATUS_COMPLETED,
                    source_content_hash,
                    CONVERSATION_SUMMARY_SOURCE_VERSION,
                    job.job_id,
                ),
            )
            return True
        except SummaryNotReadyError as exc:
            self._handle_job_failure(
                cur,
                table_name="conversation_summary_jobs",
                job_id=job.job_id,
                attempt_count=job.attempt_count,
                message=str(exc),
                retryable=True,
                preserve_attempt_budget=False,
                summary_target="conversation",
                target_id=job.conversation_id,
            )
            return False
        except SummaryServiceError as exc:
            self._handle_job_failure(
                cur,
                table_name="conversation_summary_jobs",
                job_id=job.job_id,
                attempt_count=job.attempt_count,
                message=exc.message,
                retryable=exc.retryable,
                preserve_attempt_budget=self._should_preserve_attempt_budget(
                    error_code=exc.code,
                    message=exc.message,
                ),
                retry_after_seconds=exc.retry_after_seconds,
                summary_target="conversation",
                target_id=job.conversation_id,
            )
            self._upsert_failed_conversation_summary(cur, conversation_id=job.conversation_id, message=exc.message)
            return False
        except Exception as exc:
            self._handle_job_failure(
                cur,
                table_name="conversation_summary_jobs",
                job_id=job.job_id,
                attempt_count=job.attempt_count,
                message=str(exc),
                retryable=True,
                preserve_attempt_budget=self._should_preserve_attempt_budget(message=str(exc)),
                summary_target="conversation",
                target_id=job.conversation_id,
            )
            self._upsert_failed_conversation_summary(cur, conversation_id=job.conversation_id, message=str(exc))
            return False

    def _fetch_document_name(self, cur, *, document_id: str) -> str:
        cur.execute(
            """
            SELECT original_filename
            FROM documents
            WHERE document_id = %s::uuid
            """,
            (document_id,),
        )
        row = cur.fetchone()
        return str((row or ["Untitled document"])[0] or "Untitled document")

    def _load_conversation_summary_inputs(self, cur, *, conversation_id: str) -> list[dict[str, Any]]:
        cur.execute(
            """
            SELECT
                d.document_id::text,
                d.original_filename,
                COALESCE(de.parser_status, 'pending'),
                ds.status,
                ds.summary_text,
                ds.summary_payload,
                ds.title_hint,
                ds.source_content_hash,
                ds.source_version
            FROM conversation_documents cd
            JOIN documents d
              ON d.document_id = cd.document_id
            LEFT JOIN document_extractions de
              ON de.document_id = d.document_id
            LEFT JOIN document_summaries ds
              ON ds.document_id = d.document_id
            WHERE cd.conversation_id = %s::uuid
              AND d.is_deleted = FALSE
            ORDER BY cd.added_at ASC, d.created_at ASC
            """,
            (conversation_id,),
        )
        rows = cur.fetchall()
        if not rows:
            raise SummaryNotReadyError("Conversation has no documents to summarize yet.")

        inputs: list[dict[str, Any]] = []
        pending_documents = 0
        for row in rows:
            document_id = str(row[0] or "")
            document_name = str(row[1] or "")
            parser_status = str(row[2] or "pending").strip().lower()
            summary_status = str(row[3] or "").strip().lower()
            if parser_status == "pending":
                pending_documents += 1
                continue
            if parser_status != "success":
                continue
            if summary_status == JOB_STATUS_FAILED:
                raise SummaryServiceError(
                    code="document_summary_failed",
                    message=f"Document summary failed for document_id={document_id}.",
                    status_code=500,
                    retryable=False,
                )
            if summary_status != JOB_STATUS_COMPLETED:
                raise SummaryNotReadyError(
                    f"Document summary is not ready for document_id={document_id}."
                )

            summary_payload = row[5] if isinstance(row[5], dict) else {}
            inputs.append(
                {
                    "document_id": document_id,
                    "document_name": document_name,
                    "summary_text": str(row[4] or summary_payload.get("summary_text") or ""),
                    "key_points": summary_payload.get("key_points") or [],
                    "topics": summary_payload.get("topics") or [],
                    "title_hint": str(row[6] or summary_payload.get("title_hint") or ""),
                    "source_content_hash": str(row[7] or ""),
                    "source_version": str(row[8] or ""),
                }
            )

        if pending_documents:
            raise SummaryNotReadyError("Conversation still has documents pending parse completion.")
        if not inputs:
            raise SummaryNotReadyError("No successful document summaries are available for this conversation.")
        return inputs

    def _upsert_document_summary(self, cur, *, job: PendingDocumentSummaryJob, document_name: str, summary: dict[str, Any]) -> None:
        payload = {
            "summary_text": summary.get("summary_text") or "",
            "key_points": summary.get("key_points") or [],
            "topics": summary.get("topics") or [],
            "title_hint": summary.get("title_hint") or document_name,
            "prompt_version": summary.get("prompt_version") or DOCUMENT_SUMMARY_SOURCE_VERSION,
        }
        cur.execute(
            """
            INSERT INTO document_summaries (
                document_id,
                conversation_id,
                source_content_hash,
                source_version,
                status,
                summary_text,
                title_hint,
                summary_payload,
                provider_name,
                model_name,
                token_count,
                error_message,
                completed_at
            )
            VALUES (
                %s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, NULL, CURRENT_TIMESTAMP
            )
            ON CONFLICT (document_id)
            DO UPDATE SET
                conversation_id = EXCLUDED.conversation_id,
                source_content_hash = EXCLUDED.source_content_hash,
                source_version = EXCLUDED.source_version,
                status = EXCLUDED.status,
                summary_text = EXCLUDED.summary_text,
                title_hint = EXCLUDED.title_hint,
                summary_payload = EXCLUDED.summary_payload,
                provider_name = EXCLUDED.provider_name,
                model_name = EXCLUDED.model_name,
                token_count = EXCLUDED.token_count,
                error_message = NULL,
                completed_at = EXCLUDED.completed_at
            """,
            (
                job.document_id,
                job.conversation_id,
                job.content_hash,
                job.content_version or DOCUMENT_SUMMARY_SOURCE_VERSION,
                JOB_STATUS_COMPLETED,
                payload["summary_text"],
                payload["title_hint"],
                json.dumps(payload),
                summary.get("provider_name"),
                summary.get("model_name"),
                summary.get("token_count"),
            ),
        )

    def _upsert_failed_document_summary(self, cur, *, job: PendingDocumentSummaryJob, message: str) -> None:
        cur.execute(
            """
            INSERT INTO document_summaries (
                document_id,
                conversation_id,
                source_content_hash,
                source_version,
                status,
                summary_text,
                title_hint,
                summary_payload,
                error_message
            )
            VALUES (%s::uuid, %s::uuid, %s, %s, %s, NULL, NULL, '{}'::jsonb, %s)
            ON CONFLICT (document_id)
            DO UPDATE SET
                conversation_id = EXCLUDED.conversation_id,
                source_content_hash = EXCLUDED.source_content_hash,
                source_version = EXCLUDED.source_version,
                status = EXCLUDED.status,
                summary_text = NULL,
                title_hint = NULL,
                summary_payload = '{}'::jsonb,
                error_message = EXCLUDED.error_message
            """,
            (
                job.document_id,
                job.conversation_id,
                job.content_hash,
                job.content_version or DOCUMENT_SUMMARY_SOURCE_VERSION,
                JOB_STATUS_FAILED,
                message[:2000],
            ),
        )

    def _upsert_conversation_summary(
        self,
        cur,
        *,
        job: PendingConversationSummaryJob,
        summary: dict[str, Any],
        source_content_hash: str,
        document_count: int,
    ) -> None:
        payload = {
            "summary_text": summary.get("summary_text") or "",
            "key_points": summary.get("key_points") or [],
            "topics": summary.get("topics") or [],
            "generated_title": summary.get("generated_title") or "",
            "prompt_version": summary.get("prompt_version") or CONVERSATION_SUMMARY_PROMPT_VERSION,
            "title_prompt_version": summary.get("title_prompt_version") or CONVERSATION_TITLE_PROMPT_VERSION,
        }
        cur.execute(
            """
            INSERT INTO conversation_summaries (
                conversation_id,
                source_content_hash,
                source_version,
                status,
                document_count,
                summary_text,
                generated_title,
                summary_payload,
                provider_name,
                model_name,
                token_count,
                error_message,
                completed_at
            )
            VALUES (
                %s::uuid, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, NULL, CURRENT_TIMESTAMP
            )
            ON CONFLICT (conversation_id)
            DO UPDATE SET
                source_content_hash = EXCLUDED.source_content_hash,
                source_version = EXCLUDED.source_version,
                status = EXCLUDED.status,
                document_count = EXCLUDED.document_count,
                summary_text = EXCLUDED.summary_text,
                generated_title = EXCLUDED.generated_title,
                summary_payload = EXCLUDED.summary_payload,
                provider_name = EXCLUDED.provider_name,
                model_name = EXCLUDED.model_name,
                token_count = EXCLUDED.token_count,
                error_message = NULL,
                completed_at = EXCLUDED.completed_at
            """,
            (
                job.conversation_id,
                source_content_hash,
                CONVERSATION_SUMMARY_SOURCE_VERSION,
                JOB_STATUS_COMPLETED,
                document_count,
                payload["summary_text"],
                payload["generated_title"],
                json.dumps(payload),
                summary.get("provider_name"),
                summary.get("model_name"),
                (summary.get("token_count") or 0) + (summary.get("title_token_count") or 0),
            ),
        )
        generated_title = _normalize_text(summary.get("generated_title"))[:255]
        if generated_title:
            cur.execute(
                """
                UPDATE conversations
                SET
                    title = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE conversation_id = %s::uuid
                """,
                (generated_title, job.conversation_id),
            )
        self._upsert_conversation_summary_intro_message(
            cur,
            conversation_id=job.conversation_id,
            document_count=document_count,
            summary=summary,
        )

    def _upsert_failed_conversation_summary(self, cur, *, conversation_id: str, message: str) -> None:
        cur.execute(
            """
            INSERT INTO conversation_summaries (
                conversation_id,
                source_content_hash,
                source_version,
                status,
                document_count,
                summary_text,
                generated_title,
                summary_payload,
                error_message
            )
            VALUES (%s::uuid, %s, %s, %s, 0, NULL, NULL, '{}'::jsonb, %s)
            ON CONFLICT (conversation_id)
            DO UPDATE SET
                source_content_hash = EXCLUDED.source_content_hash,
                source_version = EXCLUDED.source_version,
                status = EXCLUDED.status,
                document_count = EXCLUDED.document_count,
                summary_text = NULL,
                generated_title = NULL,
                summary_payload = '{}'::jsonb,
                error_message = EXCLUDED.error_message
            """,
            (
                conversation_id,
                hashlib.sha256(f"failed:{conversation_id}".encode("utf-8")).hexdigest(),
                CONVERSATION_SUMMARY_SOURCE_VERSION,
                JOB_STATUS_FAILED,
                message[:2000],
            ),
        )

    def _upsert_conversation_summary_intro_message(
        self,
        cur,
        *,
        conversation_id: str,
        document_count: int,
        summary: dict[str, Any],
    ) -> None:
        if not _relation_exists(cur, "conversation_messages"):
            return

        intro_text = _build_conversation_summary_intro_text(
            document_count=document_count,
            summary=summary,
        )
        retrieval_payload = {
            "summary_intro": True,
            "summary_type": "conversation_overview",
            "document_count": document_count,
            "grounded_answer": {
                "confidence": "high",
            },
            "summary_payload": {
                "summary_text": summary.get("summary_text") or "",
                "key_points": summary.get("key_points") or [],
                "topics": summary.get("topics") or [],
                "generated_title": summary.get("generated_title") or "",
            },
            "results": [],
            "citations": [],
            "filter_summary": {},
            "returned_count": 0,
            "k": 0,
            "strategy": "summary_intro",
        }

        cur.execute(
            """
            SELECT user_id, created_at
            FROM conversations
            WHERE conversation_id = %s::uuid
            LIMIT 1
            """,
            (conversation_id,),
        )
        conversation_row = cur.fetchone()
        if not conversation_row:
            return
        user_id = conversation_row[0]
        conversation_created_at = conversation_row[1]

        cur.execute(
            """
            SELECT message_id::text, created_at
            FROM conversation_messages
            WHERE conversation_id = %s::uuid
              AND prompt_version = %s
              AND role = 'assistant'
              AND reply_to_message_id IS NULL
            ORDER BY created_at ASC, message_id ASC
            LIMIT 1
            """,
            (conversation_id, CONVERSATION_SUMMARY_INTRO_PROMPT_VERSION),
        )
        existing_row = cur.fetchone()

        if existing_row:
            cur.execute(
                """
                UPDATE conversation_messages
                SET
                    message_text = %s,
                    retrieval_payload = %s::jsonb,
                    model_provider = %s,
                    model_name = %s
                WHERE message_id = %s::uuid
                """,
                (
                    intro_text,
                    json.dumps(retrieval_payload),
                    summary.get("provider_name"),
                    summary.get("model_name"),
                    existing_row[0],
                ),
            )
            return

        cur.execute(
            """
            SELECT created_at
            FROM conversation_messages
            WHERE conversation_id = %s::uuid
            ORDER BY created_at ASC, message_id ASC
            LIMIT 1
            """,
            (conversation_id,),
        )
        first_message_row = cur.fetchone()
        intro_created_at = conversation_created_at or datetime.now(timezone.utc)
        if first_message_row and first_message_row[0]:
            intro_created_at = first_message_row[0] - timedelta(seconds=1)

        if _relation_exists(cur, "conversation_messages") and self._conversation_messages_support_versioning(cur):
            cur.execute(
                """
                INSERT INTO conversation_messages (
                    conversation_id,
                    user_id,
                    role,
                    message_text,
                    selected_document_ids,
                    retrieval_payload,
                    model_provider,
                    model_name,
                    prompt_version,
                    reply_to_message_id,
                    family_id,
                    family_version_number,
                    branch_parent_message_id,
                    is_active_in_family,
                    created_at
                )
                VALUES (
                    %s::uuid, %s::uuid, 'assistant', %s, '[]'::jsonb, %s::jsonb, %s, %s, %s, NULL, gen_random_uuid(), 1, NULL, TRUE, %s
                )
                """,
                (
                    conversation_id,
                    user_id,
                    intro_text,
                    json.dumps(retrieval_payload),
                    summary.get("provider_name"),
                    summary.get("model_name"),
                    CONVERSATION_SUMMARY_INTRO_PROMPT_VERSION,
                    intro_created_at,
                ),
            )
            return

        cur.execute(
            """
            INSERT INTO conversation_messages (
                conversation_id,
                user_id,
                role,
                message_text,
                selected_document_ids,
                retrieval_payload,
                model_provider,
                model_name,
                prompt_version,
                reply_to_message_id,
                created_at
            )
            VALUES (
                %s::uuid, %s::uuid, 'assistant', %s, '[]'::jsonb, %s::jsonb, %s, %s, %s, NULL, %s
            )
            """,
            (
                conversation_id,
                user_id,
                intro_text,
                json.dumps(retrieval_payload),
                summary.get("provider_name"),
                summary.get("model_name"),
                CONVERSATION_SUMMARY_INTRO_PROMPT_VERSION,
                intro_created_at,
            ),
        )

    @staticmethod
    def _conversation_messages_support_versioning(cur) -> bool:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'conversation_messages'
              AND column_name IN ('family_id', 'family_version_number', 'branch_parent_message_id', 'is_active_in_family')
            """,
        )
        available = {str(row[0]) for row in cur.fetchall()}
        return {
            'family_id',
            'family_version_number',
            'branch_parent_message_id',
            'is_active_in_family',
        }.issubset(available)

    def _mark_job_processing(self, cur, *, table_name: str, job_id: str) -> None:
        cur.execute(
            f"""
            UPDATE {table_name}
            SET
                status = %s,
                started_at = CURRENT_TIMESTAMP,
                error_message = NULL
            WHERE job_id = %s::uuid
            """,
            (JOB_STATUS_PROCESSING, job_id),
        )

    def _handle_job_failure(
        self,
        cur,
        *,
        table_name: str,
        job_id: str,
        attempt_count: int,
        message: str,
        retryable: bool,
        preserve_attempt_budget: bool = False,
        retry_after_seconds: float | None = None,
        summary_target: str,
        target_id: str,
    ) -> None:
        next_attempt_count = max(0, int(attempt_count or 0))
        if not preserve_attempt_budget:
            next_attempt_count += 1
        should_retry = retryable and next_attempt_count < self.max_attempts
        next_attempt_at = None
        status = JOB_STATUS_FAILED
        if should_retry:
            if retry_after_seconds is not None:
                delay_seconds = max(0.0, float(retry_after_seconds))
            else:
                retry_delay_attempt = next_attempt_count or 1
                delay_seconds = self._compute_retry_delay(attempt_count=retry_delay_attempt)
            next_attempt_at = datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)
            status = JOB_STATUS_RETRYING
        cur.execute(
            f"""
            UPDATE {table_name}
            SET
                status = %s,
                attempt_count = %s,
                error_message = %s,
                next_attempt_at = %s,
                completed_at = CASE WHEN %s = '{JOB_STATUS_FAILED}' THEN CURRENT_TIMESTAMP ELSE NULL END
            WHERE job_id = %s::uuid
            """,
            (
                status,
                next_attempt_count,
                str(message or "")[:2000],
                next_attempt_at,
                status,
                job_id,
            ),
        )
        logger.warning(
            "summary-worker %s failure target=%s status=%s attempts=%s message=%s",
            summary_target,
            target_id,
            status,
            next_attempt_count,
            message,
        )

    def _compute_retry_delay(self, *, attempt_count: int) -> float:
        exponential_delay = self.retry_backoff_seconds * (2 ** max(0, attempt_count - 1))
        bounded_delay = min(self.retry_max_seconds, exponential_delay)
        jitter = random.uniform(0.0, self.retry_jitter_seconds) if self.retry_jitter_seconds > 0 else 0.0
        return max(0.0, bounded_delay + jitter)


def run_worker(
    *,
    loop: bool,
    sleep_seconds: float,
    limit: int,
    max_attempts: int,
    retry_backoff_seconds: float,
    retry_max_seconds: float,
    retry_jitter_seconds: float,
) -> int:
    worker = SummaryWorker(
        limit=limit,
        max_attempts=max_attempts,
        retry_backoff_seconds=retry_backoff_seconds,
        retry_max_seconds=retry_max_seconds,
        retry_jitter_seconds=retry_jitter_seconds,
    )

    while True:
        stats = worker.run_once()
        logger.info(
            "summary-worker doc_selected=%s doc_completed=%s doc_failed=%s convo_selected=%s convo_completed=%s convo_failed=%s",
            stats["document_jobs_selected"],
            stats["document_jobs_completed"],
            stats["document_jobs_failed"],
            stats["conversation_jobs_selected"],
            stats["conversation_jobs_completed"],
            stats["conversation_jobs_failed"],
        )
        total_selected = stats["document_jobs_selected"] + stats["conversation_jobs_selected"]
        if not loop or total_selected == 0:
            return 0
        time.sleep(max(0.1, sleep_seconds))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate document and conversation summaries.")
    parser.add_argument("--loop", action="store_true", help="Keep polling for queued summary jobs.")
    parser.add_argument("--sleep-seconds", type=float, default=DEFAULT_POLL_SECONDS, help="Sleep interval when --loop is enabled.")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Max jobs to claim per pass for each queue.")
    parser.add_argument("--max-attempts", type=int, default=DEFAULT_MAX_ATTEMPTS, help="Max retry attempts per job.")
    parser.add_argument("--retry-backoff-seconds", type=float, default=DEFAULT_RETRY_BACKOFF_SECONDS)
    parser.add_argument("--retry-max-seconds", type=float, default=DEFAULT_RETRY_MAX_SECONDS)
    parser.add_argument("--retry-jitter-seconds", type=float, default=DEFAULT_RETRY_JITTER_SECONDS)
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    return run_worker(
        loop=args.loop,
        sleep_seconds=args.sleep_seconds,
        limit=args.limit,
        max_attempts=args.max_attempts,
        retry_backoff_seconds=args.retry_backoff_seconds,
        retry_max_seconds=args.retry_max_seconds,
        retry_jitter_seconds=args.retry_jitter_seconds,
    )


if __name__ == "__main__":
    raise SystemExit(main())
