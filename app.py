from flask import Flask, render_template, jsonify, request, send_from_directory, abort, session, redirect, url_for
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path
from threading import Lock, Thread
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin, urlencode, quote, urlparse
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
import subprocess
import psycopg2
from psycopg2 import errors
from psycopg2.extras import Json
from werkzeug.security import generate_password_hash, check_password_hash
import hashlib
import secrets
import os
import re
import json
import logging
import mimetypes
import time
from dotenv import load_dotenv
from huggingface_hub import login as huggingface_login

load_dotenv()

from db import get_db_connection
from email_service import send_email

from werkzeug.middleware.proxy_fix import ProxyFix
from services.document_parser import parse_document
from services.diagram_vision_service import (
    DiagramVisionThrottleError,
    get_diagram_vision_provider_order,
    get_primary_diagram_vision_provider,
    run_diagram_analysis_for_document,
)
from services.embedding_worker import EmbeddingWorker
from services.embedding_service import EmbeddingServiceError
from services.gemini_credentials import load_gemini_api_credentials
from services.extraction_store import (
    build_extraction_payload,
    build_pending_extraction_payload,
    save_document_extraction,
    get_document_extraction as fetch_document_extraction,
    get_conversation_extractions as fetch_conversation_extractions,
)
from services.summary_jobs import enqueue_document_summary_job
from services.summary_worker import SummaryWorker
from services.chat_answer_service import ChatAnswerService, ChatAnswerServiceError
from services.prompt_profile_service import (
    get_default_prompt_profiles,
    PROMPT_PROFILE_MAX_LENGTH,
    PROMPT_TYPE_QNA,
    PROMPT_TYPE_VISION,
    get_prompt_profiles_for_user,
    save_prompt_profiles_for_user,
)
from services.retrieval_service import RetrievalService, RetrievalServiceError
from services.quota_router import (
    TASK_TYPE_DIAGRAM_VISION,
    TASK_TYPE_TEXT,
    format_quota_timestamp,
    get_quota_project_id,
    get_task_models,
    get_quota_display_timezone,
    load_model_limits,
    load_usage_state,
)


# ===========================================================================
# 1. APP SETUP & CONFIGURATION
# ===========================================================================

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret-change-me")
logger = logging.getLogger(__name__)

hf_token = os.getenv("HF_TOKEN")
if hf_token:
    huggingface_login(token=hf_token)
else:
    logger.warning("HF_TOKEN not set; continuing without Hugging Face authentication")

DOCUMENT_PARSE_MAX_WORKERS = max(1, int(os.getenv("DOCUMENT_PARSE_MAX_WORKERS", "2")))
document_parse_executor = ThreadPoolExecutor(
    max_workers=DOCUMENT_PARSE_MAX_WORKERS,
    thread_name_prefix="document-parse",
)

UPLOADS_DIR = Path(app.root_path) / "uploads"
PREVIEW_DIR = UPLOADS_DIR / ".preview"
PREVIEW_DIR.mkdir(parents=True, exist_ok=True)

ALLOWED_UPLOAD_EXTENSIONS = {
    ".pdf", ".doc", ".docx", ".ppt", ".pptx", ".txt",
    ".png", ".jpg", ".jpeg", ".webp",
}

STRONG_PASSWORD_REGEX = re.compile(
    r"^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[^A-Za-z\d]).{8,}$"
)
PASSWORD_POLICY_ERROR = (
    "Password must be at least 8 characters and include uppercase, "
    "lowercase, number, and special character."
)

def _build_default_user_system_prompt(username: str) -> str:
    safe_name = (username or "").strip() or "there"
    return (
        f"You are InsightHub assistant helping {safe_name}.\n"
        "Prioritize clear, practical answers with short steps.\n"
        "When files are attached, ground answers in the uploaded content and cite specific evidence.\n"
        "If information is uncertain or missing, say so and propose the next best action."
    )


def _is_truthy_env(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _read_float_env(value: str | None, default: float) -> float:
    try:
        parsed = float(str(value or "").strip())
    except (TypeError, ValueError):
        return default
    return parsed


EMBEDDING_AUTORUN_ENABLED = _is_truthy_env(os.getenv("EMBEDDING_AUTORUN_ENABLED", "1"))
EMBEDDING_AUTORUN_LIMIT = max(1, int(os.getenv("EMBEDDING_AUTORUN_LIMIT", "256")))
EMBEDDING_RETRY_POLL_SECONDS = max(1.0, _read_float_env(os.getenv("EMBEDDING_RETRY_POLL_SECONDS"), 15.0))
embedding_autorun_executor = ThreadPoolExecutor(
    max_workers=1,
    thread_name_prefix="embedding-autorun",
)
embedding_autorun_lock = Lock()
embedding_autorun_running = False
embedding_autorun_requested = False
embedding_retry_poller_lock = Lock()
embedding_retry_poller_started = False
SUMMARY_AUTORUN_ENABLED = _is_truthy_env(os.getenv("SUMMARY_AUTORUN_ENABLED", "1"))
SUMMARY_AUTORUN_LIMIT = max(1, int(os.getenv("SUMMARY_AUTORUN_LIMIT", "8")))
SUMMARY_RETRY_POLL_SECONDS = max(1.0, _read_float_env(os.getenv("SUMMARY_RETRY_POLL_SECONDS"), 20.0))
summary_autorun_executor = ThreadPoolExecutor(
    max_workers=1,
    thread_name_prefix="summary-autorun",
)
summary_autorun_lock = Lock()
summary_autorun_running = False
summary_autorun_requested = False
summary_retry_poller_lock = Lock()
summary_retry_poller_started = False


def _relation_exists(cur, relation_name: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (relation_name,))
    row = cur.fetchone()
    return bool(row and row[0])


def _diagram_vision_schema_ready(cur) -> bool:
    required_relations = (
        "document_blocks",
        "document_block_assets",
        "diagram_block_details",
        "diagram_block_analysis_runs",
    )
    missing = [name for name in required_relations if not _relation_exists(cur, name)]
    if missing:
        logger.warning(
            "Skipping diagram vision analysis because required tables are missing: %s",
            ", ".join(missing),
        )
        return False
    return True


def _read_optional_int_env(name: str) -> int | None:
    raw_value = str(os.getenv(name, "") or "").strip()
    if not raw_value:
        return None
    try:
        return max(0, int(raw_value))
    except (TypeError, ValueError):
        logger.warning("Ignoring invalid integer env value for %s=%r", name, raw_value)
        return None


def _run_embedding_autorun(trigger: str, limit: int) -> None:
    global embedding_autorun_requested, embedding_autorun_running

    while True:
        try:
            stats = EmbeddingWorker().run_once(limit=limit)
            logger.info(
                "embedding autorun completed trigger=%s selected=%s embedded=%s failed=%s skipped_idempotent=%s",
                trigger,
                stats["selected"],
                stats["embedded"],
                stats["failed"],
                stats["skipped_idempotent"],
            )
        except Exception:
            logger.exception("Embedding autorun failed for trigger=%s", trigger)

        with embedding_autorun_lock:
            if embedding_autorun_requested:
                embedding_autorun_requested = False
                trigger = f"{trigger}:coalesced"
                continue

            embedding_autorun_running = False
            break


def _run_summary_autorun(trigger: str, limit: int) -> None:
    global summary_autorun_requested, summary_autorun_running

    while True:
        try:
            stats = SummaryWorker(limit=limit).run_once()
            logger.info(
                "summary autorun completed trigger=%s doc_selected=%s doc_completed=%s doc_failed=%s convo_selected=%s convo_completed=%s convo_failed=%s",
                trigger,
                stats["document_jobs_selected"],
                stats["document_jobs_completed"],
                stats["document_jobs_failed"],
                stats["conversation_jobs_selected"],
                stats["conversation_jobs_completed"],
                stats["conversation_jobs_failed"],
            )
        except Exception:
            logger.exception("Summary autorun failed for trigger=%s", trigger)

        with summary_autorun_lock:
            if summary_autorun_requested:
                summary_autorun_requested = False
                trigger = f"{trigger}:coalesced"
                continue

            summary_autorun_running = False
            break


def _schedule_embedding_autorun(trigger: str, *, limit: int = EMBEDDING_AUTORUN_LIMIT) -> None:
    global embedding_autorun_requested, embedding_autorun_running

    if not EMBEDDING_AUTORUN_ENABLED:
        return

    with embedding_autorun_lock:
        if embedding_autorun_running:
            embedding_autorun_requested = True
            logger.info("Embedding autorun already running; coalescing trigger=%s", trigger)
            return
        embedding_autorun_running = True
        embedding_autorun_requested = False

    try:
        embedding_autorun_executor.submit(_run_embedding_autorun, trigger, limit)
    except Exception:
        with embedding_autorun_lock:
            embedding_autorun_running = False
            embedding_autorun_requested = False
        logger.exception("Unable to start embedding autorun for trigger=%s", trigger)


def _schedule_summary_autorun(trigger: str, *, limit: int = SUMMARY_AUTORUN_LIMIT) -> None:
    global summary_autorun_requested, summary_autorun_running

    if not SUMMARY_AUTORUN_ENABLED:
        return

    with summary_autorun_lock:
        if summary_autorun_running:
            summary_autorun_requested = True
            logger.info("Summary autorun already running; coalescing trigger=%s", trigger)
            return
        summary_autorun_running = True
        summary_autorun_requested = False

    try:
        summary_autorun_executor.submit(_run_summary_autorun, trigger, limit)
    except Exception:
        with summary_autorun_lock:
            summary_autorun_running = False
            summary_autorun_requested = False
        logger.exception("Unable to start summary autorun for trigger=%s", trigger)


FLASHCARD_GENERATION_RESPONSE_SCHEMA = {
    "type": "object",
    "required": ["deck_title", "cards"],
    "properties": {
        "deck_title": {"type": "string"},
        "cards": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["front", "back"],
                "properties": {
                    "front": {"type": "string"},
                    "back": {"type": "string"},
                },
            },
        },
    },
}

MINDMAP_GENERATION_RESPONSE_SCHEMA = {
    "type": "object",
    "required": ["title", "nodes"],
    "properties": {
        "title": {"type": "string"},
        "nodes": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["id", "parentId", "text"],
                "properties": {
                    "id": {"type": "string"},
                    "parentId": {"type": "string", "nullable": True},
                    "text": {"type": "string"},
                },
            },
        },
    },
}


def _extract_gemini_text_payload(raw_response: dict) -> str:
    candidates = raw_response.get("candidates") if isinstance(raw_response.get("candidates"), list) else []
    first_candidate = candidates[0] if candidates else {}
    parts = ((first_candidate.get("content") or {}).get("parts") or [])
    for part in parts:
        text = str((part or {}).get("text") or "").strip()
        if text:
            return text
    return ""


def _coerce_json_object_text(raw_text: str) -> str:
    text = str(raw_text or "").strip()
    if not text:
        return ""

    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text).strip()

    first_brace = text.find("{")
    last_brace = text.rfind("}")
    if first_brace != -1 and last_brace != -1 and last_brace > first_brace:
        text = text[first_brace:last_brace + 1].strip()

    return text


def _request_gemini_structured_output(
    *,
    prompt: str,
    response_schema: dict,
    max_output_tokens: int,
    temperature: float = 0.4,
) -> tuple[dict, str]:
    credentials = load_gemini_api_credentials()
    api_key = credentials[0].api_key if credentials else ""
    if not api_key:
        raise ValueError("Gemini API key is missing. Set GEMINI_API_KEY or GEMINI_API_KEYS.")

    model_candidates = get_task_models(
        TASK_TYPE_TEXT,
        fallback_model=os.getenv("GEMINI_TEXT_MODEL", "gemini-2.5-flash"),
    )
    model_name = model_candidates[0] if model_candidates else "gemini-2.5-flash"
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key}"

    def _submit_request(prompt_text: str, request_temperature: float) -> str:
        payload = {
            "contents": [
                {
                    "role": "user",
                    "parts": [{"text": prompt_text}],
                }
            ],
            "generationConfig": {
                "responseMimeType": "application/json",
                "responseSchema": response_schema,
                "temperature": request_temperature,
                "topP": 0.9,
                "maxOutputTokens": max_output_tokens,
            },
        }
        request_payload = json.dumps(payload).encode("utf-8")
        req = Request(url, data=request_payload, headers={"Content-Type": "application/json"}, method="POST")

        try:
            with urlopen(req, timeout=90) as response:
                response_body = response.read().decode("utf-8")
        except HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")
            logger.warning("Flashcard generation HTTP error status=%s body=%s", exc.code, error_body[:500])
            error_message = f"AI generation request failed ({exc.code})."
            try:
                error_payload = json.loads(error_body)
                api_message = (
                    error_payload.get("error", {}).get("message")
                    if isinstance(error_payload.get("error"), dict)
                    else error_payload.get("message")
                )
                if api_message:
                    error_message = f"{error_message} {str(api_message).strip()}"
            except json.JSONDecodeError:
                snippet = error_body[:240].strip()
                if snippet:
                    error_message = f"{error_message} {snippet}"
            raise ValueError(error_message) from exc
        except URLError as exc:
            raise ValueError("AI generation is currently unavailable.") from exc

        raw_response = json.loads(response_body)
        response_text = _extract_gemini_text_payload(raw_response)
        if not response_text:
            raise ValueError("AI returned an empty response.")
        return response_text

    response_text = _submit_request(prompt, temperature)
    try:
        parsed = json.loads(_coerce_json_object_text(response_text))
    except json.JSONDecodeError as exc:
        logger.warning("Structured AI response was invalid JSON. Retrying once. error=%s snippet=%s", exc, response_text[:400])
        retry_prompt = (
            f"{prompt}\n\n"
            "IMPORTANT: Return exactly one valid JSON object matching the schema. "
            "Do not include markdown fences, commentary, or unescaped quotes inside JSON strings."
        )
        retry_text = _submit_request(retry_prompt, 0.2)
        parsed = json.loads(_coerce_json_object_text(retry_text))

    if not isinstance(parsed, dict):
        raise ValueError("AI returned an invalid JSON payload.")
    return parsed, model_name


def _parse_page_range(page_range_raw: str | None) -> list[int]:
    normalized = str(page_range_raw or "").strip()
    if not normalized:
        return []

    pages: set[int] = set()
    for chunk in re.split(r"\s*,\s*", normalized):
        if not chunk:
            continue
        if "-" in chunk:
            start_raw, end_raw = chunk.split("-", 1)
            start = int(start_raw.strip())
            end = int(end_raw.strip())
            if start <= 0 or end <= 0:
                raise ValueError("Page numbers must be positive.")
            if end < start:
                start, end = end, start
            for value in range(start, min(end, start + 49) + 1):
                pages.add(value)
        else:
            value = int(chunk)
            if value <= 0:
                raise ValueError("Page numbers must be positive.")
            pages.add(value)

    return sorted(pages)[:200]


def _extract_segment_page_number(segment: dict) -> int | None:
    metadata = segment.get("metadata") if isinstance(segment.get("metadata"), dict) else {}
    candidate_keys = (
        "page",
        "page_number",
        "source_page",
        "source_page_number",
        "page_no",
    )
    for key in candidate_keys:
        raw_value = metadata.get(key)
        try:
            page_number = int(raw_value)
        except (TypeError, ValueError):
            continue
        if page_number > 0:
            return page_number
    raw_page_index = metadata.get("page_index")
    try:
        page_index = int(raw_page_index)
    except (TypeError, ValueError):
        return None
    return page_index + 1 if page_index >= 0 else None


def _load_document_study_source(
    *,
    user_id,
    document_id: str,
    conversation_id: str | None = None,
    page_range: str | None = None,
) -> dict:
    normalized_document_id = str(document_id or "").strip()
    normalized_conversation_id = str(conversation_id or "").strip() or None
    if not user_id or not normalized_document_id:
        raise ValueError("A document is required.")

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            if normalized_conversation_id:
                cur.execute(
                    """
                    SELECT d.original_filename
                    FROM documents d
                    JOIN conversation_documents cd ON cd.document_id = d.document_id
                    JOIN conversations c ON c.conversation_id = cd.conversation_id
                    WHERE d.document_id = %s::uuid
                      AND cd.conversation_id = %s::uuid
                      AND c.user_id = %s
                      AND d.is_deleted = FALSE
                    LIMIT 1
                    """,
                    (normalized_document_id, normalized_conversation_id, user_id),
                )
            else:
                cur.execute(
                    """
                    SELECT d.original_filename
                    FROM documents d
                    WHERE d.document_id = %s::uuid
                      AND d.user_id = %s
                      AND d.is_deleted = FALSE
                    LIMIT 1
                    """,
                    (normalized_document_id, user_id),
                )
            row = cur.fetchone()
            if not row:
                raise ValueError("Document not found.")

            extraction_payload = fetch_document_extraction(
                cur,
                document_id=normalized_document_id,
                conversation_id=normalized_conversation_id,
            ) or {}
    finally:
        if conn is not None:
            conn.close()

    summary_payload = get_document_summary(
        user_id,
        normalized_document_id,
        conversation_id=normalized_conversation_id,
    ) or {}
    selected_pages = _parse_page_range(page_range)
    selected_page_set = set(selected_pages)

    document_name = str(row[0] or "").strip() or "Document"
    segments = extraction_payload.get("segments") if isinstance(extraction_payload.get("segments"), list) else []
    segment_lines: list[str] = []
    matched_page_count = 0
    for segment in segments:
        text = str(segment.get("text") or "").strip()
        if not text:
            continue
        page_number = _extract_segment_page_number(segment)
        if selected_page_set and page_number and page_number not in selected_page_set:
            continue
        if selected_page_set and page_number in selected_page_set:
            matched_page_count += 1
        prefix = f"[Page {page_number}] " if page_number else ""
        segment_lines.append(f"{prefix}{text}")
        if len(segment_lines) >= 80:
            break

    if selected_page_set and not matched_page_count and segment_lines:
        segment_lines = segment_lines[:40]

    summary_text = str(summary_payload.get("summary_text") or "").strip()
    text_parts = []
    if summary_text:
        text_parts.append(f"Document summary:\n{summary_text}")
    if segment_lines:
        text_parts.append("Document excerpts:\n" + "\n\n".join(segment_lines))
    source_text = "\n\n".join(text_parts).strip()
    if not source_text:
        raise ValueError("Document text is not ready yet. Try again after parsing finishes.")

    return {
        "document_id": normalized_document_id,
        "document_name": document_name,
        "conversation_id": normalized_conversation_id,
        "page_range": str(page_range or "").strip(),
        "source_text": source_text[:24000],
    }


def _study_aids_table_ready(cur) -> bool:
    return _relation_exists(cur, "study_aids")


def save_study_aid(
    *,
    user_id,
    aid_type: str,
    title: str,
    payload_json: dict,
    conversation_id: str | None = None,
    document_id: str | None = None,
    source_requirements: str = "",
    page_range: str = "",
) -> dict | None:
    if not user_id:
        return None

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            if not _study_aids_table_ready(cur):
                return None
            cur.execute(
                """
                INSERT INTO study_aids (
                    user_id,
                    conversation_id,
                    document_id,
                    aid_type,
                    title,
                    source_requirements,
                    page_range,
                    payload_json
                )
                VALUES (%s, %s::uuid, %s::uuid, %s, %s, %s, %s, %s::jsonb)
                RETURNING study_aid_id::text, created_at
                """,
                (
                    user_id,
                    conversation_id or None,
                    document_id or None,
                    aid_type,
                    str(title or "").strip(),
                    str(source_requirements or "").strip(),
                    str(page_range or "").strip(),
                    Json(payload_json or {}),
                ),
            )
            row = cur.fetchone()
            conn.commit()
        return {
            "study_aid_id": str(row[0] or ""),
            "created_at": row[1].isoformat() if row and row[1] else None,
        }
    except Exception:
        if conn is not None:
            conn.rollback()
        logger.exception("Unable to save study aid aid_type=%s document_id=%s conversation_id=%s", aid_type, document_id, conversation_id)
        return None
    finally:
        if conn is not None:
            conn.close()


def get_study_aid(user_id, study_aid_id: str, *, aid_type: str | None = None) -> dict | None:
    if not user_id or not study_aid_id:
        return None

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            if not _study_aids_table_ready(cur):
                return None
            query = """
                SELECT
                    study_aid_id::text,
                    user_id::text,
                    conversation_id::text,
                    document_id::text,
                    aid_type,
                    title,
                    source_requirements,
                    page_range,
                    payload_json,
                    created_at,
                    updated_at
                FROM study_aids
                WHERE study_aid_id = %s::uuid
                  AND user_id = %s
            """
            params: list = [study_aid_id, user_id]
            if aid_type:
                query += " AND aid_type = %s"
                params.append(aid_type)
            query += " LIMIT 1"
            cur.execute(query, tuple(params))
            row = cur.fetchone()
        if not row:
            return None
        return {
            "study_aid_id": str(row[0] or ""),
            "user_id": str(row[1] or ""),
            "conversation_id": str(row[2] or "") if row[2] else "",
            "document_id": str(row[3] or "") if row[3] else "",
            "aid_type": str(row[4] or ""),
            "title": str(row[5] or ""),
            "source_requirements": str(row[6] or ""),
            "page_range": str(row[7] or ""),
            "payload_json": row[8] if isinstance(row[8], dict) else {},
            "created_at": row[9].isoformat() if row[9] else None,
            "updated_at": row[10].isoformat() if row[10] else None,
        }
    except Exception:
        logger.exception("Unable to load study aid study_aid_id=%s", study_aid_id)
        return None
    finally:
        if conn is not None:
            conn.close()


def list_study_aids(user_id, *, aid_type: str, limit: int = 50) -> list[dict]:
    if not user_id:
        return []
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            if not _study_aids_table_ready(cur):
                return []
            cur.execute(
                """
                SELECT
                    study_aid_id::text,
                    title,
                    document_id::text,
                    conversation_id::text,
                    created_at,
                    updated_at
                FROM study_aids
                WHERE user_id = %s
                  AND aid_type = %s
                ORDER BY updated_at DESC, created_at DESC
                LIMIT %s
                """,
                (user_id, aid_type, max(1, int(limit))),
            )
            rows = cur.fetchall()
        return [
            {
                "study_aid_id": str(row[0] or ""),
                "title": str(row[1] or "").strip() or ("Untitled Flashcards" if aid_type == "flashcards" else "Untitled Mind Map"),
                "document_id": str(row[2] or "") if row[2] else "",
                "conversation_id": str(row[3] or "") if row[3] else "",
                "created_at": row[4].isoformat() if row[4] else None,
                "updated_at": row[5].isoformat() if row[5] else None,
            }
            for row in rows
        ]
    except Exception:
        logger.exception("Unable to list study aids aid_type=%s", aid_type)
        return []
    finally:
        if conn is not None:
            conn.close()


def list_conversation_study_aids(user_id, conversation_id: str, *, limit: int = 20) -> list[dict]:
    if not user_id or not conversation_id:
        return []
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            if not _study_aids_table_ready(cur):
                return []
            cur.execute(
                """
                SELECT
                    study_aid_id::text,
                    aid_type,
                    title,
                    document_id::text,
                    created_at,
                    updated_at
                FROM study_aids
                WHERE user_id = %s
                  AND conversation_id = %s::uuid
                ORDER BY updated_at DESC, created_at DESC
                LIMIT %s
                """,
                (user_id, conversation_id, max(1, int(limit))),
            )
            rows = cur.fetchall()
        return [
            {
                "study_aid_id": str(row[0] or ""),
                "aid_type": str(row[1] or ""),
                "title": str(row[2] or "").strip() or "Untitled Study Aid",
                "document_id": str(row[3] or "") if row[3] else "",
                "created_at": row[4].isoformat() if row[4] else None,
                "updated_at": row[5].isoformat() if row[5] else None,
            }
            for row in rows
        ]
    except Exception:
        logger.exception("Unable to list conversation study aids conversation_id=%s", conversation_id)
        return []
    finally:
        if conn is not None:
            conn.close()


def update_study_aid(
    *,
    user_id,
    study_aid_id: str,
    title: str,
    payload_json: dict,
    source_requirements: str = "",
    page_range: str = "",
) -> dict | None:
    if not user_id or not study_aid_id:
        return None
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            if not _study_aids_table_ready(cur):
                return None
            cur.execute(
                """
                UPDATE study_aids
                SET title = %s,
                    source_requirements = %s,
                    page_range = %s,
                    payload_json = %s::jsonb,
                    updated_at = CURRENT_TIMESTAMP
                WHERE study_aid_id = %s::uuid
                  AND user_id = %s
                RETURNING study_aid_id::text, updated_at
                """,
                (
                    str(title or "").strip(),
                    str(source_requirements or "").strip(),
                    str(page_range or "").strip(),
                    Json(payload_json or {}),
                    study_aid_id,
                    user_id,
                ),
            )
            row = cur.fetchone()
            conn.commit()
        if not row:
            return None
        return {"study_aid_id": str(row[0] or ""), "updated_at": row[1].isoformat() if row[1] else None}
    except Exception:
        if conn is not None:
            conn.rollback()
        logger.exception("Unable to update study aid study_aid_id=%s", study_aid_id)
        return None
    finally:
        if conn is not None:
            conn.close()


DEFAULT_FLASHCARD_GENERATION_PROMPT = """I want you to act as a professional Anki flashcard creator, able to create Anki cards based on topics I provide. Use the SuperMemo principles for creating cards. Make sure that the cards are self-contained and that the questions are clear and unambiguous. Prefer questions that have a single well-defined answer.

Here are some good examples of flashcards for the topic "Linear Regression":
Q: What is the weak exogeneity assumption?
A: That the predictor variables can be treated as fixed values, rather than random variables.

Q: What is the most common method for fitting linear regression?
A: Ordinary least-squares.

Finally, when making flashcards, make sure that they are independent of one another.

I also want you to keep answers as short as possible. If mathematical formulas are useful, format them with MathJax delimiters like \\( \\) or \\[ \\]. Do not restate the question in the answer."""


def _generate_flashcards_with_ai(
    *,
    topic: str = "",
    count: int,
    document_source: dict | None = None,
    requirements: str = "",
    user_prompt: str = "",
) -> dict:
    normalized_requirements = str(requirements or "").strip()
    normalized_user_prompt = str(user_prompt or "").strip() or DEFAULT_FLASHCARD_GENERATION_PROMPT
    if document_source:
        prompt = (
            "Use the flashcard authoring instructions below when generating the deck.\n"
            "Keep answers short unless extra detail is required for accuracy.\n\n"
            f"{normalized_user_prompt}\n\n"
            "Generate a study flashcard deck using the document material below.\n"
            "Return only valid JSON that matches the provided schema.\n"
            f"Create exactly {count} cards.\n"
            "Each card must have a concise question or term on the front and a short, clear, accurate answer on the back.\n"
            "Avoid duplicates, filler, markdown, numbering, and commentary.\n"
            "Keep fronts short and backs compact.\n"
            f"Document name: {document_source['document_name']}\n"
            f"Page range: {document_source.get('page_range') or 'all available pages'}\n"
            f"Additional requirements: {normalized_requirements or 'None'}\n\n"
            f"{document_source['source_text']}"
        )
    else:
        prompt = (
            "Use the flashcard authoring instructions below when generating the deck.\n"
            "Keep answers short unless extra detail is required for accuracy.\n\n"
            f"{normalized_user_prompt}\n\n"
            "Generate a study flashcard deck for the topic below.\n"
            "Return only valid JSON that matches the provided schema.\n"
            f"Create exactly {count} cards.\n"
            "Each card must have a concise question or term on the front and a short, clear, accurate answer on the back.\n"
            "Avoid duplicates, filler, markdown, numbering, and commentary.\n"
            "Keep fronts short and backs compact.\n"
            f"Additional requirements: {normalized_requirements or 'None'}\n"
            f"Topic:\n{topic.strip()}"
        )

    parsed, model_name = _request_gemini_structured_output(
        prompt=prompt,
        response_schema=FLASHCARD_GENERATION_RESPONSE_SCHEMA,
        max_output_tokens=max(1024, min(8192, count * 220)),
        temperature=0.5,
    )
    cards = []
    for item in parsed.get("cards") or []:
        front = str((item or {}).get("front") or "").strip()
        back = str((item or {}).get("back") or "").strip()
        if front and back:
            cards.append({"front": front, "back": back})

    if not cards:
        raise ValueError("AI returned no valid flashcards.")

    deck_title = str(parsed.get("deck_title") or "").strip() or "AI Flashcards"
    return {
        "deck_title": deck_title,
        "cards": cards[:count],
        "model": model_name,
        "generation_prompt": normalized_user_prompt,
    }


def _generate_mindmap_with_ai(
    *,
    document_source: dict,
    requirements: str = "",
) -> dict:
    normalized_requirements = str(requirements or "").strip()
    prompt = (
        "Generate a concise study mind map using the document material below.\n"
        "Return only valid JSON that matches the provided schema.\n"
        "Create one root node, 4 to 7 major branches, and useful sub-branches where relevant.\n"
        "Node labels must stay short, clear, and study-oriented. Avoid sentences where a short label works.\n"
        "Do not include markdown or commentary.\n"
        f"Document name: {document_source['document_name']}\n"
        f"Page range: {document_source.get('page_range') or 'all available pages'}\n"
        f"Additional requirements: {normalized_requirements or 'None'}\n\n"
        f"{document_source['source_text']}"
    )
    parsed, model_name = _request_gemini_structured_output(
        prompt=prompt,
        response_schema=MINDMAP_GENERATION_RESPONSE_SCHEMA,
        max_output_tokens=4096,
        temperature=0.45,
    )
    raw_nodes = parsed.get("nodes") if isinstance(parsed.get("nodes"), list) else []
    nodes: list[dict] = []
    seen_ids: set[str] = set()
    for item in raw_nodes:
        node_id = str((item or {}).get("id") or "").strip()
        parent_id_raw = (item or {}).get("parentId")
        parent_id = str(parent_id_raw).strip() if parent_id_raw is not None else None
        text = str((item or {}).get("text") or "").strip()
        if not node_id or not text or node_id in seen_ids:
            continue
        seen_ids.add(node_id)
        nodes.append({"id": node_id, "parentId": parent_id or None, "text": text[:80]})

    if not nodes:
        raise ValueError("AI returned no valid mind map nodes.")

    if not any(node.get("parentId") is None for node in nodes):
        nodes[0]["parentId"] = None

    return {
        "title": str(parsed.get("title") or document_source["document_name"]).strip() or "Mind Map",
        "nodes": nodes[:80],
        "model": model_name,
    }


def _embedding_retry_poller_loop() -> None:
    while True:
        try:
            _schedule_embedding_autorun("retry_poller")
        except Exception:
            logger.exception("Embedding retry poller loop failed")
        time.sleep(EMBEDDING_RETRY_POLL_SECONDS)


def _summary_retry_poller_loop() -> None:
    while True:
        try:
            _schedule_summary_autorun("retry_poller")
        except Exception:
            logger.exception("Summary retry poller loop failed")
        time.sleep(SUMMARY_RETRY_POLL_SECONDS)


def _start_embedding_retry_poller() -> None:
    global embedding_retry_poller_started

    if not EMBEDDING_AUTORUN_ENABLED:
        return

    with embedding_retry_poller_lock:
        if embedding_retry_poller_started:
            return
        embedding_retry_poller_started = True

    poller = Thread(
        target=_embedding_retry_poller_loop,
        name="embedding-retry-poller",
        daemon=True,
    )
    poller.start()
    logger.info(
        "Started embedding retry poller interval_seconds=%s limit=%s",
        EMBEDDING_RETRY_POLL_SECONDS,
        EMBEDDING_AUTORUN_LIMIT,
    )


def _start_summary_retry_poller() -> None:
    global summary_retry_poller_started

    if not SUMMARY_AUTORUN_ENABLED:
        return

    with summary_retry_poller_lock:
        if summary_retry_poller_started:
            return
        summary_retry_poller_started = True

    poller = Thread(
        target=_summary_retry_poller_loop,
        name="summary-retry-poller",
        daemon=True,
    )
    poller.start()
    logger.info(
        "Started summary retry poller interval_seconds=%s limit=%s",
        SUMMARY_RETRY_POLL_SECONDS,
        SUMMARY_AUTORUN_LIMIT,
    )


def _build_diagram_analysis_usage_summary(cur) -> dict:
    del cur
    project_id = get_quota_project_id()
    ordered_models = get_task_models(TASK_TYPE_DIAGRAM_VISION, fallback_model=os.getenv("GEMINI_VISION_MODEL", "gemini-3-flash"))
    provider_order = get_diagram_vision_provider_order()
    primary_provider = get_primary_diagram_vision_provider()
    usage_state = load_usage_state(project_id=project_id, model_names=ordered_models)
    model_limits = load_model_limits(model_names=ordered_models)
    now_utc = datetime.now(timezone.utc)
    display_timezone = str(get_quota_display_timezone())

    model_statuses = []
    earliest_reset_at: datetime | None = None
    available_models: list[str] = []

    for model_name in ordered_models:
        windows = usage_state.get(model_name, {})
        model_limit = model_limits.get(model_name)
        blocked_windows = [
            {
                "window_type": window.window_type,
                "used_count": min(window.used_count, limit_value) if limit_value is not None else window.used_count,
                "raw_used_count": window.used_count,
                "limit_value": limit_value,
                "reset_at": window.reset_at.isoformat() if window.reset_at else None,
                "reset_at_display": format_quota_timestamp(window.reset_at),
                "last_error_at": window.last_error_at.isoformat() if window.last_error_at else None,
                "last_error_at_display": format_quota_timestamp(window.last_error_at),
                "last_error_code": window.last_error_code,
            }
            for window in windows.values()
            for limit_value in [model_limit.limit_for(window.window_type) if model_limit else None]
            if window.is_exhausted or (
                limit_value is not None
                and window.used_count >= int(limit_value or 0)
            )
        ]
        is_available = not blocked_windows
        status_label = "available" if is_available and windows else "untracked"
        if blocked_windows:
            status_label = "blocked"
        if is_available:
            available_models.append(model_name)
        for blocked_window in blocked_windows:
            reset_at_raw = blocked_window.get("reset_at")
            if reset_at_raw:
                try:
                    reset_at = datetime.fromisoformat(reset_at_raw)
                except ValueError:
                    reset_at = None
                if reset_at and (earliest_reset_at is None or reset_at < earliest_reset_at):
                    earliest_reset_at = reset_at

        model_statuses.append(
            {
                "model_name": model_name,
                "is_available": is_available,
                "status_label": status_label,
                "blocked_windows": blocked_windows,
                "last_reset_at": max(
                    [window.reset_at for window in windows.values() if window.reset_at > now_utc],
                    default=None,
                ).isoformat() if windows else None,
                "last_reset_at_display": format_quota_timestamp(
                    max([window.reset_at for window in windows.values() if window.reset_at > now_utc], default=None) if windows else None
                ),
            }
        )

    preferred_model = ordered_models[0] if ordered_models else os.getenv("GEMINI_VISION_MODEL", "gemini-3-flash")
    active_model = available_models[0] if available_models else None
    hover_parts = [f"{status['model_name']}: {status['status_label']}" for status in model_statuses]
    if provider_order:
        hover_parts.insert(0, f"Providers: {' -> '.join(provider_order)}")
    if earliest_reset_at is not None:
        hover_parts.append(f"Earliest reset ({display_timezone}): {format_quota_timestamp(earliest_reset_at)}")

    return {
        "provider": primary_provider,
        "project_id": project_id,
        "display_timezone": display_timezone,
        "preferred_model": preferred_model,
        "active_model": active_model,
        "all_models_exhausted": not bool(available_models),
        "earliest_reset_at": earliest_reset_at.isoformat() if earliest_reset_at else None,
        "earliest_reset_at_display": format_quota_timestamp(earliest_reset_at),
        "model_statuses": model_statuses,
        "hover_text": " | ".join(hover_parts) if hover_parts else "No vision providers configured.",
    }


def get_diagram_analysis_usage_summary() -> dict:
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            return _build_diagram_analysis_usage_summary(cur)
    except Exception:
        logger.exception("Unable to load diagram analysis usage summary")
        return {
            "provider": get_primary_diagram_vision_provider(),
            "project_id": get_quota_project_id(),
            "display_timezone": str(get_quota_display_timezone()),
            "preferred_model": os.getenv("GEMINI_VISION_MODEL", "gemini-3-flash"),
            "active_model": None,
            "all_models_exhausted": False,
            "earliest_reset_at": None,
            "earliest_reset_at_display": None,
            "model_statuses": [],
            "hover_text": "Usage information is unavailable right now.",
        }
    finally:
        if conn is not None:
            conn.close()


# ===========================================================================
# 2. GENERAL UTILITIES
# ===========================================================================

def _allowed_upload_extensions_text() -> str:
    return ", ".join(sorted(ALLOWED_UPLOAD_EXTENSIONS))


def _sanitize_next_path(next_path: str) -> str:
    if not next_path:
        return "/dashboard"
    next_path = next_path.strip()
    if not next_path.startswith("/") or next_path.startswith("//"):
        return "/dashboard"
    return next_path


def _is_local_host(hostname: str | None) -> bool:
    host = (hostname or "").strip().lower()
    return host in {"localhost", "127.0.0.1", "::1"}


def _request_base_url() -> str:
    return request.url_root.rstrip("/")


def _preferred_external_base_url(configured_url: str) -> str:
    configured = (configured_url or "").strip()
    request_base = _request_base_url()
    request_host = urlparse(request_base).hostname

    if _is_local_host(request_host):
        configured_host = urlparse(configured).hostname if configured else ""
        if not configured or not _is_local_host(configured_host):
            return request_base

    return configured.rstrip("/") if configured else request_base


def build_external_url(path: str) -> str:
    app_base_url = _preferred_external_base_url(os.getenv("APP_BASE_URL", ""))
    return urljoin(app_base_url.rstrip("/") + "/", path.lstrip("/"))


def _compose_upload_conversation_title(file_names: list[str]) -> str:
    return "New conversation"


NEW_CONVERSATION_TITLE = "New conversation"
CONVERSATION_TITLE_MAX_LENGTH = 120
CONVERSATION_TITLE_STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "by", "for", "from", "how", "in", "into",
    "is", "of", "on", "or", "paper", "study", "the", "to", "using", "with",
}


def _normalize_conversation_title_candidate(value: str) -> str:
    normalized = re.sub(r"\s+", " ", str(value or "").replace("_", " ").replace("-", " ")).strip()
    return normalized[:CONVERSATION_TITLE_MAX_LENGTH]


def _title_from_filename(file_name: str) -> str:
    stem = Path(str(file_name or "").strip()).stem
    return _normalize_conversation_title_candidate(stem)


def _extract_document_title_candidate(metadata: dict | None, original_filename: str) -> str:
    if isinstance(metadata, dict):
        canonical = metadata.get("canonical") if isinstance(metadata.get("canonical"), dict) else {}
        for key in ("title", "document_title"):
            candidate = _normalize_conversation_title_candidate(canonical.get(key) or metadata.get(key) or "")
            if candidate:
                return candidate
    return _title_from_filename(original_filename)


def _derive_conversation_title_from_documents(documents: list[dict]) -> str:
    candidates: list[str] = []
    for payload in documents:
        candidate = _extract_document_title_candidate(
            payload.get("metadata") if isinstance(payload, dict) else None,
            str(payload.get("original_filename") or "") if isinstance(payload, dict) else "",
        )
        if candidate and candidate not in candidates:
            candidates.append(candidate)

    if not candidates:
        return NEW_CONVERSATION_TITLE
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) == 2:
        return _normalize_conversation_title_candidate(f"{candidates[0]} + {candidates[1]}")

    token_counts: dict[str, int] = {}
    token_labels: dict[str, str] = {}
    for candidate in candidates:
        seen_tokens: set[str] = set()
        for token in re.findall(r"[A-Za-z][A-Za-z0-9+.-]{2,}", candidate):
            normalized_token = token.lower()
            if normalized_token in CONVERSATION_TITLE_STOPWORDS:
                continue
            if normalized_token in seen_tokens:
                continue
            seen_tokens.add(normalized_token)
            token_counts[normalized_token] = token_counts.get(normalized_token, 0) + 1
            token_labels.setdefault(normalized_token, token)

    ranked_tokens = sorted(
        token_counts.items(),
        key=lambda item: (-item[1], -len(item[0]), item[0]),
    )
    topic_tokens = [token_labels[token] for token, count in ranked_tokens if count >= 2][:3]
    if topic_tokens:
        return _normalize_conversation_title_candidate(" / ".join(topic_tokens))

    return _normalize_conversation_title_candidate(f"{candidates[0]} + {len(candidates) - 1} more")


def _maybe_refresh_conversation_title(cur, conversation_id: str) -> str | None:
    if not conversation_id:
        return None

    cur.execute(
        """
        SELECT title
        FROM conversations
        WHERE conversation_id = %s
        """,
        (conversation_id,),
    )
    conversation_row = cur.fetchone()
    if not conversation_row:
        return None

    current_title = str(conversation_row[0] or "").strip()
    if current_title and current_title.lower() != NEW_CONVERSATION_TITLE.lower():
        return None

    cur.execute(
        """
        SELECT
            d.original_filename,
            de.parser_status,
            de.metadata
        FROM conversation_documents cd
        JOIN documents d ON d.document_id = cd.document_id
        LEFT JOIN document_extractions de ON de.document_id = cd.document_id
        WHERE cd.conversation_id = %s
          AND d.is_deleted = FALSE
        ORDER BY d.created_at ASC, d.document_id ASC
        """,
        (conversation_id,),
    )
    document_rows = cur.fetchall()
    if not document_rows:
        return None

    document_payloads: list[dict] = []
    for original_filename, parser_status, metadata in document_rows:
        normalized_status = str(parser_status or "pending").strip().lower()
        if normalized_status == "pending":
            return None
        document_payloads.append(
            {
                "original_filename": str(original_filename or ""),
                "parser_status": normalized_status,
                "metadata": metadata if isinstance(metadata, dict) else {},
            }
        )

    completed_payloads = [
        payload for payload in document_payloads
        if payload.get("parser_status") == "success"
    ]
    if not completed_payloads:
        return None

    next_title = _derive_conversation_title_from_documents(completed_payloads)
    if not next_title or next_title == NEW_CONVERSATION_TITLE:
        return None

    cur.execute(
        """
        UPDATE conversations
        SET title = %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE conversation_id = %s
          AND COALESCE(NULLIF(title, ''), %s) = %s
        """,
        (next_title, conversation_id, NEW_CONVERSATION_TITLE, NEW_CONVERSATION_TITLE),
    )
    return next_title if cur.rowcount else None


def _demo_chat_messages() -> list[dict]:
    return [
        {
            "role": "user",
            "text": "Can you summarize the main contributions of this paper for my presentation?",
        },
        {
            "role": "assistant",
            "text": "The paper contributes three main ideas: a structured document parsing pipeline, a normalization layer that converts extracted content into reusable blocks, and a grounded study workflow that turns parsed materials into chat, tables, and diagram-ready knowledge.",
        },
        {
            "role": "user",
            "text": "What should I mention if the examiner asks why asynchronous parsing is used?",
        },
        {
            "role": "assistant",
            "text": "You can explain that parsing large academic documents takes noticeable time, so the system stores uploads immediately, marks them as pending, and processes them in the background. That keeps the interface responsive while still exposing parser progress and results after completion.",
        },
        {
            "role": "user",
            "text": "Can you give me a short explanation of the MinerU ZIP output flow?",
        },
        {
            "role": "assistant",
            "text": "After upload, MinerU returns a ZIP containing structured artifacts such as content lists, model JSON, markdown, and extracted images. The app parses that ZIP, normalizes layout blocks, resolves references, extracts assets, and stores a consistent document representation for later retrieval.",
        },
    ]


def convert_docx_to_pdf(source_path: Path, output_path: Path) -> bool:
    """Try docx2pdf first (uses Word on Windows), then fall back to LibreOffice."""
    try:
        from docx2pdf import convert as docx2pdf_convert
        docx2pdf_convert(str(source_path), str(output_path))
        return output_path.exists()
    except Exception:
        pass

    try:
        subprocess.run(
            ["soffice", "--headless", "--convert-to", "pdf",
             "--outdir", str(output_path.parent), str(source_path)],
            check=True, capture_output=True,
        )
    except Exception:
        return False

    libreoffice_output = output_path.parent / f"{source_path.stem}.pdf"
    if libreoffice_output.exists() and libreoffice_output != output_path:
        libreoffice_output.replace(output_path)
    return output_path.exists()


def get_preview_pdf_path(file_path: Path) -> Path:
    return PREVIEW_DIR / f"{file_path.stem}.pdf"


def _resolve_authorized_upload_path(user_id: str | None, requested_path: str) -> Path | None:
    if not user_id:
        return None

    safe_requested_path = str(requested_path or "").strip().replace("\\", "/")
    if not safe_requested_path:
        return None

    requested_parts = Path(safe_requested_path).parts
    if (
        not requested_parts
        or requested_parts[0] != user_id
        or any(part in {"..", ""} for part in requested_parts)
    ):
        return None

    user_root = (UPLOADS_DIR / user_id).resolve()
    candidate = (UPLOADS_DIR / Path(*requested_parts)).resolve()

    try:
        candidate.relative_to(user_root)
    except ValueError:
        return None

    if not candidate.exists() or not candidate.is_file():
        return None

    return candidate


# ===========================================================================
# 3. SERIALIZERS  (DB row → dict)
# ===========================================================================

def serialize_user_row(user_row) -> dict | None:
    if not user_row:
        return None
    auth_provider = user_row[5] if len(user_row) > 5 else "local"
    google_sub = user_row[6] if len(user_row) > 6 else None
    has_password = bool(user_row[7]) if len(user_row) > 7 else auth_provider == "local"
    return {
        "user_id":        str(user_row[0]),
        "username":       user_row[1],
        "email":          user_row[2],
        "created_at":     user_row[3].isoformat() if user_row[3] else None,
        "email_verified": bool(user_row[4]),
        "auth_provider":  auth_provider,
        "is_google_linked": bool(google_sub) or auth_provider == "google",
        "has_password": has_password,
    }


def _serialize_dashboard_conversation(row) -> dict:
    updated_at = row[2]
    documents  = row[5] or []
    serialized_documents = [
        {
            "document_id":       str(doc.get("document_id") or ""),
            "original_filename": doc.get("original_filename") or "",
        }
        for doc in documents
        if isinstance(doc, dict)
    ]
    return {
        "id":            str(row[0]),
        "title":         (row[1] or "Untitled conversation").strip() or "Untitled conversation",
        "updated_at":    updated_at.isoformat() if updated_at else "",
        "formatted_date": updated_at.strftime("%b %d, %Y") if updated_at else "",
        "source_count":  int(row[3] or 0),
        "sources_joined": row[4] or "",
        "documents":     [d for d in serialized_documents if d["document_id"]],
    }


def _serialize_sidebar_conversation(row) -> dict:
    return {
        "id":         str(row[0]),
        "title":      (row[1] or "Untitled conversation").strip() or "Untitled conversation",
        "updated_at": row[2].isoformat() if row[2] else "",
    }


def _serialize_conversation_document(row) -> dict:
    raw_metadata = row[9] if len(row) > 9 else None
    metadata = raw_metadata if isinstance(raw_metadata, dict) else {}
    raw_progress = metadata.get("processing") if isinstance(metadata, dict) else None
    parser_progress = raw_progress if isinstance(raw_progress, dict) else None
    created_at_value = row[5]
    created_at = created_at_value.isoformat() if created_at_value else ""
    created_at_ts = int(created_at_value.timestamp() * 1000) if created_at_value else 0

    return {
        "document_id":       str(row[0]),
        "original_filename": row[1] or "",
        "stored_filename":   row[2] or "",
        "file_extension":    row[3] or "",
        "mime_type":         row[4] or "",
        "created_at":        created_at,
        "uploaded_at":       created_at,
        "created_at_ts":     created_at_ts,
        "uploaded_at_ts":    created_at_ts,
        "upload_path":       f"{row[6]}/{row[2]}" if row[6] and row[2] else "",
        "parser_status":     row[8] or "pending",
        "parser_progress":   parser_progress,
    }


def _serialize_conversation_message(row, version_count: int | None = None) -> dict:
    selected_document_ids = row[5] if isinstance(row[5], list) else []
    retrieval_payload = row[6] if isinstance(row[6], dict) else None
    citations = retrieval_payload.get("citations") if isinstance(retrieval_payload, dict) else []
    citations = citations if isinstance(citations, list) else []
    family_id = str(row[12]) if len(row) >= 13 and row[12] else str(row[10] or row[0])
    version_index = int(row[13] or 1) if len(row) >= 14 else 1
    return {
        "message_id":          str(row[0]),
        "conversation_id":     str(row[1]),
        "user_id":             str(row[2]),
        "role":                row[3] or "",
        "message_text":        row[4] or "",
        "selected_document_ids": [str(item) for item in selected_document_ids],
        "retrieval_payload":   retrieval_payload,
        "citations":           citations,
        "confidence":          ((retrieval_payload or {}).get("grounded_answer") or {}).get("confidence", ""),
        "model_provider":      row[7] or "",
        "model_name":          row[8] or "",
        "prompt_version":      row[9] or "",
        "reply_to_message_id": str(row[10]) if row[10] else None,
        "created_at":          row[11].isoformat() if row[11] else "",
        "family_id":           family_id,
        "version_index":       version_index,
        "version_count":       int(version_count or version_index or 1),
        "branch_parent_message_id": str(row[14]) if len(row) >= 15 and row[14] else None,
    }


def _conversation_messages_support_versioning(cur) -> bool:
    if not _relation_exists(cur, "conversation_messages"):
        return False
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


def _conversation_rows_support_versioning(rows: list) -> bool:
    return bool(rows) and len(rows[0]) >= 16


def _get_family_version_counts(rows: list) -> dict[tuple[str, str], int]:
    counts: dict[tuple[str, str], set[int]] = {}
    if not _conversation_rows_support_versioning(rows):
        return counts
    for row in rows:
        family_id = str(row[12] or "").strip()
        role = str(row[3] or "").strip().lower()
        if not family_id:
            continue
        counts.setdefault((family_id, role), set()).add(int(row[13] or 1))
    return {family_key: len(versions) for family_key, versions in counts.items()}


def _resolve_active_conversation_branch_rows(rows: list) -> list:
    if not _conversation_rows_support_versioning(rows):
        return rows

    user_rows_by_parent: dict[str | None, list] = {}
    assistant_rows_by_user: dict[str, list] = {}
    prelude_assistant_rows: list = []
    for row in rows:
        role = str(row[3] or "").strip().lower()
        if role == "user":
            parent_id = str(row[14]) if row[14] else None
            user_rows_by_parent.setdefault(parent_id, []).append(row)
        elif role == "assistant" and row[10]:
            assistant_rows_by_user.setdefault(str(row[10]), []).append(row)
        elif role == "assistant" and not row[10]:
            prelude_assistant_rows.append(row)

    active_rows: list = sorted(
        prelude_assistant_rows,
        key=lambda item: (str(item[11] or ""), str(item[0] or "")),
    )
    visited_user_ids: set[str] = set()
    parent_assistant_id: str | None = None

    while True:
        candidates = [
            row for row in user_rows_by_parent.get(parent_assistant_id, [])
            if bool(row[15])
        ]
        if not candidates:
            break
        user_row = sorted(
            candidates,
            key=lambda item: (int(item[13] or 1), str(item[11] or ""), str(item[0] or "")),
        )[-1]
        user_message_id = str(user_row[0] or "")
        if not user_message_id or user_message_id in visited_user_ids:
            break
        visited_user_ids.add(user_message_id)
        active_rows.append(user_row)

        assistant_candidates = assistant_rows_by_user.get(user_message_id, [])
        if not assistant_candidates:
            break
        active_assistant_candidates = [
            row for row in assistant_candidates if bool(row[15])
        ]
        assistant_row = sorted(
            active_assistant_candidates or assistant_candidates,
            key=lambda item: (int(item[13] or 1), str(item[11] or ""), str(item[0] or "")),
        )[-1]
        active_rows.append(assistant_row)
        parent_assistant_id = str(assistant_row[0] or "") or None

    return active_rows


# ===========================================================================
# 4. DATA ACCESS LAYER  (DB queries, no HTTP concerns)
# ===========================================================================

def get_current_user() -> dict | None:
    user_id = session.get("user_id")
    if not user_id:
        return None

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT user_id, username, email, created_at, email_verified, auth_provider, google_sub, password_hash
                FROM users
                WHERE user_id = %s
                """,
                (user_id,),
            )
            user_row = cur.fetchone()
        if not user_row:
            session.pop("user_id", None)
            return None
        return serialize_user_row(user_row)
    except Exception:
        return None
    finally:
        if conn is not None:
            conn.close()


def get_sidebar_conversations(user_id, limit: int = 8) -> list:
    if not user_id:
        return []

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT conversation_id, title, updated_at
                FROM conversations
                WHERE user_id = %s
                ORDER BY updated_at DESC
                LIMIT %s
                """,
                (user_id, limit),
            )
            rows = cur.fetchall()
        return [_serialize_sidebar_conversation(row) for row in rows]
    except Exception:
        return []
    finally:
        if conn is not None:
            conn.close()


def _build_diagram_analysis_failure_payload(exc: Exception) -> tuple[dict, int]:
    message = str(exc or "").strip()
    normalized = message.lower()
    if "no compatible model is currently available" in normalized:
        return ({
            "error": "All compatible Gemini vision models are temporarily unavailable. Try again later, or use Copy Selected Image and Copy AI Prompt if you need an immediate external fallback.",
            "error_type": "vision_models_exhausted",
            "fallback_action": "copy_image_and_prompt",
            "usage": get_diagram_analysis_usage_summary(),
        }, 429)
    if "503" in normalized and ("high demand" in normalized or "unavailable" in normalized):
        return ({
            "error": "Gemini is currently under high demand. Try again later, or use Copy Selected Image and Copy AI Prompt to ask an external AI yourself.",
            "error_type": "provider_high_demand",
            "fallback_action": "copy_image_and_prompt",
            "usage": get_diagram_analysis_usage_summary(),
        }, 503)
    return ({
        "error": message or "Unable to analyze selected diagrams right now.",
        "error_type": "analysis_failed",
        "usage": get_diagram_analysis_usage_summary(),
    }, 500)


def _load_latest_diagram_analysis_error(cur, block_ids: list[str]) -> str:
    normalized_block_ids = [str(block_id).strip() for block_id in (block_ids or []) if str(block_id).strip()]
    if not normalized_block_ids or not _relation_exists(cur, "diagram_block_analysis_runs"):
        return ""
    cur.execute(
        """
        SELECT status, error_message
        FROM diagram_block_analysis_runs
        WHERE block_id = ANY(%s::uuid[])
        ORDER BY started_at DESC
        LIMIT 1
        """,
        (normalized_block_ids,),
    )
    row = cur.fetchone()
    if not row:
        return ""
    status = str(row[0] or "").strip().lower()
    error_message = str(row[1] or "").strip()
    if status != "failed" or not error_message:
        return ""
    return error_message


def get_conversation_title(user_id, conversation_id) -> str:
    if not user_id or not conversation_id:
        return ""

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT title
                FROM conversations
                WHERE user_id = %s
                  AND conversation_id = %s
                LIMIT 1
                """,
                (user_id, conversation_id),
            )
            row = cur.fetchone()
        return (row[0] or "").strip() if row else ""
    except Exception:
        return ""
    finally:
        if conn is not None:
            conn.close()


def get_conversation_documents(user_id, conversation_id) -> list:
    if not user_id or not conversation_id:
        return []

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    d.document_id,
                    d.original_filename,
                    d.stored_filename,
                    d.file_extension,
                    d.mime_type,
                    d.created_at,
                    d.user_id,
                    d.storage_path,
                    de.parser_status,
                    de.metadata
                FROM conversations c
                JOIN conversation_documents cd ON cd.conversation_id = c.conversation_id
                JOIN documents d              ON d.document_id       = cd.document_id
                LEFT JOIN document_extractions de ON de.document_id = d.document_id
                WHERE c.conversation_id = %s
                  AND c.user_id         = %s
                  AND d.is_deleted      = FALSE
                ORDER BY cd.added_at DESC, d.created_at DESC
                """,
                (conversation_id, user_id),
            )
            rows = cur.fetchall()
        return [_serialize_conversation_document(row) for row in rows]
    except Exception:
        return []
    finally:
        if conn is not None:
            conn.close()


def get_conversation_document_record(user_id, conversation_id, document_id) -> dict | None:
    if not user_id or not conversation_id or not document_id:
        return None

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    d.document_id,
                    d.original_filename,
                    d.stored_filename,
                    d.file_extension,
                    d.mime_type,
                    d.created_at,
                    d.user_id,
                    d.storage_path,
                    de.parser_status,
                    de.metadata
                FROM conversations c
                JOIN conversation_documents cd ON cd.conversation_id = c.conversation_id
                JOIN documents d              ON d.document_id       = cd.document_id
                LEFT JOIN document_extractions de ON de.document_id = d.document_id
                WHERE c.conversation_id = %s
                  AND c.user_id         = %s
                  AND d.document_id     = %s
                  AND d.is_deleted      = FALSE
                LIMIT 1
                """,
                (conversation_id, user_id, document_id),
            )
            row = cur.fetchone()
            return _serialize_conversation_document(row) if row else None
    except Exception:
        return None
    finally:
        if conn is not None:
            conn.close()


def get_document_summary(user_id, document_id, conversation_id=None) -> dict | None:
    if not user_id or not document_id:
        return None

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            if conversation_id:
                cur.execute(
                    """
                    SELECT
                        ds.document_id::text,
                        ds.conversation_id::text,
                        ds.status,
                        ds.summary_text,
                        ds.title_hint,
                        ds.summary_payload,
                        ds.provider_name,
                        ds.model_name,
                        ds.token_count,
                        ds.error_message,
                        ds.completed_at
                    FROM document_summaries ds
                    JOIN conversation_documents cd
                      ON cd.document_id = ds.document_id
                    JOIN conversations c
                      ON c.conversation_id = cd.conversation_id
                    JOIN documents d
                      ON d.document_id = ds.document_id
                    WHERE ds.document_id = %s::uuid
                      AND cd.conversation_id = %s::uuid
                      AND c.user_id = %s
                      AND d.is_deleted = FALSE
                    LIMIT 1
                    """,
                    (document_id, conversation_id, user_id),
                )
            else:
                cur.execute(
                    """
                    SELECT
                        ds.document_id::text,
                        ds.conversation_id::text,
                        ds.status,
                        ds.summary_text,
                        ds.title_hint,
                        ds.summary_payload,
                        ds.provider_name,
                        ds.model_name,
                        ds.token_count,
                        ds.error_message,
                        ds.completed_at
                    FROM document_summaries ds
                    JOIN documents d
                      ON d.document_id = ds.document_id
                    WHERE ds.document_id = %s::uuid
                      AND d.user_id = %s
                      AND d.is_deleted = FALSE
                    LIMIT 1
                    """,
                    (document_id, user_id),
                )
            row = cur.fetchone()
        if not row:
            return None
        return {
            "document_id": str(row[0] or ""),
            "conversation_id": str(row[1] or "") if row[1] else None,
            "status": str(row[2] or ""),
            "summary_text": row[3] or "",
            "title_hint": row[4] or "",
            "summary_payload": row[5] or {},
            "provider_name": row[6] or "",
            "model_name": row[7] or "",
            "token_count": int(row[8] or 0),
            "error_message": row[9] or "",
            "completed_at": row[10].isoformat() if row[10] else None,
        }
    except Exception:
        return None
    finally:
        if conn is not None:
            conn.close()


def get_conversation_summary(user_id, conversation_id) -> dict | None:
    if not user_id or not conversation_id:
        return None

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    cs.conversation_id::text,
                    cs.status,
                    cs.document_count,
                    cs.summary_text,
                    cs.generated_title,
                    cs.summary_payload,
                    cs.provider_name,
                    cs.model_name,
                    cs.token_count,
                    cs.error_message,
                    cs.completed_at
                FROM conversation_summaries cs
                JOIN conversations c
                  ON c.conversation_id = cs.conversation_id
                WHERE cs.conversation_id = %s::uuid
                  AND c.user_id = %s
                LIMIT 1
                """,
                (conversation_id, user_id),
            )
            row = cur.fetchone()
        if not row:
            return None
        return {
            "conversation_id": str(row[0] or ""),
            "status": str(row[1] or ""),
            "document_count": int(row[2] or 0),
            "summary_text": row[3] or "",
            "generated_title": row[4] or "",
            "summary_payload": row[5] or {},
            "provider_name": row[6] or "",
            "model_name": row[7] or "",
            "token_count": int(row[8] or 0),
            "error_message": row[9] or "",
            "completed_at": row[10].isoformat() if row[10] else None,
        }
    except Exception:
        return None
    finally:
        if conn is not None:
            conn.close()


def conversation_exists_for_user(user_id, conversation_id) -> bool:
    if not user_id or not conversation_id:
        return False

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM conversations
                WHERE conversation_id = %s
                  AND user_id = %s
                LIMIT 1
                """,
                (conversation_id, user_id),
            )
            return bool(cur.fetchone())
    except Exception:
        return False
    finally:
        if conn is not None:
            conn.close()


def get_conversation_messages(user_id, conversation_id) -> list:
    if not user_id or not conversation_id:
        return []

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            if not _relation_exists(cur, "conversation_messages"):
                return []

            if _conversation_messages_support_versioning(cur):
                cur.execute(
                    """
                    SELECT
                        cm.message_id,
                        cm.conversation_id,
                        cm.user_id,
                        cm.role,
                        cm.message_text,
                        cm.selected_document_ids,
                        cm.retrieval_payload,
                        cm.model_provider,
                        cm.model_name,
                        cm.prompt_version,
                        cm.reply_to_message_id,
                        cm.created_at,
                        cm.family_id,
                        cm.family_version_number,
                        cm.branch_parent_message_id,
                        cm.is_active_in_family
                    FROM conversation_messages cm
                    JOIN conversations c ON c.conversation_id = cm.conversation_id
                    WHERE cm.conversation_id = %s
                      AND c.user_id = %s
                    ORDER BY
                        cm.created_at ASC,
                        cm.family_id ASC NULLS LAST,
                        cm.family_version_number ASC NULLS LAST,
                        CASE WHEN cm.role = 'user' THEN 0 ELSE 1 END ASC,
                        cm.message_id ASC
                    """,
                    (conversation_id, user_id),
                )
            else:
                cur.execute(
                    """
                    SELECT
                        cm.message_id,
                        cm.conversation_id,
                        cm.user_id,
                        cm.role,
                        cm.message_text,
                        cm.selected_document_ids,
                        cm.retrieval_payload,
                        cm.model_provider,
                        cm.model_name,
                        cm.prompt_version,
                        cm.reply_to_message_id,
                        cm.created_at
                    FROM conversation_messages cm
                    JOIN conversations c ON c.conversation_id = cm.conversation_id
                    WHERE cm.conversation_id = %s
                      AND c.user_id = %s
                    ORDER BY
                        cm.created_at ASC,
                        COALESCE(cm.reply_to_message_id, cm.message_id) ASC,
                        CASE WHEN cm.role = 'user' THEN 0 ELSE 1 END ASC,
                        cm.message_id ASC
                    """,
                    (conversation_id, user_id),
                )
            rows = cur.fetchall()
        active_rows = _resolve_active_conversation_branch_rows(rows)
        version_counts = _get_family_version_counts(rows)
        return [
            _serialize_conversation_message(
                row,
                version_count=version_counts.get(
                    (str(row[12] or ""), str(row[3] or "").strip().lower()),
                    1,
                ) if len(row) >= 13 else 1,
            )
            for row in active_rows
        ]
    except Exception:
        return []
    finally:
        if conn is not None:
            conn.close()


def get_document_parser_result(user_id, document_id, conversation_id=None) -> dict | None:
    if not user_id or not document_id:
        return None

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            if conversation_id:
                cur.execute(
                    """
                    SELECT d.document_id, d.original_filename, d.file_extension, d.mime_type
                    FROM conversations c
                    JOIN conversation_documents cd ON cd.conversation_id = c.conversation_id
                    JOIN documents d              ON d.document_id       = cd.document_id
                    WHERE c.user_id         = %s
                      AND c.conversation_id = %s
                      AND d.document_id     = %s
                      AND d.is_deleted      = FALSE
                    LIMIT 1
                    """,
                    (user_id, conversation_id, document_id),
                )
            else:
                cur.execute(
                    """
                    SELECT document_id, original_filename, file_extension, mime_type
                    FROM documents
                    WHERE user_id    = %s
                      AND document_id = %s
                      AND is_deleted  = FALSE
                    LIMIT 1
                    """,
                    (user_id, document_id),
                )

            document_row = cur.fetchone()
            if not document_row:
                return None

            document_file_record = get_document_file_record(user_id, document_id, conversation_id)
            extraction_payload = (
                fetch_document_extraction(cur, document_id=document_id, conversation_id=conversation_id)
                or build_pending_extraction_payload(document_id=document_id)
            )

            return {
                "document_id":       str(document_row[0]),
                "original_filename": document_row[1] or "",
                "file_extension":    document_row[2] or "",
                "mime_type":         document_row[3] or "",
                "upload_path":       (document_file_record or {}).get("upload_path", ""),
                "parser_result":     extraction_payload,
            }
    except Exception:
        return None
    finally:
        if conn is not None:
            conn.close()


def get_document_parser_result_context(user_id, document_id, conversation_id=None) -> dict:
    context = {
        "conversation_title": "",
        "document_name": "",
    }
    if not user_id:
        return context

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            if conversation_id:
                cur.execute(
                    """
                    SELECT title
                    FROM conversations
                    WHERE user_id = %s
                      AND conversation_id = %s
                    LIMIT 1
                    """,
                    (user_id, conversation_id),
                )
                row = cur.fetchone()
                if row:
                    context["conversation_title"] = row[0] or ""

                cur.execute(
                    """
                    SELECT d.original_filename
                    FROM conversations c
                    JOIN conversation_documents cd ON cd.conversation_id = c.conversation_id
                    JOIN documents d              ON d.document_id       = cd.document_id
                    WHERE c.user_id         = %s
                      AND c.conversation_id = %s
                      AND d.document_id     = %s
                      AND d.is_deleted      = FALSE
                    LIMIT 1
                    """,
                    (user_id, conversation_id, document_id),
                )
            else:
                cur.execute(
                    """
                    SELECT original_filename
                    FROM documents
                    WHERE user_id = %s
                      AND document_id = %s
                      AND is_deleted = FALSE
                    LIMIT 1
                    """,
                    (user_id, document_id),
                )

            row = cur.fetchone()
            if row:
                context["document_name"] = row[0] or ""
    except Exception:
        return context
    finally:
        if conn is not None:
            conn.close()

    return context


def _coerce_int(value, default=None):
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_review_text(value) -> str:
    text = str(value or "").replace("\r\n", "\n").replace("\r", "\n")
    lines = [line.rstrip() for line in text.split("\n")]
    return "\n".join(lines).strip()


def _normalize_inline_text(value) -> str:
    return " ".join(str(value or "").split()).strip()


DIAGRAM_REVIEW_DESCRIPTION_MAX_CHARS = 2400
DIAGRAM_REVIEW_FACT_MAX_ITEMS = 12
DIAGRAM_REVIEW_FACT_MAX_CHARS = 280


def _trim_review_text_to_max(value, max_chars: int) -> str:
    normalized = _normalize_review_text(value)
    if not normalized or len(normalized) <= max_chars:
        return normalized
    clipped = normalized[:max_chars].strip()
    last_boundary = max(clipped.rfind(" "), clipped.rfind("\n"))
    if last_boundary > int(max_chars * 0.6):
        clipped = clipped[:last_boundary]
    return clipped.strip()


def _extract_manual_diagram_json(value) -> dict | None:
    raw_text = str(value or "").strip()
    if not raw_text.startswith("{"):
        return None
    try:
        parsed = json.loads(raw_text)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(parsed, dict):
        return None
    if not any(key in parsed for key in ("visual_description", "question_answerable_facts", "summary")):
        return None
    return parsed


def _normalize_diagram_fact_list(values) -> list[str]:
    if isinstance(values, list):
        source_items = values
    elif isinstance(values, str) and str(values).strip():
        source_items = [values]
    else:
        source_items = []

    facts: list[str] = []
    seen: set[str] = set()
    for item in source_items:
        fact = _trim_review_text_to_max(item, DIAGRAM_REVIEW_FACT_MAX_CHARS)
        if not fact:
            continue
        dedupe_key = fact.casefold()
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        facts.append(fact)
        if len(facts) >= DIAGRAM_REVIEW_FACT_MAX_ITEMS:
            break
    return facts


def _normalize_diagram_review_fields(visual_description, question_answerable_facts) -> dict:
    parsed_payload = _extract_manual_diagram_json(visual_description)
    description_source = visual_description
    facts_source = question_answerable_facts

    if parsed_payload:
        description_source = (
            parsed_payload.get("visual_description")
            or parsed_payload.get("summary")
            or visual_description
        )
        if not facts_source:
            facts_source = (
                parsed_payload.get("question_answerable_facts")
                or parsed_payload.get("facts")
                or []
            )

    return {
        "visual_description": _trim_review_text_to_max(description_source, DIAGRAM_REVIEW_DESCRIPTION_MAX_CHARS) or None,
        "question_answerable_facts": _normalize_diagram_fact_list(facts_source),
    }


def _review_block_sort_key(block: dict):
    block_type_rank = {"text": 0, "table": 1, "diagram": 2}
    return (
        _coerce_int(block.get("source_unit_index"), 0),
        _coerce_int(block.get("reading_order"), 10**6),
        block_type_rank.get(str(block.get("block_type") or "").lower(), 9),
        str(block.get("block_id") or ""),
    )


def _normalize_matrix(matrix) -> list[list[str]]:
    rows = []
    if not isinstance(matrix, list):
        return rows
    max_cols = 0
    for row in matrix:
        if isinstance(row, list):
            normalized_row = [str(cell or "").strip() for cell in row]
        else:
            normalized_row = [str(row or "").strip()]
        max_cols = max(max_cols, len(normalized_row))
        rows.append(normalized_row)

    if max_cols == 0:
        return []

    normalized_rows = []
    for row in rows:
        padded = row + [""] * (max_cols - len(row))
        normalized_rows.append(padded)
    return normalized_rows


def _split_header_and_body_rows(matrix: list[list[str]]) -> tuple[list[list[str]], list[list[str]]]:
    if not matrix:
        return [], []
    if len(matrix) == 1:
        return [matrix[0]], []
    return [matrix[0]], matrix[1:]


def _build_table_cells(matrix: list[list[str]]) -> list[dict]:
    cells = []
    for row_index, row in enumerate(matrix):
        for col_index, text in enumerate(row):
            cells.append({
                "row_index": row_index,
                "col_index": col_index,
                "row_span": 1,
                "col_span": 1,
                "text": text,
                "is_header": row_index == 0,
                "bbox": None,
            })
    return cells


def _build_table_row_objects(header_rows: list[list[str]], body_rows: list[list[str]]) -> list[dict]:
    headers = header_rows[0][:] if header_rows else []
    for index in range(len(headers)):
        if not headers[index]:
            headers[index] = headers[index - 1] if index > 0 and headers[index - 1] else f"column_{index + 1}"

    if not headers:
        return [{"row_index": index, "values": row} for index, row in enumerate(body_rows, start=1)]

    row_objects = []
    for row_index, row in enumerate(body_rows, start=1):
        values = {}
        for col_index, value in enumerate(row):
            key = headers[col_index] if col_index < len(headers) else f"column_{col_index + 1}"
            values[key] = value
        row_objects.append({
            "row_index": row_index,
            "values": values,
        })
    return row_objects


def _linearize_review_table(
    *,
    title: str | None,
    caption: str | None,
    header_rows: list[list[str]],
    body_rows: list[list[str]],
    footnotes: list[str],
    context_lines: list[str],
) -> str:
    parts = []
    if title:
        parts.append(f"Table: {title}.")
    if caption and _normalize_inline_text(caption).lower() != _normalize_inline_text(title).lower():
        parts.append(f"Table: {caption}.")
    if header_rows:
        parts.append(f"Headers: {' | '.join(header_rows[0])}.")
    if body_rows:
        headers = header_rows[0] if header_rows else []
        for row_index, row in enumerate(body_rows, start=1):
            if headers:
                pairs = []
                for col_index, value in enumerate(row):
                    header = headers[col_index] if col_index < len(headers) else f"Column {col_index + 1}"
                    pairs.append(f"{header}={value}")
                parts.append(f"Row {row_index}: {'; '.join(pairs)}.")
            else:
                parts.append(f"Row {row_index}: {' | '.join(row)}.")
    for footnote in footnotes:
        footnote_text = _normalize_inline_text(footnote)
        if footnote_text:
            parts.append(f"Footnote: {footnote_text}.")
    if context_lines:
        context_summary = " ".join(line for line in context_lines if line)
        if context_summary:
            parts.append(f"Context: {context_summary}")
    return " ".join(parts).strip()


def _build_context_lines(block: dict, blocks_by_id: dict[str, dict]) -> list[str]:
    linked_context = block.get("linked_context") or {}
    candidate_ids = linked_context.get("explainer_block_ids") or linked_context.get("nearby_block_ids") or []
    lines = []
    for block_id in candidate_ids:
        candidate = blocks_by_id.get(str(block_id))
        if not candidate:
            continue
        candidate_text = ""
        candidate_type = str(candidate.get("block_type") or "").lower()
        normalized = candidate.get("normalized_content") or {}
        if candidate_type == "text":
            candidate_text = normalized.get("normalized_text") or normalized.get("text_content") or candidate.get("display_text") or ""
        elif candidate_type == "table":
            candidate_text = normalized.get("linearized_text") or normalized.get("retrieval_text") or candidate.get("display_text") or ""
        elif candidate_type == "diagram":
            candidate_text = normalized.get("visual_description") or candidate.get("caption_text") or candidate.get("display_text") or ""
        candidate_text = _normalize_inline_text(candidate_text)
        if candidate_text:
            lines.append(candidate_text)
    return lines[:4]


def _build_text_retrieval_text(block: dict) -> str:
    normalized = block.get("normalized_content") or {}
    section_path = normalized.get("section_path") or []
    parts = []
    if section_path:
        parts.append(f"Heading Path: {' > '.join(str(item) for item in section_path if item)}.")
    text_role = _normalize_inline_text(normalized.get("text_role"))
    if text_role:
        parts.append(f"Text Role: {text_role}.")
    text_value = normalized.get("normalized_text") or normalized.get("text_content") or block.get("display_text") or ""
    text_value = _normalize_review_text(text_value)
    if text_value:
        parts.append(text_value)
    return " ".join(part for part in parts if part).strip()


def _build_diagram_retrieval_text(block: dict, blocks_by_id: dict[str, dict], diagram_detail: dict | None) -> str:
    normalized = block.get("normalized_content") or {}
    lines = []
    caption_text = _normalize_inline_text(block.get("caption_text") or normalized.get("caption_text") or block.get("display_text"))
    visual_description = _normalize_review_text(
        (diagram_detail or {}).get("visual_description")
        or normalized.get("visual_description")
        or ""
    )
    ocr_values = (diagram_detail or {}).get("ocr_text") or normalized.get("ocr_text") or []
    if isinstance(ocr_values, str):
        ocr_values = [ocr_values]
    if caption_text:
        lines.append(f"Caption: {caption_text}.")
    if visual_description:
        lines.append(f"Description: {_normalize_inline_text(visual_description)}.")
    ocr_summary = " ".join(_normalize_inline_text(item) for item in ocr_values if _normalize_inline_text(item))
    if ocr_summary:
        lines.append(f"OCR: {ocr_summary}.")
    context_lines = _build_context_lines(block, blocks_by_id)
    if context_lines:
        lines.append(f"Nearby Context: {' '.join(context_lines)}")
    return " ".join(line for line in lines if line).strip()


def _refresh_review_block_content(blocks_by_id: dict[str, dict], diagram_details_by_block: dict[str, dict]) -> None:
    for block in blocks_by_id.values():
        block_type = str(block.get("block_type") or "").lower()
        normalized = dict(block.get("normalized_content") or {})
        if block_type == "text":
            text_value = _normalize_review_text(normalized.get("text_content") or normalized.get("normalized_text") or block.get("display_text") or "")
            normalized["text_content"] = text_value
            normalized["normalized_text"] = text_value
            block["display_text"] = text_value
            retrieval_text = _build_text_retrieval_text({**block, "normalized_content": normalized})
        elif block_type == "table":
            matrix = _normalize_matrix(normalized.get("matrix") or [])
            header_rows, body_rows = _split_header_and_body_rows(matrix)
            footnotes = [str(item or "").strip() for item in (normalized.get("footnotes") or []) if str(item or "").strip()]
            title = _normalize_inline_text(normalized.get("title"))
            caption = _normalize_review_text(block.get("caption_text") or normalized.get("caption") or title)
            normalized["title"] = title or None
            normalized["caption"] = caption or None
            normalized["matrix"] = matrix
            normalized["header_rows"] = header_rows
            normalized["body_rows"] = body_rows
            normalized["cells"] = _build_table_cells(matrix)
            normalized["row_objects"] = _build_table_row_objects(header_rows, body_rows)
            retrieval_text = _linearize_review_table(
                title=title or None,
                caption=caption or None,
                header_rows=header_rows,
                body_rows=body_rows,
                footnotes=footnotes,
                context_lines=_build_context_lines(block, blocks_by_id),
            )
            normalized["linearized_text"] = retrieval_text
            block["display_text"] = caption or retrieval_text
            block["caption_text"] = caption or None
        elif block_type == "diagram":
            detail = diagram_details_by_block.get(str(block.get("block_id")))
            normalized_diagram_fields = _normalize_diagram_review_fields(
                (detail or {}).get("visual_description")
                or normalized.get("visual_description")
                or "",
                (detail or {}).get("question_answerable_facts")
                or normalized.get("question_answerable_facts")
                or [],
            )
            visual_description = normalized_diagram_fields["visual_description"]
            normalized["visual_description"] = visual_description
            normalized["question_answerable_facts"] = normalized_diagram_fields["question_answerable_facts"]
            if detail is not None:
                detail["visual_description"] = visual_description
                detail["question_answerable_facts"] = normalized_diagram_fields["question_answerable_facts"]
            retrieval_text = _build_diagram_retrieval_text({**block, "normalized_content": normalized}, blocks_by_id, detail)
            caption_text = _normalize_review_text(block.get("caption_text") or normalized.get("caption_text") or block.get("display_text") or "")
            block["caption_text"] = caption_text or None
            if caption_text:
                block["display_text"] = caption_text
        else:
            retrieval_text = _normalize_review_text(normalized.get("retrieval_text") or block.get("display_text") or "")

        normalized["retrieval_text"] = retrieval_text
        block["normalized_content"] = normalized
        block["embedding_status"] = "ready" if retrieval_text else "not_ready"
        block["processing_status"] = "retrieval_prepared" if retrieval_text else block.get("processing_status")


def _get_review_impacted_block_ids(blocks_by_id: dict[str, dict], changed_block_ids: set[str]) -> set[str]:
    impacted_block_ids = {str(block_id) for block_id in changed_block_ids if str(block_id)}
    if not impacted_block_ids:
        return impacted_block_ids

    for block_id, block in blocks_by_id.items():
        candidate_ids = _get_context_candidate_ids(block)
        if any(candidate_id in impacted_block_ids for candidate_id in candidate_ids):
            impacted_block_ids.add(str(block_id))

    return impacted_block_ids


def _get_context_candidate_ids(block: dict) -> list[str]:
    linked_context = block.get("linked_context") or {}
    candidate_ids = linked_context.get("explainer_block_ids") or linked_context.get("nearby_block_ids") or []
    return [str(block_id) for block_id in candidate_ids if str(block_id)]


def _build_review_persist_snapshot(block: dict, diagram_detail: dict | None = None) -> dict:
    normalized = deepcopy(block.get("normalized_content") or {})
    source_metadata = deepcopy(block.get("source_metadata") or {})
    snapshot = {
        "subtype": block.get("subtype"),
        "normalized_content": normalized,
        "display_text": block.get("display_text"),
        "caption_text": block.get("caption_text"),
        "source_metadata": source_metadata,
        "embedding_status": block.get("embedding_status"),
        "processing_status": block.get("processing_status"),
    }
    if diagram_detail is not None:
        snapshot["diagram_detail"] = deepcopy(diagram_detail)
    return snapshot


def _matrix_to_markdown(matrix: list[list[str]]) -> str:
    normalized_rows = _normalize_matrix(matrix)
    if not normalized_rows:
        return ""
    width = len(normalized_rows[0])
    header = normalized_rows[0]
    divider = ["---"] * width
    lines = [
        f"| {' | '.join(header)} |",
        f"| {' | '.join(divider)} |",
    ]
    for row in normalized_rows[1:]:
        lines.append(f"| {' | '.join(row)} |")
    return "\n".join(lines)


def _build_review_markdown(blocks: list[dict], diagram_details_by_block: dict[str, dict]) -> str:
    parts = []
    for block in sorted(blocks, key=_review_block_sort_key):
        block_type = str(block.get("block_type") or "").lower()
        normalized = block.get("normalized_content") or {}
        if block_type == "text":
            text_value = _normalize_review_text(normalized.get("text_content") or normalized.get("normalized_text") or block.get("display_text") or "")
            if text_value:
                parts.append(text_value)
        elif block_type == "table":
            caption = _normalize_review_text(block.get("caption_text") or normalized.get("caption") or "")
            table_md = _matrix_to_markdown(normalized.get("matrix") or [])
            if caption:
                parts.append(f"### {caption}")
            if table_md:
                parts.append(table_md)
        elif block_type == "diagram":
            detail = diagram_details_by_block.get(str(block.get("block_id"))) or {}
            caption = _normalize_review_text(block.get("caption_text") or "")
            description = _normalize_review_text(detail.get("visual_description") or normalized.get("visual_description") or "")
            section_lines = []
            if caption:
                section_lines.append(f"### {caption}")
            if description:
                section_lines.append(f"Description: {description}")
            if section_lines:
                parts.append("\n".join(section_lines))
    return "\n\n".join(part for part in parts if part).strip()


def _collect_segment_ids_from_block(block: dict) -> list[str]:
    raw_content = block.get("raw_content") or {}
    segment_ids = []
    for key in ("segment", "primary_segment"):
        segment = raw_content.get(key) or {}
        segment_id = str(segment.get("segment_id") or "").strip()
        if segment_id:
            segment_ids.append(segment_id)
    for segment in raw_content.get("segments") or []:
        segment_id = str((segment or {}).get("segment_id") or "").strip()
        if segment_id:
            segment_ids.append(segment_id)
    return list(dict.fromkeys(segment_ids))


def _source_anchor_key_from_locator(source_file: str, source_locator: str) -> str:
    source_file = str(source_file or "").strip()
    source_locator = str(source_locator or "").strip()
    if not source_file or not source_locator:
        return ""
    match = re.search(r"page:(\d+):block:(\d+)", source_locator)
    if not match:
        return ""
    stem = Path(source_file).stem or source_file
    return f"{stem}:page:{match.group(1)}:block:{match.group(2)}"


def _coerce_rect_payload(rect: dict | list | tuple | None) -> dict:
    if isinstance(rect, dict):
        x0 = rect.get("x0")
        y0 = rect.get("y0")
        x1 = rect.get("x1")
        y1 = rect.get("y1")
        if all(value is not None for value in (x0, y0, x1, y1)):
            return {
                "x0": x0,
                "y0": y0,
                "x1": x1,
                "y1": y1,
                "coordinate_space": rect.get("coordinate_space") or "normalized_0_1",
                "page_width": rect.get("page_width"),
                "page_height": rect.get("page_height"),
                "origin": rect.get("origin") or "top_left",
            }
        return {}
    if isinstance(rect, (list, tuple)) and len(rect) == 4:
        return {
            "x0": rect[0],
            "y0": rect[1],
            "x1": rect[2],
            "y1": rect[3],
            "coordinate_space": "normalized_0_1",
            "page_width": None,
            "page_height": None,
            "origin": "top_left",
        }
    return {}


def _rects_union_bbox(rects: list[dict]) -> dict:
    valid_rects = []
    for rect in rects:
        payload = _coerce_rect_payload(rect)
        if payload:
            valid_rects.append(payload)
    if not valid_rects:
        return {}
    x0 = min(float(rect["x0"]) for rect in valid_rects)
    y0 = min(float(rect["y0"]) for rect in valid_rects)
    x1 = max(float(rect["x1"]) for rect in valid_rects)
    y1 = max(float(rect["y1"]) for rect in valid_rects)
    first = valid_rects[0]
    return {
        "x0": x0,
        "y0": y0,
        "x1": x1,
        "y1": y1,
        "coordinate_space": first.get("coordinate_space") or "normalized_0_1",
        "page_width": first.get("page_width"),
        "page_height": first.get("page_height"),
        "origin": first.get("origin") or "top_left",
    }


def _extract_block_source_anchor_key(block: dict) -> str:
    source_metadata = block.get("source_metadata") or {}
    for candidate in (
        source_metadata.get("source_anchor_key"),
        (source_metadata.get("segment_metadata") or {}).get("source_anchor_key"),
        (source_metadata.get("asset_metadata") or {}).get("source_anchor_key"),
    ):
        value = str(candidate or "").strip()
        if value:
            return value
    for metadata_key in ("segment_metadata", "asset_metadata"):
        metadata = source_metadata.get(metadata_key) or {}
        source_key = _source_anchor_key_from_locator(
            str(metadata.get("source_file") or ""),
            str(metadata.get("source_locator") or ""),
        )
        if source_key:
            return source_key
    return ""


def _build_preview_anchor_from_rects(page_index: int, rects: list[dict], source_anchor_key: str = "") -> dict:
    clean_rects = []
    for rect in rects:
        payload = _coerce_rect_payload(rect)
        if payload:
            clean_rects.append(payload)
    bbox = _rects_union_bbox(clean_rects)
    if not clean_rects or not bbox:
        return {}
    return {
        "page_index": page_index,
        "source_anchor_key": source_anchor_key,
        "rects": clean_rects,
        "bbox": bbox,
    }


def _fallback_preview_anchor(page_index: int, *bbox_candidates: dict | list | tuple | None, source_anchor_key: str = "") -> dict:
    for candidate in bbox_candidates:
        payload = _coerce_rect_payload(candidate)
        if payload:
            return _build_preview_anchor_from_rects(page_index, [payload], source_anchor_key=source_anchor_key)
    return {}


def _resolve_preview_anchor(block: dict, anchor_registry: dict[str, dict], *bbox_candidates: dict | list | tuple | None) -> dict:
    page_index = _coerce_int(block.get("source_unit_index"), 0)
    source_anchor_key = _extract_block_source_anchor_key(block)
    if source_anchor_key:
        entry = anchor_registry.get(source_anchor_key) or {}
        rects = entry.get("rects") or []
        if rects:
            return _build_preview_anchor_from_rects(
                _coerce_int(entry.get("page_index"), page_index) or page_index,
                rects,
                source_anchor_key=source_anchor_key,
            )
    return _fallback_preview_anchor(page_index, *bbox_candidates, source_anchor_key=source_anchor_key)


def _build_layout_preview_anchor_map(_parser_result: dict) -> dict:
    # Compatibility shim for any stale call sites during Flask reload.
    return {}


def _build_parser_review_payload(document_result: dict) -> dict:
    document_result = deepcopy(document_result or {})
    parser_result = document_result.get("parser_result") or {}
    blocks = sorted(parser_result.get("document_blocks") or [], key=_review_block_sort_key)
    block_assets = parser_result.get("block_assets") or []
    diagram_details = parser_result.get("diagram_block_details") or []
    metadata = parser_result.get("metadata") or {}
    anchor_registry = (metadata.get("mineru_anchor_registry") or {}) if isinstance(metadata, dict) else {}

    assets_by_block = {}
    for asset in block_assets:
        block_id = str(asset.get("block_id") or "")
        if not block_id:
            continue
        assets_by_block.setdefault(block_id, []).append(asset)

    diagram_by_block = {
        str(detail.get("block_id")): detail
        for detail in diagram_details
        if detail.get("block_id")
    }

    review_blocks = []
    counts = {"text": 0, "table": 0, "diagram": 0}
    for block in blocks:
        block_id = str(block.get("block_id") or "")
        block_type = str(block.get("block_type") or "").lower()
        normalized = block.get("normalized_content") or {}
        block_bbox = block.get("bbox") or {}
        source_location = block.get("source_location") or {}
        source_anchor_key = _extract_block_source_anchor_key(block)
        counts[block_type] = counts.get(block_type, 0) + 1

        item = {
            "block_id": block_id,
            "block_type": block_type,
            "subtype": block.get("subtype"),
            "source_unit_type": block.get("source_unit_type"),
            "source_unit_index": block.get("source_unit_index"),
            "reading_order": block.get("reading_order"),
            "confidence": block.get("confidence"),
            "updated_at": block.get("updated_at"),
            "display_text": block.get("display_text"),
            "caption_text": block.get("caption_text"),
            "embedding_status": block.get("embedding_status"),
            "processing_status": block.get("processing_status"),
            "linked_context": block.get("linked_context") or {},
            "source_anchor_key": source_anchor_key,
            "preview_anchor": _resolve_preview_anchor(
                block,
                anchor_registry,
                block_bbox,
                source_location.get("bbox"),
            ),
            "normalized_content": {},
        }

        if block_type == "text":
            item["normalized_content"] = {
                "text_role": normalized.get("text_role") or "paragraph",
                "text_content": normalized.get("text_content") or normalized.get("normalized_text") or block.get("display_text") or "",
                "normalized_text": normalized.get("normalized_text") or normalized.get("text_content") or block.get("display_text") or "",
                "section_path": normalized.get("section_path") or [],
                "retrieval_text": normalized.get("retrieval_text") or "",
            }
        elif block_type == "table":
            item["normalized_content"] = {
                "title": normalized.get("title"),
                "caption": normalized.get("caption") or block.get("caption_text") or "",
                "matrix": _normalize_matrix(normalized.get("matrix") or []),
                "header_rows": normalized.get("header_rows") or [],
                "body_rows": normalized.get("body_rows") or [],
                "footnotes": normalized.get("footnotes") or [],
                "linearized_text": normalized.get("linearized_text") or "",
                "retrieval_text": normalized.get("retrieval_text") or "",
            }
        elif block_type == "diagram":
            detail = diagram_by_block.get(block_id) or {}
            block_asset = (assets_by_block.get(block_id) or [{}])[0]
            storage_path = str(
                block_asset.get("storage_path")
                or detail.get("storage_path")
                or ""
            ).strip()
            image_url = f"/uploads/{quote(storage_path, safe='/')}" if storage_path else ""
            normalized_diagram_fields = _normalize_diagram_review_fields(
                detail.get("visual_description") or normalized.get("visual_description") or "",
                detail.get("question_answerable_facts") or normalized.get("question_answerable_facts") or [],
            )
            item["normalized_content"] = {
                "diagram_kind": normalized.get("diagram_kind") or detail.get("diagram_kind") or block.get("subtype"),
                "visual_description": normalized_diagram_fields["visual_description"] or "",
                "ocr_text": detail.get("ocr_text") or normalized.get("ocr_text") or [],
                "question_answerable_facts": normalized_diagram_fields["question_answerable_facts"],
                "semantic_links": detail.get("semantic_links") or normalized.get("semantic_links") or [],
                "vision_status": detail.get("vision_status") or normalized.get("vision_status") or "",
                "vision_confidence": detail.get("vision_confidence"),
                "vision_gate_score": detail.get("vision_gate_score"),
                "vision_gate_reasons": detail.get("vision_gate_reasons") or [],
                "provider_name": detail.get("provider_name") or "",
                "model_name": detail.get("model_name") or "",
                "prompt_version": detail.get("prompt_version") or "",
                "last_analyzed_at": detail.get("last_analyzed_at"),
                "image_url": image_url,
                "storage_path": storage_path,
                "retrieval_text": normalized.get("retrieval_text") or "",
            }
            item["preview_anchor"] = {
                **_resolve_preview_anchor(
                    block,
                    anchor_registry,
                    block_bbox,
                    ((detail.get("image_region") or {}).get("bbox") or {}),
                    source_location.get("bbox"),
                ),
            }

        review_blocks.append(item)

    upload_path = str(document_result.get("upload_path") or "").strip()
    preview_url = f"/uploads/preview/{quote(upload_path, safe='/')}" if upload_path else ""
    upload_url = f"/uploads/{quote(upload_path, safe='/')}" if upload_path else ""
    return {
        "document_id": document_result.get("document_id"),
        "original_filename": document_result.get("original_filename") or "",
        "file_extension": document_result.get("file_extension") or "",
        "mime_type": document_result.get("mime_type") or "",
        "upload_path": upload_path,
        "preview_url": preview_url,
        "upload_url": upload_url,
        "parser_result": {
            "parser_status": parser_result.get("parser_status"),
            "parser_version": parser_result.get("parser_version"),
            "extraction_timestamp": parser_result.get("extraction_timestamp"),
            "file_type": parser_result.get("file_type"),
            "metadata": {
                "markdown_output": metadata.get("markdown_output") or "",
                "review": metadata.get("review") or {},
            },
            "review_summary": {
                "total_blocks": len(review_blocks),
                "text_blocks": counts.get("text", 0),
                "table_blocks": counts.get("table", 0),
                "diagram_blocks": counts.get("diagram", 0),
            },
            "review_blocks": review_blocks,
        },
    }


def get_extracted_document_result(user_id, document_id, conversation_id=None):
    if not user_id or not document_id:
        return None

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            if conversation_id:
                cur.execute(
                    """
                    SELECT 1
                    FROM conversations c
                    JOIN conversation_documents cd ON cd.conversation_id = c.conversation_id
                    WHERE c.user_id         = %s
                      AND c.conversation_id = %s
                      AND cd.document_id    = %s
                    """,
                    (user_id, conversation_id, document_id),
                )
            else:
                cur.execute(
                    """
                    SELECT 1
                    FROM documents
                    WHERE user_id    = %s
                      AND document_id = %s
                      AND is_deleted  = FALSE
                    """,
                    (user_id, document_id),
                )

            if not cur.fetchone():
                return None

            return fetch_document_extraction(
                cur,
                document_id=document_id,
                conversation_id=conversation_id,
            )
    except Exception:
        return None
    finally:
        if conn is not None:
            conn.close()


def get_conversation_extracted_results(user_id, conversation_id) -> list:
    if not user_id or not conversation_id:
        return []

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM conversations
                WHERE user_id = %s AND conversation_id = %s
                """,
                (user_id, conversation_id),
            )
            if not cur.fetchone():
                return []
            return fetch_conversation_extractions(cur, conversation_id=conversation_id)
    except Exception:
        return []
    finally:
        if conn is not None:
            conn.close()


# ===========================================================================
# 5. AUTH HELPERS  (Google OAuth, email dispatch, username utils)
# ===========================================================================

def _google_redirect_uri() -> str:
    configured = (os.getenv("GOOGLE_REDIRECT_URI") or "").strip()
    if configured:
        configured_base = configured.rsplit("/api/auth/google/callback", 1)[0]
        preferred_base = _preferred_external_base_url(configured_base)
        return urljoin(preferred_base.rstrip("/") + "/", "api/auth/google/callback")
    return build_external_url("/api/auth/google/callback")


def _build_google_return_url(next_path: str, status: str) -> str:
    safe_next = _sanitize_next_path(next_path)
    separator = "&" if "?" in safe_next else "?"
    return f"{safe_next}{separator}{urlencode({'google_auth': status})}"


def _google_username_from_profile(name: str, email: str) -> str:
    candidate = (name or "").strip()
    if not candidate and email and "@" in email:
        candidate = email.split("@", 1)[0]
    cleaned = re.sub(r"[^A-Za-z0-9_]", "", candidate)
    return (cleaned or "user")[:50]


def _pick_unique_username(cur, base_username: str) -> str:
    base = (base_username or "user").strip()[:50] or "user"
    cur.execute("SELECT 1 FROM users WHERE username = %s", (base,))
    if not cur.fetchone():
        return base

    for _ in range(20):
        suffix = secrets.token_hex(2)
        max_base_len = max(1, 50 - len(suffix) - 1)
        candidate = f"{base[:max_base_len]}_{suffix}"
        cur.execute("SELECT 1 FROM users WHERE username = %s", (candidate,))
        if not cur.fetchone():
            return candidate

    return f"user_{secrets.token_hex(4)}"   # extremely unlikely fallback


def _http_post_form(url: str, data: dict, timeout_seconds: int = 10) -> tuple[int, dict]:
    encoded = urlencode(data).encode("utf-8")
    req = Request(url, data=encoded,
                  headers={"Content-Type": "application/x-www-form-urlencoded"},
                  method="POST")
    try:
        with urlopen(req, timeout=timeout_seconds) as resp:
            payload = resp.read().decode("utf-8")
            return resp.status, json.loads(payload) if payload else {}
    except HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode("utf-8"))
        except Exception:
            return e.code, {}
    except (URLError, TimeoutError):
        return 0, {}


def _http_get_json(url: str, timeout_seconds: int = 10) -> tuple[int, dict]:
    req = Request(url, headers={"Accept": "application/json"}, method="GET")
    try:
        with urlopen(req, timeout=timeout_seconds) as resp:
            payload = resp.read().decode("utf-8")
            return resp.status, json.loads(payload) if payload else {}
    except HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode("utf-8"))
        except Exception:
            return e.code, {}
    except (URLError, TimeoutError):
        return 0, {}


def send_signup_verification_email(to_email: str, username: str, token: str) -> None:
    verify_url = build_external_url(f"/api/auth/verify-email?token={token}")
    subject    = "Verify your InsightHub email"

    text_body = (
        f"Hi {username},\n\n"
        "Thanks for signing up for InsightHub.\n"
        "Please verify your email by clicking the link below:\n\n"
        f"{verify_url}\n\n"
        "This link expires in 24 hours.\n\n"
        "If you did not create this account, you can ignore this email."
    )
    html_body = f"""
    <html>
      <body style="font-family: Arial, sans-serif; background-color: #f4f6f9; padding: 20px;">
        <div style="max-width: 500px; margin: auto; background: white; padding: 30px; border-radius: 8px;">
          <h2 style="color: #2c3e50;">Welcome to InsightHub</h2>
          <p>Hi {username},</p>
          <p>Thanks for signing up for InsightHub. Please verify your email by clicking the button below:</p>
          <div style="text-align: center; margin: 30px 0;">
            <a href="{verify_url}"
               style="background-color: #4f46e5; color: white; padding: 12px 24px;
                      text-decoration: none; border-radius: 6px; display: inline-block;">
              Verify Email
            </a>
          </div>
          <p style="font-size: 12px; color: #666;">
            This link expires in 24 hours.<br>
            If you did not create this account, you can safely ignore this email.
          </p>
        </div>
      </body>
    </html>
    """
    send_email(to_email, subject, text_body, html_body)


def send_forgot_password_link_email(to_email: str, username: str, token: str) -> None:
    reset_url = build_external_url(f"/api/auth/forgot-password/verify?token={token}")
    subject   = "Reset your InsightHub password"

    text_body = (
        f"Hi {username},\n\n"
        "We received a request to reset your InsightHub password.\n"
        "Please open the link below to continue:\n\n"
        f"{reset_url}\n\n"
        "This link expires in 30 minutes and can only be used once.\n\n"
        "If you did not request this, you can ignore this email."
    )
    html_body = f"""
    <html>
      <body style="font-family: Arial, sans-serif; background-color: #f4f6f9; padding: 20px;">
        <div style="max-width: 500px; margin: auto; background: white; padding: 30px; border-radius: 8px;">
          <h2 style="color: #2c3e50;">Password Reset Request</h2>
          <p>Hi {username},</p>
          <p>We received a request to reset your InsightHub password. Please click the button below to continue:</p>
          <div style="text-align: center; margin: 30px 0;">
            <a href="{reset_url}"
               style="background-color: #dc2626; color: white; padding: 12px 24px;
                      text-decoration: none; border-radius: 6px; display: inline-block;">
              Reset Password
            </a>
          </div>
          <p style="font-size: 12px; color: #666;">
            This link expires in 30 minutes and can only be used once.<br>
            If you did not request this, you can safely ignore this email.
          </p>
        </div>
      </body>
    </html>
    """
    send_email(to_email, subject, text_body, html_body)


# ===========================================================================
# 6. TEMPLATE CONTEXT
# ===========================================================================

@app.context_processor
def inject_auth_user():
    auth_user = get_current_user()
    sidebar_conversations = get_sidebar_conversations(
        auth_user.get("user_id") if auth_user else None
    )
    return {
        "auth_user":             auth_user,
        "sidebar_conversations": sidebar_conversations,
    }


# ===========================================================================
# 7. PAGE ROUTES
# ===========================================================================

@app.route('/')
def root():
    return dashboard()


@app.route('/dashboard')
def dashboard():
    user_id       = session.get("user_id")
    conversations = []

    if user_id:
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        c.conversation_id,
                        c.title,
                        c.updated_at,
                        COUNT(cd.document_id) AS source_count,
                        COALESCE(
                            STRING_AGG(d.original_filename, ' | ' ORDER BY d.created_at)
                            FILTER (WHERE d.document_id IS NOT NULL),
                            ''
                        ) AS sources_joined,
                        COALESCE(
                            JSONB_AGG(
                                DISTINCT JSONB_BUILD_OBJECT(
                                    'document_id',       d.document_id,
                                    'original_filename', d.original_filename
                                )
                            ) FILTER (WHERE d.document_id IS NOT NULL),
                            '[]'::jsonb
                        ) AS documents
                    FROM conversations c
                    LEFT JOIN conversation_documents cd ON cd.conversation_id = c.conversation_id
                    LEFT JOIN documents d
                           ON d.document_id = cd.document_id AND d.is_deleted = FALSE
                    WHERE c.user_id = %s
                    GROUP BY c.conversation_id, c.title, c.updated_at
                    ORDER BY c.updated_at DESC
                    """,
                    (user_id,),
                )
                rows = cur.fetchall()
                conversations = [_serialize_dashboard_conversation(row) for row in rows]
        except Exception:
            conversations = []
        finally:
            if conn is not None:
                conn.close()

    return render_template('dashboard.html', active_page='dashboard', conversations=conversations)


@app.route('/chat')
def chat():
    user_id                     = session.get("user_id")
    current_conversation_id     = (request.args.get("conversation_id") or "").strip()
    highlight_new_conversation   = request.args.get("new") == "1"
    conversation_title          = get_conversation_title(user_id, current_conversation_id)

    if current_conversation_id and not conversation_exists_for_user(user_id, current_conversation_id):
        return render_template(
            "conversation_not_found.html",
            active_page="chat",
            requested_conversation_id=current_conversation_id,
        ), 404

    conversation_documents       = get_conversation_documents(user_id, current_conversation_id)
    conversation_messages        = get_conversation_messages(user_id, current_conversation_id)
    conversation_study_aids      = list_conversation_study_aids(user_id, current_conversation_id) if current_conversation_id else []

    return render_template(
        'chat.html',
        active_page                 = 'chat',
        current_conversation_id     = current_conversation_id,
        conversation_title          = conversation_title,
        highlight_new_conversation  = highlight_new_conversation,
        conversation_documents      = conversation_documents,
        conversation_messages       = conversation_messages,
        conversation_study_aids     = conversation_study_aids,
        demo_messages               = _demo_chat_messages() if not current_conversation_id else [],
    )


@app.route('/documents/<document_id>/parser-results')
def document_parser_results(document_id):
    user_id = session.get("user_id")
    if not user_id:
        return redirect(url_for("dashboard"))

    conversation_id = (request.args.get("conversation_id") or "").strip() or None
    parser_result_context = get_document_parser_result_context(
        user_id=user_id,
        document_id=document_id,
        conversation_id=conversation_id,
    )
    document_result = get_document_parser_result(
        user_id         = user_id,
        document_id     = document_id,
        conversation_id = conversation_id,
    )
    if not document_result:
        return render_template(
            "document_parser_results_not_found.html",
            active_page="chat",
            requested_document_name=parser_result_context.get("document_name") or "",
            requested_conversation_title=parser_result_context.get("conversation_title") or "",
            requested_conversation_id=conversation_id,
        ), 404

    review_document = _build_parser_review_payload(document_result)
    review_document["diagram_analysis_usage"] = get_diagram_analysis_usage_summary()
    return render_template(
        "document_parser_results.html",
        active_page="chat",
        parser_document=review_document,
        parser_result_context=parser_result_context,
    )


@app.route('/flashcards')
def flashcards():
    user_id = session.get("user_id")
    study_aid_id = (request.args.get("aid_id") or "").strip()
    conversation_id = (request.args.get("conversation_id") or "").strip()
    document_id = (request.args.get("document_id") or "").strip()
    requirements = (request.args.get("requirements") or "").strip()
    page_range = (request.args.get("page_range") or "").strip()
    autostart = request.args.get("autostart") == "1"
    saved_study_aid = get_study_aid(user_id, study_aid_id, aid_type="flashcards") if study_aid_id else None
    saved_payload = saved_study_aid.get("payload_json") if saved_study_aid else {}
    generation_prompt = ""
    if saved_study_aid:
        conversation_id = saved_study_aid.get("conversation_id") or conversation_id
        document_id = saved_study_aid.get("document_id") or document_id
        requirements = saved_study_aid.get("source_requirements") or requirements
        page_range = saved_study_aid.get("page_range") or page_range
        generation_prompt = str(saved_payload.get("generation_prompt") or "").strip()
        autostart = False
    conversation_documents = get_conversation_documents(user_id, conversation_id) if conversation_id else []
    selected_document = next(
        (doc for doc in conversation_documents if str(doc.get("document_id") or "") == document_id),
        None,
    )
    return render_template(
        'flashcards.html',
        active_page='study',
        study_context={
            "conversation_id": conversation_id,
            "document_id": document_id,
            "document_name": selected_document.get("original_filename") if selected_document else "",
            "requirements": requirements,
            "page_range": page_range,
            "generation_prompt": generation_prompt or DEFAULT_FLASHCARD_GENERATION_PROMPT,
            "default_generation_prompt": DEFAULT_FLASHCARD_GENERATION_PROMPT,
            "autostart": autostart,
            "study_aid_id": saved_study_aid.get("study_aid_id") if saved_study_aid else "",
            "saved_payload": saved_payload,
            "saved_items": list_study_aids(user_id, aid_type="flashcards"),
            "documents": conversation_documents,
        },
    )


@app.route('/mindmap')
def mindmap():
    user_id = session.get("user_id")
    study_aid_id = (request.args.get("aid_id") or "").strip()
    conversation_id = (request.args.get("conversation_id") or "").strip()
    document_id = (request.args.get("document_id") or "").strip()
    requirements = (request.args.get("requirements") or "").strip()
    page_range = (request.args.get("page_range") or "").strip()
    autostart = request.args.get("autostart") == "1"
    saved_study_aid = get_study_aid(user_id, study_aid_id, aid_type="mindmap") if study_aid_id else None
    if saved_study_aid:
        conversation_id = saved_study_aid.get("conversation_id") or conversation_id
        document_id = saved_study_aid.get("document_id") or document_id
        requirements = saved_study_aid.get("source_requirements") or requirements
        page_range = saved_study_aid.get("page_range") or page_range
        autostart = False
    conversation_documents = get_conversation_documents(user_id, conversation_id) if conversation_id else []
    selected_document = next(
        (doc for doc in conversation_documents if str(doc.get("document_id") or "") == document_id),
        None,
    )
    return render_template(
        'mindmap.html',
        active_page='study',
        study_context={
            "conversation_id": conversation_id,
            "document_id": document_id,
            "document_name": selected_document.get("original_filename") if selected_document else "",
            "requirements": requirements,
            "page_range": page_range,
            "autostart": autostart,
            "study_aid_id": saved_study_aid.get("study_aid_id") if saved_study_aid else "",
            "saved_payload": saved_study_aid.get("payload_json") if saved_study_aid else {},
            "saved_items": list_study_aids(user_id, aid_type="mindmap"),
            "documents": conversation_documents,
        },
    )


# ===========================================================================
# 8. AUTH API ROUTES
# ===========================================================================

@app.route('/api/flashcards/generate', methods=['POST'])
def generate_flashcards():
    user_id = session.get("user_id")
    data = request.get_json(silent=True) or {}
    topic = str(data.get('topic') or '').strip()
    document_id = str(data.get('document_id') or '').strip()
    conversation_id = str(data.get('conversation_id') or '').strip() or None
    requirements = str(data.get('requirements') or '').strip()
    page_range = str(data.get('page_range') or '').strip()
    generation_prompt = str(data.get('generation_prompt') or '').strip()
    count = data.get('count')

    try:
        count = max(3, min(25, int(count or 10)))
    except (TypeError, ValueError):
        count = 10

    if not topic and not document_id:
        return jsonify({'error': 'A topic or document is required.'}), 400

    try:
        document_source = None
        if document_id:
            if not user_id:
                return jsonify({'error': 'You must be logged in.'}), 401
            document_source = _load_document_study_source(
                user_id=user_id,
                document_id=document_id,
                conversation_id=conversation_id,
                page_range=page_range,
            )
        payload = _generate_flashcards_with_ai(
            topic=topic,
            count=count,
            document_source=document_source,
            requirements=requirements,
            user_prompt=generation_prompt,
        )
        saved_study_aid = save_study_aid(
            user_id=user_id,
            aid_type="flashcards",
            title=payload["deck_title"],
            payload_json={
                "deck_title": payload["deck_title"],
                "cards": payload["cards"],
                "generation_prompt": payload["generation_prompt"],
            },
            conversation_id=conversation_id,
            document_id=document_id,
            source_requirements=requirements,
            page_range=page_range,
        )
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 503
    except json.JSONDecodeError:
        logger.exception("Flashcard generation returned invalid JSON")
        return jsonify({'error': 'AI returned an invalid response.'}), 502
    except Exception as exc:
        logger.exception("Unexpected flashcard generation failure")
        return jsonify({'error': f'Unable to generate flashcards right now. {exc}'}), 500

    return jsonify({
        'deck_title': payload['deck_title'],
        'cards': payload['cards'],
        'model': payload['model'],
        'generation_prompt': payload['generation_prompt'],
        'study_aid_id': saved_study_aid.get('study_aid_id') if saved_study_aid else '',
    })


@app.route('/api/mindmap/generate', methods=['POST'])
def generate_mindmap():
    user_id = session.get("user_id")
    if not user_id:
        return jsonify({'error': 'You must be logged in.'}), 401

    data = request.get_json(silent=True) or {}
    document_id = str(data.get('document_id') or '').strip()
    conversation_id = str(data.get('conversation_id') or '').strip() or None
    requirements = str(data.get('requirements') or '').strip()
    page_range = str(data.get('page_range') or '').strip()

    if not document_id:
        return jsonify({'error': 'Document is required.'}), 400

    try:
        document_source = _load_document_study_source(
            user_id=user_id,
            document_id=document_id,
            conversation_id=conversation_id,
            page_range=page_range,
        )
        payload = _generate_mindmap_with_ai(
            document_source=document_source,
            requirements=requirements,
        )
        saved_study_aid = save_study_aid(
            user_id=user_id,
            aid_type="mindmap",
            title=payload["title"],
            payload_json={
                "title": payload["title"],
                "nodes": payload["nodes"],
            },
            conversation_id=conversation_id,
            document_id=document_id,
            source_requirements=requirements,
            page_range=page_range,
        )
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 503
    except json.JSONDecodeError:
        logger.exception("Mind map generation returned invalid JSON")
        return jsonify({'error': 'AI returned an invalid response.'}), 502
    except Exception as exc:
        logger.exception("Unexpected mind map generation failure")
        return jsonify({'error': f'Unable to generate a mind map right now. {exc}'}), 500

    return jsonify({
        'title': payload['title'],
        'nodes': payload['nodes'],
        'model': payload['model'],
        'study_aid_id': saved_study_aid.get('study_aid_id') if saved_study_aid else '',
    })


@app.route('/api/study-aids/<study_aid_id>', methods=['GET'])
def api_study_aid_detail(study_aid_id):
    user_id = session.get("user_id")
    if not user_id:
        return jsonify({'error': 'You must be logged in.'}), 401

    payload = get_study_aid(user_id, (study_aid_id or "").strip())
    if not payload:
        return jsonify({'error': 'Study aid not found.'}), 404
    return jsonify(payload), 200


@app.route('/api/study-aids', methods=['POST'])
def api_create_study_aid():
    user_id = session.get("user_id")
    if not user_id:
        return jsonify({'error': 'You must be logged in.'}), 401
    data = request.get_json(silent=True) or {}
    aid_type = str(data.get('aid_type') or '').strip()
    title = str(data.get('title') or '').strip()
    payload_json = data.get('payload_json') if isinstance(data.get('payload_json'), dict) else {}
    result = save_study_aid(
        user_id=user_id,
        aid_type=aid_type,
        title=title,
        payload_json=payload_json,
        conversation_id=str(data.get('conversation_id') or '').strip() or None,
        document_id=str(data.get('document_id') or '').strip() or None,
        source_requirements=str(data.get('source_requirements') or '').strip(),
        page_range=str(data.get('page_range') or '').strip(),
    )
    if not result:
        return jsonify({'error': 'Unable to save study aid.'}), 500
    return jsonify(result), 201


@app.route('/api/study-aids/<study_aid_id>', methods=['PUT'])
def api_update_study_aid(study_aid_id):
    user_id = session.get("user_id")
    if not user_id:
        return jsonify({'error': 'You must be logged in.'}), 401
    data = request.get_json(silent=True) or {}
    payload_json = data.get('payload_json') if isinstance(data.get('payload_json'), dict) else {}
    result = update_study_aid(
        user_id=user_id,
        study_aid_id=(study_aid_id or '').strip(),
        title=str(data.get('title') or '').strip(),
        payload_json=payload_json,
        source_requirements=str(data.get('source_requirements') or '').strip(),
        page_range=str(data.get('page_range') or '').strip(),
    )
    if not result:
        return jsonify({'error': 'Unable to update study aid.'}), 404
    return jsonify(result), 200

@app.route('/api/auth/signup', methods=['POST'])
def signup():
    data     = request.get_json(silent=True) or {}
    username = (data.get('username') or '').strip()
    email    = (data.get('email')    or '').strip().lower()
    password =  data.get('password') or ''

    if not username:
        return jsonify({'error': 'Username is required.'}), 400
    if not email:
        return jsonify({'error': 'Email is required.'}), 400
    if not password:
        return jsonify({'error': 'Password is required.'}), 400
    if not STRONG_PASSWORD_REGEX.match(password):
        return jsonify({'error': PASSWORD_POLICY_ERROR}), 400

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            password_hash = generate_password_hash(password)
            cur.execute(
                """
                INSERT INTO users (username, email, auth_provider, password_hash)
                VALUES (%s, %s, 'local', %s)
                RETURNING user_id, username, email, created_at, email_verified
                """,
                (username, email, password_hash),
            )
            created_user = cur.fetchone()

            verification_token = secrets.token_urlsafe(32)
            token_hash  = hashlib.sha256(verification_token.encode("utf-8")).hexdigest()
            expires_at  = datetime.now(timezone.utc) + timedelta(hours=24)
            cur.execute(
                """
                INSERT INTO user_verification_tokens (user_id, purpose, token_hash, expires_at)
                VALUES (%s, 'email_verify', %s, %s)
                """,
                (created_user[0], token_hash, expires_at),
            )

        conn.commit()
        session.pop("password_reset_user_id", None)
        session["user_id"] = str(created_user[0])

        user_payload = serialize_user_row(created_user)
        email_sent   = True
        try:
            send_signup_verification_email(email, username, verification_token)
        except Exception:
            email_sent = False

        return jsonify({
            'message': (
                'Signup successful. Please check your email to verify your account.'
                if email_sent
                else 'Signup successful, but we could not send the verification email right now.'
            ),
            'user':                       user_payload,
            'verification_required':      not user_payload["email_verified"],
            'verification_email_sent':    email_sent,
        }), 201
    except errors.UniqueViolation:
        if conn is not None:
            conn.rollback()
        return jsonify({'error': 'An account with this email already exists.'}), 409
    except psycopg2.IntegrityError as e:
        if conn is not None:
            conn.rollback()
        logging.getLogger(__name__).warning("Integrity error during signup: %s", e)
        return jsonify({'error': 'Invalid signup data.'}), 400
    except Exception:
        if conn is not None:
            conn.rollback()
        logging.getLogger(__name__).exception("Unexpected signup failure for email=%s", email)
        return jsonify({'error': 'Unable to create account right now.'}), 500
    finally:
        if conn is not None:
            conn.close()


@app.route('/api/auth/login', methods=['POST'])
def login():
    data     = request.get_json(silent=True) or {}
    email    = (data.get('email')    or '').strip().lower()
    password =  data.get('password') or ''

    if not email:
        return jsonify({'error': 'Email is required.'}), 400
    if not password:
        return jsonify({'error': 'Password is required.'}), 400

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT user_id, username, email, created_at, email_verified, password_hash, auth_provider
                FROM users
                WHERE email = %s
                """,
                (email,),
            )
            user_row = cur.fetchone()

        if not user_row:
            return jsonify({'error': 'Invalid email or password.'}), 401
        if user_row[6] != 'local':
            return jsonify({'error': 'This account uses a different sign-in method.'}), 400

        password_hash = user_row[5] or ''
        if not password_hash or not check_password_hash(password_hash, password):
            return jsonify({'error': 'Invalid email or password.'}), 401

        session["user_id"] = str(user_row[0])
        session.pop("password_reset_user_id", None)
        return jsonify({'message': 'Login successful.', 'user': serialize_user_row(user_row[:5])}), 200
    except Exception:
        return jsonify({'error': 'Unable to log in right now.'}), 500
    finally:
        if conn is not None:
            conn.close()


@app.route('/api/auth/logout', methods=['POST'])
def logout():
    session.pop("user_id", None)
    session.pop("password_reset_user_id", None)
    return jsonify({'message': 'Logged out.'}), 200


@app.route('/api/auth/verify-email', methods=['GET'])
def verify_email():
    token = (request.args.get("token") or "").strip()
    if not token:
        return redirect(url_for('dashboard', email_verified='invalid'))

    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
    now_utc    = datetime.now(timezone.utc)

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT user_id FROM user_verification_tokens
                WHERE purpose    = 'email_verify'
                  AND token_hash = %s
                  AND used_at    IS NULL
                  AND expires_at > %s
                """,
                (token_hash, now_utc),
            )
            token_row = cur.fetchone()
            if not token_row:
                return redirect(url_for('dashboard', email_verified='invalid'))

            user_id = token_row[0]
            cur.execute("UPDATE users SET email_verified = TRUE WHERE user_id = %s", (user_id,))
            cur.execute(
                """
                UPDATE user_verification_tokens
                SET used_at = %s
                WHERE user_id    = %s
                  AND purpose    = 'email_verify'
                  AND token_hash = %s
                  AND used_at    IS NULL
                """,
                (now_utc, user_id, token_hash),
            )

        conn.commit()
        return redirect(url_for('dashboard', email_verified='success'))
    except Exception:
        if conn is not None:
            conn.rollback()
        return redirect(url_for('dashboard', email_verified='error'))
    finally:
        if conn is not None:
            conn.close()


@app.route('/api/auth/resend-verification', methods=['POST'])
def resend_verification():
    user_id = session.get("user_id")
    if not user_id:
        return jsonify({'error': 'You must be logged in to resend verification email.'}), 401

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT user_id, username, email, email_verified, auth_provider
                FROM users WHERE user_id = %s
                """,
                (user_id,),
            )
            user_row = cur.fetchone()
            if not user_row:
                session.pop("user_id", None)
                return jsonify({'error': 'User not found.'}), 404
            if user_row[4] != 'local':
                return jsonify({'error': 'This account uses a different sign-in method.'}), 400
            if bool(user_row[3]):
                return jsonify({'error': 'Your email is already verified.'}), 400

            verification_token = secrets.token_urlsafe(32)
            token_hash = hashlib.sha256(verification_token.encode("utf-8")).hexdigest()
            expires_at = datetime.now(timezone.utc) + timedelta(hours=24)

            cur.execute(
                "DELETE FROM user_verification_tokens WHERE user_id = %s AND purpose = 'email_verify'",
                (user_row[0],),
            )
            cur.execute(
                """
                INSERT INTO user_verification_tokens (user_id, purpose, token_hash, expires_at)
                VALUES (%s, 'email_verify', %s, %s)
                """,
                (user_row[0], token_hash, expires_at),
            )

        conn.commit()
        try:
            send_signup_verification_email(user_row[2], user_row[1], verification_token)
            return jsonify({'message': 'Verification email sent. Please check your inbox.'}), 200
        except Exception:
            return jsonify({'error': 'Unable to send verification email right now.'}), 500
    except Exception:
        if conn is not None:
            conn.rollback()
        return jsonify({'error': 'Unable to resend verification right now.'}), 500
    finally:
        if conn is not None:
            conn.close()


@app.route('/api/auth/profile', methods=['POST'])
def update_profile():
    user_id  = session.get("user_id")
    if not user_id:
        return jsonify({'error': 'You must be logged in.'}), 401

    data     = request.get_json(silent=True) or {}
    username = (data.get('username') or '').strip()
    if not username:
        return jsonify({'error': 'Username is required.'}), 400

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE users SET username = %s WHERE user_id = %s
                RETURNING user_id, username, email, created_at, email_verified, auth_provider, google_sub, password_hash
                """,
                (username, user_id),
            )
            updated_user = cur.fetchone()

        if not updated_user:
            if conn is not None:
                conn.rollback()
            session.pop("user_id", None)
            return jsonify({'error': 'User not found.'}), 404

        conn.commit()
        return jsonify({'message': 'Profile updated.', 'user': serialize_user_row(updated_user)}), 200
    except errors.UniqueViolation:
        if conn is not None:
            conn.rollback()
        return jsonify({'error': 'That username is already taken.'}), 409
    except psycopg2.IntegrityError:
        if conn is not None:
            conn.rollback()
        return jsonify({'error': 'Invalid profile data.'}), 400
    except Exception:
        if conn is not None:
            conn.rollback()
        return jsonify({'error': 'Unable to update profile right now.'}), 500
    finally:
        if conn is not None:
            conn.close()


@app.route('/api/auth/change-password', methods=['POST'])
def change_password():
    user_id = session.get("user_id")
    if not user_id:
        return jsonify({'error': 'You must be logged in.'}), 401

    data = request.get_json(silent=True) or {}
    current_password = data.get('current_password') or ''
    new_password = data.get('new_password') or ''
    confirm_password = data.get('confirm_password') or ''
    if not current_password:
        return jsonify({'error': 'Current password is required.'}), 400
    if not new_password:
        return jsonify({'error': 'New password is required.'}), 400
    if not confirm_password:
        return jsonify({'error': 'Confirm password is required.'}), 400
    if new_password != confirm_password:
        return jsonify({'error': 'New password and confirm password must match.'}), 400
    if not STRONG_PASSWORD_REGEX.match(new_password):
        return jsonify({'error': PASSWORD_POLICY_ERROR}), 400
    if current_password == new_password:
        return jsonify({'error': 'New password must be different from the current password.'}), 400

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT auth_provider, password_hash FROM users WHERE user_id = %s", (user_id,))
            row = cur.fetchone()
            if not row:
                session.pop("user_id", None)
                return jsonify({'error': 'User not found.'}), 404
            if row[0] != 'local':
                return jsonify({'error': 'This account uses a different sign-in method.'}), 400
            password_hash = row[1] or ''
            if not password_hash or not check_password_hash(password_hash, current_password):
                return jsonify({'error': 'Current password is incorrect.'}), 401

            cur.execute(
                "UPDATE users SET password_hash = %s WHERE user_id = %s",
                (generate_password_hash(new_password), user_id),
            )
        conn.commit()
        session.pop("password_reset_user_id", None)
        return jsonify({'message': 'Password updated successfully.'}), 200
    except Exception:
        if conn is not None:
            conn.rollback()
        return jsonify({'error': 'Unable to update password right now.'}), 500
    finally:
        if conn is not None:
            conn.close()


@app.route('/api/auth/settings', methods=['GET'])
def auth_settings():
    user_id = session.get("user_id")
    if not user_id:
        return jsonify({'error': 'You must be logged in.'}), 401

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    user_id,
                    username,
                    email,
                    created_at,
                    email_verified,
                    auth_provider,
                    google_sub,
                    password_hash
                FROM users
                WHERE user_id = %s
                """,
                (user_id,),
            )
            row = cur.fetchone()

            if not row:
                session.pop("user_id", None)
                return jsonify({'error': 'User not found.'}), 404

            prompt_profiles = get_prompt_profiles_for_user(cur, user_id)

        default_prompt_profiles = get_default_prompt_profiles()
        effective_prompt_profiles = {
            prompt_type: prompt_profiles.get(prompt_type) or default_prompt_profiles.get(prompt_type, '')
            for prompt_type in default_prompt_profiles
        }
        user_payload = serialize_user_row(row[:8]) or {}
        return jsonify({
            'user': user_payload,
            'custom_system_prompt': prompt_profiles.get(PROMPT_TYPE_QNA, ''),
            'prompt_profiles': prompt_profiles,
            'default_prompt_profiles': default_prompt_profiles,
            'effective_prompt_profiles': effective_prompt_profiles,
        }), 200
    except Exception:
        return jsonify({'error': 'Unable to load settings right now.'}), 500
    finally:
        if conn is not None:
            conn.close()


@app.route('/api/auth/system-prompt', methods=['POST'])
def update_system_prompt():
    user_id = session.get("user_id")
    if not user_id:
        return jsonify({'error': 'You must be logged in.'}), 401

    data = request.get_json(silent=True) or {}
    custom_system_prompt = (data.get('custom_system_prompt') or '').strip()
    if len(custom_system_prompt) > PROMPT_PROFILE_MAX_LENGTH:
        return jsonify({'error': f'System prompt must be {PROMPT_PROFILE_MAX_LENGTH} characters or fewer.'}), 400

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT user_id FROM users WHERE user_id = %s",
                (user_id,),
            )
            updated = cur.fetchone()

            if not updated:
                session.pop("user_id", None)
                return jsonify({'error': 'User not found.'}), 404
            prompt_profiles = save_prompt_profiles_for_user(
                cur,
                user_id,
                {PROMPT_TYPE_QNA: custom_system_prompt},
            )
        conn.commit()
        default_prompt_profiles = get_default_prompt_profiles()
        return jsonify({
            'message': 'System prompt updated.',
            'custom_system_prompt': prompt_profiles.get(PROMPT_TYPE_QNA, ''),
            'prompt_profiles': prompt_profiles,
            'default_prompt_profiles': default_prompt_profiles,
            'effective_prompt_profiles': {
                prompt_type: prompt_profiles.get(prompt_type) or default_prompt_profiles.get(prompt_type, '')
                for prompt_type in default_prompt_profiles
            },
        }), 200
    except Exception:
        if conn is not None:
            conn.rollback()
        return jsonify({'error': 'Unable to update system prompt right now.'}), 500
    finally:
        if conn is not None:
            conn.close()


@app.route('/api/auth/prompt-profiles', methods=['POST'])
def update_prompt_profiles():
    user_id = session.get("user_id")
    if not user_id:
        return jsonify({'error': 'You must be logged in.'}), 401

    data = request.get_json(silent=True) or {}
    submitted_profiles = data.get('prompt_profiles') if isinstance(data.get('prompt_profiles'), dict) else {}
    prompt_profiles = {
        PROMPT_TYPE_QNA: str(submitted_profiles.get(PROMPT_TYPE_QNA) or '').strip(),
        PROMPT_TYPE_VISION: str(submitted_profiles.get(PROMPT_TYPE_VISION) or '').strip(),
    }

    for prompt_type, prompt_text in prompt_profiles.items():
        if len(prompt_text) > PROMPT_PROFILE_MAX_LENGTH:
            return jsonify({
                'error': f'{prompt_type.upper()} prompt must be {PROMPT_PROFILE_MAX_LENGTH} characters or fewer.',
            }), 400

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT user_id FROM users WHERE user_id = %s",
                (user_id,),
            )
            existing = cur.fetchone()
            if not existing:
                session.pop("user_id", None)
                return jsonify({'error': 'User not found.'}), 404
            saved_profiles = save_prompt_profiles_for_user(cur, user_id, prompt_profiles)
        conn.commit()
        default_prompt_profiles = get_default_prompt_profiles()
        return jsonify({
            'message': 'Prompt profiles updated.',
            'custom_system_prompt': saved_profiles.get(PROMPT_TYPE_QNA, ''),
            'prompt_profiles': saved_profiles,
            'default_prompt_profiles': default_prompt_profiles,
            'effective_prompt_profiles': {
                prompt_type: saved_profiles.get(prompt_type) or default_prompt_profiles.get(prompt_type, '')
                for prompt_type in default_prompt_profiles
            },
        }), 200
    except Exception:
        if conn is not None:
            conn.rollback()
        return jsonify({'error': 'Unable to update prompt profiles right now.'}), 500
    finally:
        if conn is not None:
            conn.close()


@app.route('/api/auth/system-prompt/regenerate', methods=['POST'])
def regenerate_system_prompt():
    user_id = session.get("user_id")
    if not user_id:
        return jsonify({'error': 'You must be logged in.'}), 401

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT username
                FROM users
                WHERE user_id = %s
                """,
                (user_id,),
            )
            row = cur.fetchone()
            if not row:
                session.pop("user_id", None)
                return jsonify({'error': 'User not found.'}), 404

            regenerated_prompt = _build_default_user_system_prompt(row[0] or "")
            cur.execute(
                """
                UPDATE users
                SET custom_system_prompt = %s
                WHERE user_id = %s
                RETURNING custom_system_prompt
                """,
                (regenerated_prompt, user_id),
            )
            updated = cur.fetchone()

        conn.commit()
        return jsonify({
            'message': 'System prompt regenerated.',
            'custom_system_prompt': (updated[0] if updated else regenerated_prompt) or regenerated_prompt,
        }), 200
    except Exception:
        if conn is not None:
            conn.rollback()
        return jsonify({'error': 'Unable to regenerate system prompt right now.'}), 500
    finally:
        if conn is not None:
            conn.close()


@app.route('/api/auth/forgot-password/request', methods=['POST'])
def forgot_password_request():
    data  = request.get_json(silent=True) or {}
    email = (data.get('email') or '').strip().lower()
    if not email:
        return jsonify({'error': 'Email is required.'}), 400

    conn        = None
    reset_token = None
    username    = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT user_id, auth_provider, username FROM users WHERE email = %s",
                (email,),
            )
            user_row = cur.fetchone()

            if user_row and user_row[1] == 'local':
                user_id     = user_row[0]
                username    = user_row[2] or "there"
                reset_token = secrets.token_urlsafe(32)
                token_hash  = hashlib.sha256(reset_token.encode("utf-8")).hexdigest()
                expires_at  = datetime.now(timezone.utc) + timedelta(minutes=30)

                cur.execute(
                    """
                    UPDATE user_verification_tokens
                    SET used_at = %s
                    WHERE user_id = %s AND purpose = 'password_reset' AND used_at IS NULL
                    """,
                    (datetime.now(timezone.utc), user_id),
                )
                cur.execute(
                    """
                    INSERT INTO user_verification_tokens (user_id, purpose, token_hash, expires_at)
                    VALUES (%s, 'password_reset', %s, %s)
                    """,
                    (user_id, token_hash, expires_at),
                )

        conn.commit()
        if reset_token:
            try:
                send_forgot_password_link_email(email, username or "there", reset_token)
            except Exception:
                pass

        # Always return generic success to avoid account enumeration.
        return jsonify({
            'message': 'If an account exists with this email, a reset link has been sent.'
        }), 200
    except Exception:
        if conn is not None:
            conn.rollback()
        return jsonify({'error': 'Unable to process request right now.'}), 500
    finally:
        if conn is not None:
            conn.close()


@app.route('/api/auth/forgot-password/verify', methods=['GET'])
def forgot_password_verify():
    token = (request.args.get("token") or "").strip()
    if not token:
        return redirect(url_for('dashboard', pwd_reset='invalid'))

    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
    now_utc    = datetime.now(timezone.utc)

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT u.user_id
                FROM users u
                JOIN user_verification_tokens t ON t.user_id = u.user_id
                WHERE t.purpose    = 'password_reset'
                  AND t.token_hash = %s
                  AND t.used_at    IS NULL
                  AND t.expires_at > %s
                """,
                (token_hash, now_utc),
            )
            row = cur.fetchone()
            if not row:
                return redirect(url_for('dashboard', pwd_reset='invalid'))

            cur.execute(
                """
                UPDATE user_verification_tokens
                SET used_at = %s
                WHERE user_id    = %s
                  AND purpose    = 'password_reset'
                  AND token_hash = %s
                  AND used_at    IS NULL
                """,
                (now_utc, row[0], token_hash),
            )
            session.pop("password_reset_user_id", None)
            session["password_reset_user_id"] = str(row[0])

        conn.commit()
        return redirect(url_for('dashboard', pwd_reset='verified'))
    except Exception:
        if conn is not None:
            conn.rollback()
        return redirect(url_for('dashboard', pwd_reset='error'))
    finally:
        if conn is not None:
            conn.close()


@app.route('/api/auth/forgot-password/reset', methods=['POST'])
def forgot_password_reset():
    pending_reset_user_id = session.get("password_reset_user_id")
    if not pending_reset_user_id:
        return jsonify({'error': 'Your reset session is invalid or expired. Please request a new reset link.'}), 401

    data         = request.get_json(silent=True) or {}
    new_password = data.get('new_password') or ''
    if not new_password:
        return jsonify({'error': 'New password is required.'}), 400
    if not STRONG_PASSWORD_REGEX.match(new_password):
        return jsonify({'error': PASSWORD_POLICY_ERROR}), 400

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT user_id, auth_provider FROM users WHERE user_id = %s",
                (pending_reset_user_id,),
            )
            user_row = cur.fetchone()
            if not user_row:
                session.pop("password_reset_user_id", None)
                return jsonify({'error': 'User not found.'}), 404
            if user_row[1] != 'local':
                session.pop("password_reset_user_id", None)
                return jsonify({'error': 'This account uses a different sign-in method.'}), 400

            cur.execute(
                """
                UPDATE users SET password_hash = %s WHERE user_id = %s
                RETURNING user_id, username, email, created_at, email_verified
                """,
                (generate_password_hash(new_password), pending_reset_user_id),
            )
            updated_user = cur.fetchone()

        conn.commit()
        session.pop("password_reset_user_id", None)
        session["user_id"] = str(updated_user[0])
        return jsonify({'message': 'Password updated successfully.', 'user': serialize_user_row(updated_user)}), 200
    except Exception:
        if conn is not None:
            conn.rollback()
        return jsonify({'error': 'Unable to update password right now.'}), 500
    finally:
        if conn is not None:
            conn.close()


@app.route('/api/auth/google/start', methods=['GET'])
def google_auth_start():
    google_client_id     = (os.getenv("GOOGLE_CLIENT_ID")     or "").strip()
    google_client_secret = (os.getenv("GOOGLE_CLIENT_SECRET") or "").strip()
    next_path            = _sanitize_next_path(request.args.get("next") or "/dashboard")

    if not google_client_id or not google_client_secret:
        return redirect(_build_google_return_url(next_path, "error"))

    oauth_state = secrets.token_urlsafe(32)
    session["google_oauth_state"] = oauth_state
    session["google_oauth_next"]  = next_path
    session["google_oauth_intent"] = "signin"

    auth_params = {
        "client_id":     google_client_id,
        "redirect_uri":  _google_redirect_uri(),
        "response_type": "code",
        "scope":         "openid email profile",
        "state":         oauth_state,
        "prompt":        "select_account",
    }
    return redirect(f"https://accounts.google.com/o/oauth2/v2/auth?{urlencode(auth_params)}")


@app.route('/api/auth/google/link/start', methods=['GET'])
def google_link_start():
    user_id = session.get("user_id")
    if not user_id:
        return redirect(_build_google_return_url("/dashboard", "error"))

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT auth_provider, google_sub
                FROM users
                WHERE user_id = %s
                """,
                (user_id,),
            )
            row = cur.fetchone()
            if not row:
                session.pop("user_id", None)
                return redirect(_build_google_return_url("/dashboard", "error"))
            if row[0] != 'local':
                return redirect(_build_google_return_url("/dashboard", "error"))
            if row[1]:
                return redirect(_build_google_return_url("/dashboard", "linked"))
    except Exception:
        return redirect(_build_google_return_url("/dashboard", "error"))
    finally:
        if conn is not None:
            conn.close()

    google_client_id     = (os.getenv("GOOGLE_CLIENT_ID")     or "").strip()
    google_client_secret = (os.getenv("GOOGLE_CLIENT_SECRET") or "").strip()
    if not google_client_id or not google_client_secret:
        return redirect(_build_google_return_url("/dashboard", "error"))

    oauth_state = secrets.token_urlsafe(32)
    session["google_oauth_state"] = oauth_state
    session["google_oauth_next"] = "/dashboard"
    session["google_oauth_intent"] = "link"

    auth_params = {
        "client_id":     google_client_id,
        "redirect_uri":  _google_redirect_uri(),
        "response_type": "code",
        "scope":         "openid email profile",
        "state":         oauth_state,
        "prompt":        "select_account",
    }
    return redirect(f"https://accounts.google.com/o/oauth2/v2/auth?{urlencode(auth_params)}")


@app.route('/api/auth/google/callback', methods=['GET'])
def google_auth_callback():
    logger         = logging.getLogger(__name__)
    error          = (request.args.get("error")  or "").strip()
    code           = (request.args.get("code")   or "").strip()
    returned_state = (request.args.get("state")  or "").strip()

    expected_state = session.pop("google_oauth_state", None)
    next_path      = _sanitize_next_path(session.pop("google_oauth_next", "/dashboard"))
    oauth_intent = (session.pop("google_oauth_intent", "signin") or "signin").strip().lower()

    if error or not code or not expected_state or returned_state != expected_state:
        logger.error(
            "Google OAuth state error: error=%s code=%s expected=%s returned=%s",
            error, code, expected_state, returned_state,
        )
        return redirect(_build_google_return_url(next_path, "error"))

    google_client_id     = (os.getenv("GOOGLE_CLIENT_ID")     or "").strip()
    google_client_secret = (os.getenv("GOOGLE_CLIENT_SECRET") or "").strip()
    if not google_client_id or not google_client_secret:
        return redirect(_build_google_return_url(next_path, "error"))

    token_status, token_payload = _http_post_form(
        "https://oauth2.googleapis.com/token",
        {
            "code":          code,
            "client_id":     google_client_id,
            "client_secret": google_client_secret,
            "redirect_uri":  _google_redirect_uri(),
            "grant_type":    "authorization_code",
        },
    )
    access_token = (token_payload or {}).get("access_token")
    if token_status != 200 or not access_token:
        logger.error("Google token exchange failed: status=%s payload=%s", token_status, token_payload)
        return redirect(_build_google_return_url(next_path, "error"))

    userinfo_status, userinfo_payload = _http_get_json(
        f"https://openidconnect.googleapis.com/v1/userinfo?{urlencode({'access_token': access_token})}"
    )
    if userinfo_status != 200:
        logger.error("Google userinfo failed: status=%s payload=%s", userinfo_status, userinfo_payload)
        return redirect(_build_google_return_url(next_path, "error"))

    email = (userinfo_payload.get("email") or "").strip().lower()
    if not email:
        return redirect(_build_google_return_url(next_path, "error"))

    email_verified = userinfo_payload.get("email_verified")
    if isinstance(email_verified, str):
        email_verified = email_verified.lower() == "true"
    if not bool(email_verified):
        logger.error("Google account email not verified: %s", email)
        return redirect(_build_google_return_url(next_path, "error"))

    profile_name = (userinfo_payload.get("name") or "").strip()
    google_sub   = (userinfo_payload.get("sub")  or "").strip()

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT user_id, username, email, created_at, email_verified, auth_provider, google_sub
                FROM users WHERE email = %s
                """,
                (email,),
            )
            user_row = cur.fetchone()

            if oauth_intent == "link":
                linking_user_id = session.get("user_id")
                if not linking_user_id:
                    return redirect(_build_google_return_url(next_path, "error"))
                if not user_row or str(user_row[0]) != str(linking_user_id):
                    return redirect(_build_google_return_url(next_path, "link_mismatch"))
                if user_row[5] != 'local':
                    return redirect(_build_google_return_url(next_path, "error"))
                if user_row[6] and user_row[6] != google_sub:
                    return redirect(_build_google_return_url(next_path, "error"))

                cur.execute(
                    """
                    UPDATE users
                    SET google_sub = %s, email_verified = TRUE
                    WHERE user_id = %s
                    RETURNING user_id, username, email, created_at, email_verified, auth_provider, google_sub
                    """,
                    (google_sub, linking_user_id),
                )
                selected_user = cur.fetchone()
                if not selected_user:
                    return redirect(_build_google_return_url(next_path, "error"))
            else:
                if user_row and user_row[5] != 'google':
                    if user_row[6] == google_sub:
                        selected_user = user_row[:5]
                    else:
                        return redirect(_build_google_return_url(next_path, "conflict"))

                if not user_row:
                    base_username = _google_username_from_profile(profile_name, email)
                    username      = _pick_unique_username(cur, base_username)
                    cur.execute(
                        """
                        INSERT INTO users (username, email, auth_provider, google_sub, password_hash, email_verified)
                        VALUES (%s, %s, 'google', %s, NULL, TRUE)
                        RETURNING user_id, username, email, created_at, email_verified
                        """,
                        (username, email, google_sub),
                    )
                    selected_user = cur.fetchone()
                else:
                    if not bool(user_row[4]):
                        cur.execute(
                            """
                            UPDATE users SET email_verified = TRUE WHERE user_id = %s
                            RETURNING user_id, username, email, created_at, email_verified
                            """,
                            (user_row[0],),
                        )
                        selected_user = cur.fetchone()
                    else:
                        selected_user = user_row[:5]

        conn.commit()
        session["user_id"] = str(selected_user[0])
        session.pop("password_reset_user_id", None)
        if oauth_intent == "link":
            return redirect(_build_google_return_url(next_path, "linked"))
        return redirect(_build_google_return_url(next_path, "success"))
    except Exception:
        logger.exception("Google OAuth callback failed")
        if conn is not None:
            conn.rollback()
        return redirect(_build_google_return_url(next_path, "error"))
    finally:
        if conn is not None:
            conn.close()


# ===========================================================================
# 9. DOCUMENT API ROUTES
# ===========================================================================

def _save_uploaded_file(incoming_file, destination: Path) -> tuple[int, str]:
    """Stream an uploaded file to disk. Returns (file_size_bytes, sha256_hex)."""
    file_hash      = hashlib.sha256()
    file_size_bytes = 0
    incoming_file.stream.seek(0)
    with destination.open("wb") as output_file:
        while True:
            chunk = incoming_file.stream.read(1024 * 1024)
            if not chunk:
                break
            file_size_bytes += len(chunk)
            file_hash.update(chunk)
            output_file.write(chunk)
    return file_size_bytes, file_hash.hexdigest()


def _validate_uploaded_files(incoming_files) -> tuple[list, list]:
    """
    Separate incoming file list into (selected_files, invalid_files).
    selected_files entries: (file_object, original_name, extension)
    """
    selected_files = []
    invalid_files  = []
    for incoming_file in incoming_files:
        raw_name = (incoming_file.filename or "").strip()
        if not raw_name:
            continue
        original_name = Path(raw_name).name
        extension     = Path(original_name).suffix.lower()
        if extension not in ALLOWED_UPLOAD_EXTENSIONS:
            invalid_files.append(original_name)
        else:
            selected_files.append((incoming_file, original_name, extension))
    return selected_files, invalid_files


def _iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_processing_metadata(stage: str, message: str, **extra_fields) -> dict:
    metadata = {
        "processing": {
            "stage": str(stage or "").strip() or "pending",
            "message": str(message or "").strip() or "Processing document...",
            "updated_at": _iso_utc_now(),
        },
    }
    processing = metadata["processing"]
    for key, value in extra_fields.items():
        if value is not None:
            processing[key] = value
    return metadata


def _build_pending_payload_with_processing(document_id, stage: str, message: str, **extra_fields) -> dict:
    payload = build_pending_extraction_payload(document_id=document_id)
    payload["metadata"] = _build_processing_metadata(stage=stage, message=message, **extra_fields)
    return payload


def _insert_document_and_mark_pending(cur, user_id, user_upload_dir, incoming_file, original_name, extension, conversation_id=None) -> dict:
    """
    Save one file to disk, insert the documents row, mark extraction as pending,
    and return the upload result dict plus parse job metadata.
    Raises ValueError on empty file.
    """
    stored_filename = f"{secrets.token_hex(16)}{extension}"
    destination     = user_upload_dir / stored_filename

    file_size_bytes, file_hash_hex = _save_uploaded_file(incoming_file, destination)
    if file_size_bytes <= 0:
        destination.unlink(missing_ok=True)
        raise ValueError(f"File is empty: {original_name}")

    guessed_mime, _ = mimetypes.guess_type(original_name)
    mime_type = (incoming_file.mimetype or guessed_mime or "application/octet-stream")[:100]

    cur.execute(
        """
        INSERT INTO documents (
            user_id, original_filename, stored_filename, storage_path,
            mime_type, file_extension, file_size_bytes, file_hash
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING document_id, created_at
        """,
        (
            user_id,
            original_name[:255],
            stored_filename,
            str(user_upload_dir),
            mime_type,
            extension.lstrip(".")[:20],
            file_size_bytes,
            file_hash_hex,
        ),
    )
    document_row = cur.fetchone()
    document_id  = document_row[0]

    pending_payload = _build_pending_payload_with_processing(
        document_id=document_id,
        stage="queued",
        message="Queued for background parsing.",
    )
    save_document_extraction(
        cur,
        document_id=document_id,
        extraction_payload=pending_payload,
    )

    return {
        "document_id":       str(document_id),
        "original_filename": original_name,
        "stored_filename":   stored_filename,
        "file_size_bytes":   file_size_bytes,
        "mime_type":         mime_type,
        "file_extension":    extension.lstrip("."),
        "upload_path":       f"{user_id}/{stored_filename}",
        "created_at":        document_row[1].isoformat() if document_row[1] else None,
        "parser_status":     "pending",
        "parser_progress":   pending_payload.get("metadata", {}).get("processing"),
        "_parse_job": {
            "document_id":       document_id,
            "destination":       destination,
            "mime_type":         mime_type,
            "original_filename": original_name,
            "conversation_id":   conversation_id,
            "user_id":           user_id,
        },
        "_destination":      destination,   # kept for rollback cleanup; stripped before response
    }


def _run_document_parse_job(job: dict) -> None:
    document_id = job["document_id"]
    destination = Path(job["destination"])
    mime_type = job.get("mime_type")
    original_filename = job.get("original_filename")
    conversation_id = job.get("conversation_id")
    user_id = job.get("user_id")

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            last_progress_state: tuple[str, str] = ("", "")
            last_progress_save_at = 0.0

            def persist_pending_progress(stage: str, message: str, force: bool = False, **extra_fields) -> None:
                nonlocal last_progress_state, last_progress_save_at
                normalized_stage = str(stage or "").strip().lower() or "processing"
                normalized_message = str(message or "").strip() or "Processing document..."
                now = time.monotonic()
                current_state = (normalized_stage, normalized_message)
                if not force and current_state == last_progress_state and (now - last_progress_save_at) < 2.0:
                    return

                pending_payload = _build_pending_payload_with_processing(
                    document_id=document_id,
                    stage=normalized_stage,
                    message=normalized_message,
                    **extra_fields,
                )
                save_document_extraction(
                    cur,
                    document_id=document_id,
                    extraction_payload=pending_payload,
                )
                conn.commit()
                last_progress_state = current_state
                last_progress_save_at = now

            persist_pending_progress(
                stage="starting",
                message="Starting background parser.",
                force=True,
            )

            def on_progress(progress_payload: dict | None) -> None:
                if not isinstance(progress_payload, dict):
                    return
                stage = progress_payload.get("stage") or "processing"
                message = progress_payload.get("message") or "Processing document..."
                provider = progress_payload.get("provider") or "parser"
                provider_state = progress_payload.get("provider_state") or ""
                batch_id = progress_payload.get("batch_id") or ""
                poll_attempt = progress_payload.get("poll_attempt")
                progress_percent = progress_payload.get("progress_percent")

                # Real-time visibility in terminal while background parsing runs.
                progress_line = (
                    f"[parse-progress] doc={document_id} stage={stage} "
                    f"provider={provider} state={provider_state or '-'} "
                    f"batch={batch_id or '-'} poll={poll_attempt if poll_attempt is not None else '-'} "
                    f"percent={progress_percent if progress_percent is not None else '-'} "
                    f"msg={message}"
                )
                print(progress_line, flush=True)
                logger.info(progress_line)

                extra_fields = {}
                for key in (
                    "provider",
                    "provider_state",
                    "batch_id",
                    "task_id",
                    "progress_percent",
                    "poll_attempt",
                ):
                    value = progress_payload.get(key)
                    if value is not None:
                        extra_fields[key] = value
                persist_pending_progress(
                    stage=stage,
                    message=message,
                    force=bool(progress_payload.get("force")),
                    **extra_fields,
                )

            parser_result = parse_document(
                file_path=destination,
                document_id=document_id,
                mime_type=mime_type,
                original_filename=original_filename,
                progress_callback=on_progress,
            )
            parser_metadata = parser_result.get("metadata") or {}
            parser_result["metadata"] = {
                **parser_metadata,
                "processing": None,
            }
            extraction_payload = build_extraction_payload(
                document_id=document_id,
                parser_result=parser_result,
                conversation_id=conversation_id,
            )
            save_document_extraction(
                cur,
                document_id=document_id,
                extraction_payload=extraction_payload,
            )
            parsed_file_type = str(parser_result.get("file_type") or "").strip().lower()
            if parsed_file_type in {"png", "jpg", "jpeg", "webp"}:
                try:
                    prompt_profiles = get_prompt_profiles_for_user(cur, user_id) if user_id else {}
                    run_diagram_analysis_for_document(
                        cur,
                        document_id=str(document_id),
                        prompt_override=prompt_profiles.get(PROMPT_TYPE_VISION, ""),
                    )
                except Exception:
                    logger.exception("Auto vision analysis failed for uploaded image document_id=%s", document_id)
            _maybe_refresh_conversation_title(cur, str(conversation_id or ""))
        conn.commit()
        _schedule_embedding_autorun(f"document_parse:{document_id}")
        _schedule_summary_autorun(f"document_parse:{document_id}")
    except Exception as exc:
        logger.exception("Background parse failed for document_id=%s", document_id)
        if conn is not None:
            conn.rollback()
            conn.close()
            conn = None

        # Best effort: mark extraction as failed instead of leaving it pending forever.
        fallback_conn = None
        try:
            fallback_conn = get_db_connection()
            with fallback_conn.cursor() as fallback_cur:
                failed_result = {
                    "document_id": str(document_id),
                    "file_type": None,
                    "metadata": {"source_path": str(destination)},
                    "segments": [],
                    "errors": [{
                        "code": "background_parse_error",
                        "message": "Unable to parse document in background worker.",
                        "details": {"exception": str(exc)},
                    }],
                }
                failed_payload = build_extraction_payload(
                    document_id=document_id,
                    parser_result=failed_result,
                    conversation_id=conversation_id,
                )
                save_document_extraction(
                    fallback_cur,
                    document_id=document_id,
                    extraction_payload=failed_payload,
                )
                _maybe_refresh_conversation_title(fallback_cur, str(conversation_id or ""))
            fallback_conn.commit()
        except Exception:
            if fallback_conn is not None:
                fallback_conn.rollback()
            logger.exception("Failed to persist background parse failure for document_id=%s", document_id)
        finally:
            if fallback_conn is not None:
                fallback_conn.close()
    finally:
        if conn is not None:
            conn.close()


def _start_background_parse_jobs(parse_jobs: list[dict]) -> None:
    for job in parse_jobs:
        document_parse_executor.submit(_run_document_parse_job, job)


@app.route('/api/documents/upload', methods=['POST'])
def upload_documents():
    """Upload one or more documents and create a new conversation."""
    user_id = session.get("user_id")
    if not user_id:
        return jsonify({'error': 'You must be logged in to upload documents.'}), 401

    selected_files, invalid_files = _validate_uploaded_files(request.files.getlist("documents"))

    if invalid_files:
        return jsonify({
            'error':         f"Unsupported file format. Allowed formats: {_allowed_upload_extensions_text()}",
            'invalid_files': invalid_files,
        }), 400
    if not selected_files:
        return jsonify({'error': 'Please select at least one valid file to upload.'}), 400

    user_upload_dir = UPLOADS_DIR / user_id
    user_upload_dir.mkdir(parents=True, exist_ok=True)

    conn = None
    saved_paths = []
    parse_jobs = []
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            title = _compose_upload_conversation_title([name for _, name, _ in selected_files])
            cur.execute(
                "INSERT INTO conversations (user_id, title) VALUES (%s, %s) RETURNING conversation_id",
                (user_id, title),
            )
            conversation_id = cur.fetchone()[0]

            uploaded_documents = []
            for incoming_file, original_name, extension in selected_files:
                result = _insert_document_and_mark_pending(
                    cur, user_id, user_upload_dir, incoming_file, original_name, extension, conversation_id=conversation_id
                )
                parse_jobs.append(result.pop("_parse_job"))
                saved_paths.append(result.pop("_destination"))
                cur.execute(
                    "INSERT INTO conversation_documents (conversation_id, document_id) VALUES (%s, %s)",
                    (conversation_id, result["document_id"]),
                )
                uploaded_documents.append(result)

        conn.commit()
        _start_background_parse_jobs(parse_jobs)
        conversation_url = f"/chat?conversation_id={quote(str(conversation_id))}&new=1"
        return jsonify({
            'message':          f"Uploaded {len(uploaded_documents)} file(s). Documents are processing in the background.",
            'conversation':     {"conversation_id": str(conversation_id), "title": title},
            'conversation_url': conversation_url,
            'documents':        uploaded_documents,
        }), 201
    except ValueError as e:
        if conn is not None:
            conn.rollback()
        for p in saved_paths:
            p.unlink(missing_ok=True)
        return jsonify({'error': str(e)}), 400
    except Exception:
        if conn is not None:
            conn.rollback()
        for p in saved_paths:
            p.unlink(missing_ok=True)
        return jsonify({'error': 'Unable to upload documents right now.'}), 500
    finally:
        if conn is not None:
            conn.close()


@app.route('/api/conversations/<conversation_id>/documents/upload', methods=['POST'])
def upload_documents_to_conversation(conversation_id):
    """Upload one or more documents into an existing conversation."""
    user_id         = session.get("user_id")
    conversation_id = (conversation_id or "").strip()

    if not user_id:
        return jsonify({'error': 'You must be logged in to upload documents.'}), 401
    if not conversation_id:
        return jsonify({'error': 'Conversation ID is required.'}), 400

    selected_files, invalid_files = _validate_uploaded_files(request.files.getlist("documents"))

    if invalid_files:
        return jsonify({
            'error':         f"Unsupported file format. Allowed formats: {_allowed_upload_extensions_text()}",
            'invalid_files': invalid_files,
        }), 400
    if not selected_files:
        return jsonify({'error': 'Please select at least one valid file to upload.'}), 400

    user_upload_dir = UPLOADS_DIR / user_id
    user_upload_dir.mkdir(parents=True, exist_ok=True)

    conn = None
    saved_paths = []
    parse_jobs = []
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM conversations WHERE conversation_id = %s AND user_id = %s",
                (conversation_id, user_id),
            )
            if not cur.fetchone():
                return jsonify({'error': 'Conversation not found.'}), 404

            uploaded_documents = []
            for incoming_file, original_name, extension in selected_files:
                result = _insert_document_and_mark_pending(
                    cur, user_id, user_upload_dir, incoming_file, original_name, extension, conversation_id=conversation_id
                )
                parse_jobs.append(result.pop("_parse_job"))
                saved_paths.append(result.pop("_destination"))
                cur.execute(
                    "INSERT INTO conversation_documents (conversation_id, document_id) VALUES (%s, %s)",
                    (conversation_id, result["document_id"]),
                )
                uploaded_documents.append(result)

            cur.execute(
                "UPDATE conversations SET updated_at = CURRENT_TIMESTAMP WHERE conversation_id = %s",
                (conversation_id,),
            )

        conn.commit()
        _start_background_parse_jobs(parse_jobs)
        return jsonify({
            'message':      f"Uploaded {len(uploaded_documents)} file(s). Documents are processing in the background.",
            'conversation': {"conversation_id": conversation_id},
            'documents':    uploaded_documents,
        }), 201
    except ValueError as e:
        if conn is not None:
            conn.rollback()
        for p in saved_paths:
            p.unlink(missing_ok=True)
        return jsonify({'error': str(e)}), 400
    except Exception:
        if conn is not None:
            conn.rollback()
        for p in saved_paths:
            p.unlink(missing_ok=True)
        return jsonify({'error': 'Unable to upload documents right now.'}), 500
    finally:
        if conn is not None:
            conn.close()


@app.route('/api/conversations/<conversation_id>/documents', methods=['GET'])
def api_conversation_documents(conversation_id):
    user_id = session.get("user_id")
    conversation_id = (conversation_id or "").strip()

    if not user_id:
        return jsonify({'error': 'You must be logged in.'}), 401
    if not conversation_id:
        return jsonify({'error': 'Conversation ID is required.'}), 400

    if not conversation_exists_for_user(user_id, conversation_id):
        return jsonify({'error': 'Conversation not found.'}), 404

    documents = get_conversation_documents(user_id, conversation_id)
    return jsonify({
        "conversation_id": conversation_id,
        "documents": documents,
    }), 200


@app.route('/api/conversations/<conversation_id>/documents/<document_id>', methods=['PATCH'])
def api_update_conversation_document(conversation_id, document_id):
    user_id = session.get("user_id")
    conversation_id = (conversation_id or "").strip()
    document_id = (document_id or "").strip()

    if not user_id:
        return jsonify({'error': 'You must be logged in.'}), 401
    if not conversation_id:
        return jsonify({'error': 'Conversation ID is required.'}), 400
    if not document_id:
        return jsonify({'error': 'Document ID is required.'}), 400
    if not conversation_exists_for_user(user_id, conversation_id):
        return jsonify({'error': 'Conversation not found.'}), 404

    payload = request.get_json(silent=True) or {}
    original_filename = str(payload.get("original_filename") or "").strip()
    if not original_filename:
        return jsonify({'error': 'Document name is required.'}), 400

    original_filename = original_filename[:255]

    conn = None
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE documents d
                    SET original_filename = %s
                    FROM conversation_documents cd
                    JOIN conversations c ON c.conversation_id = cd.conversation_id
                    WHERE d.document_id = cd.document_id
                      AND c.conversation_id = %s
                      AND c.user_id = %s
                      AND d.document_id = %s
                      AND d.is_deleted = FALSE
                    RETURNING d.document_id
                    """,
                    (original_filename, conversation_id, user_id, document_id),
                )
                updated_row = cur.fetchone()

        if not updated_row:
            return jsonify({'error': 'Document not found.'}), 404

        document_payload = get_conversation_document_record(user_id, conversation_id, document_id)
        if not document_payload:
            return jsonify({'error': 'Document not found after update.'}), 404

        return jsonify({
            "message": "Document renamed.",
            "document": document_payload,
        }), 200
    except Exception:
        logger.exception(
            "Failed to rename document conversation_id=%s document_id=%s",
            conversation_id,
            document_id,
        )
        return jsonify({'error': 'Unable to rename document right now.'}), 500
    finally:
        if conn is not None:
            conn.close()


@app.route('/api/conversations/<conversation_id>/documents/<document_id>', methods=['DELETE'])
def api_soft_delete_conversation_document(conversation_id, document_id):
    user_id = session.get("user_id")
    conversation_id = (conversation_id or "").strip()
    document_id = (document_id or "").strip()

    if not user_id:
        return jsonify({'error': 'You must be logged in.'}), 401
    if not conversation_id:
        return jsonify({'error': 'Conversation ID is required.'}), 400
    if not document_id:
        return jsonify({'error': 'Document ID is required.'}), 400
    if not conversation_exists_for_user(user_id, conversation_id):
        return jsonify({'error': 'Conversation not found.'}), 404

    conn = None
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE documents d
                    SET is_deleted = TRUE,
                        deleted_at = CURRENT_TIMESTAMP
                    FROM conversation_documents cd
                    JOIN conversations c ON c.conversation_id = cd.conversation_id
                    WHERE d.document_id = cd.document_id
                      AND c.conversation_id = %s
                      AND c.user_id = %s
                      AND d.document_id = %s
                      AND d.is_deleted = FALSE
                    RETURNING d.document_id
                    """,
                    (conversation_id, user_id, document_id),
                )
                deleted_row = cur.fetchone()

        if not deleted_row:
            return jsonify({'error': 'Document not found.'}), 404

        return jsonify({
            "message": "Document deleted.",
            "document_id": document_id,
        }), 200
    except Exception:
        logger.exception(
            "Failed to soft delete document conversation_id=%s document_id=%s",
            conversation_id,
            document_id,
        )
        return jsonify({'error': 'Unable to delete document right now.'}), 500
    finally:
        if conn is not None:
            conn.close()


@app.route('/api/documents/<document_id>/summary', methods=['GET'])
def api_document_summary(document_id):
    user_id = session.get("user_id")
    if not user_id:
        return jsonify({'error': 'You must be logged in.'}), 401

    conversation_id = (request.args.get("conversation_id") or "").strip() or None
    summary_payload = get_document_summary(user_id, document_id, conversation_id=conversation_id)
    if not summary_payload:
        return jsonify({'error': 'Document summary not found.'}), 404
    return jsonify(summary_payload), 200


@app.route('/api/conversations/<conversation_id>/summary', methods=['GET'])
def api_conversation_summary(conversation_id):
    user_id = session.get("user_id")
    conversation_id = (conversation_id or "").strip()
    if not user_id:
        return jsonify({'error': 'You must be logged in.'}), 401
    if not conversation_id:
        return jsonify({'error': 'Conversation ID is required.'}), 400
    if not conversation_exists_for_user(user_id, conversation_id):
        return jsonify({'error': 'Conversation not found.'}), 404

    summary_payload = get_conversation_summary(user_id, conversation_id)
    if not summary_payload:
        return jsonify({'error': 'Conversation summary not found.'}), 404
    return jsonify(summary_payload), 200


@app.route('/api/conversations/<conversation_id>/retrieve', methods=['POST'])
def api_conversation_retrieve(conversation_id):
    user_id = session.get("user_id")
    conversation_id = (conversation_id or "").strip()
    if not user_id:
        return jsonify({'error': 'You must be logged in.'}), 401
    if not conversation_id:
        return jsonify({'error': 'Conversation ID is required.'}), 400
    if not conversation_exists_for_user(user_id, conversation_id):
        return jsonify({'error': 'Conversation not found.'}), 404

    payload = request.get_json(silent=True) or {}
    query = str(payload.get("query") or "").strip()
    if not query:
        return jsonify({'error': 'query is required.'}), 400

    started_at = time.perf_counter()
    try:
        retrieval_payload = RetrievalService().retrieve_conversation_blocks(
            user_id=user_id,
            conversation_id=conversation_id,
            query=query,
            k=payload.get("k"),
            document_ids=payload.get("document_ids"),
        )
        retrieval_payload["timing_ms"] = round((time.perf_counter() - started_at) * 1000.0, 2)
        return jsonify(retrieval_payload), 200
    except RetrievalServiceError as exc:
        return jsonify({
            'error': exc.message,
            'details': exc.to_dict(),
        }), exc.status_code
    except EmbeddingServiceError as exc:
        logger.exception("Conversation retrieval embedding failed conversation_id=%s", conversation_id)
        return jsonify({
            'error': 'Could not embed retrieval query.',
            'details': exc.to_dict(),
        }), 503
    except Exception as exc:
        logger.exception("Conversation retrieval failed conversation_id=%s", conversation_id)
        error_payload = {'error': 'Unable to retrieve context right now.'}
        if app.debug:
            error_payload['details'] = {
                'exception_type': type(exc).__name__,
                'message': str(exc),
            }
        return jsonify(error_payload), 500


@app.route('/api/conversations/<conversation_id>/messages', methods=['POST'])
def api_conversation_messages(conversation_id):
    user_id = session.get("user_id")
    conversation_id = (conversation_id or "").strip()
    if not user_id:
        return jsonify({'error': 'You must be logged in.'}), 401
    if not conversation_id:
        return jsonify({'error': 'Conversation ID is required.'}), 400
    if not conversation_exists_for_user(user_id, conversation_id):
        return jsonify({'error': 'Conversation not found.'}), 404

    payload = request.get_json(silent=True) or {}
    edit_message_id = str(payload.get("edit_message_id") or "").strip()
    regenerate_message_id = str(payload.get("regenerate_message_id") or "").strip()
    if edit_message_id and regenerate_message_id:
        return jsonify({'error': 'Choose edit or regenerate, not both.'}), 400

    query = str(payload.get("query") or "").strip()
    if not query and not regenerate_message_id:
        return jsonify({'error': 'query is required.'}), 400

    try:
        service = ChatAnswerService()
        if edit_message_id:
            response_payload = service.replay_conversation_query(
                user_id=user_id,
                conversation_id=conversation_id,
                target_message_id=edit_message_id,
                mode="edit",
                query=query,
                document_ids=payload.get("document_ids"),
                k=payload.get("k"),
                include_filtered=payload.get("include_filtered"),
            )
        elif regenerate_message_id:
            response_payload = service.replay_conversation_query(
                user_id=user_id,
                conversation_id=conversation_id,
                target_message_id=regenerate_message_id,
                mode="regenerate",
                query=query,
                document_ids=payload.get("document_ids"),
                k=payload.get("k"),
                include_filtered=payload.get("include_filtered"),
            )
        else:
            response_payload = service.answer_conversation_query(
                user_id=user_id,
                conversation_id=conversation_id,
                query=query,
                document_ids=payload.get("document_ids"),
                k=payload.get("k"),
                include_filtered=payload.get("include_filtered"),
            )
        response_payload["conversation_messages"] = get_conversation_messages(user_id, conversation_id)
        return jsonify(response_payload), 200
    except ChatAnswerServiceError as exc:
        return jsonify({
            'error': exc.message,
            'details': exc.to_dict(),
        }), exc.status_code
    except RetrievalServiceError as exc:
        return jsonify({
            'error': exc.message,
            'details': exc.to_dict(),
        }), exc.status_code
    except EmbeddingServiceError as exc:
        logger.exception("Conversation message embedding failed conversation_id=%s", conversation_id)
        return jsonify({
            'error': 'Could not embed the query for retrieval.',
            'details': exc.to_dict(),
        }), 503
    except Exception as exc:
        logger.exception("Conversation message generation failed conversation_id=%s", conversation_id)
        error_payload = {'error': 'Unable to send your message right now.'}
        if app.debug:
            error_payload['details'] = {
                'exception_type': type(exc).__name__,
                'message': str(exc),
            }
        return jsonify(error_payload), 500


@app.route('/api/conversations/<conversation_id>/message-versions/select', methods=['POST'])
def api_select_conversation_message_version(conversation_id):
    user_id = session.get("user_id")
    conversation_id = (conversation_id or "").strip()
    if not user_id:
        return jsonify({'error': 'You must be logged in.'}), 401
    if not conversation_id:
        return jsonify({'error': 'Conversation ID is required.'}), 400
    if not conversation_exists_for_user(user_id, conversation_id):
        return jsonify({'error': 'Conversation not found.'}), 404

    payload = request.get_json(silent=True) or {}
    family_id = str(payload.get("family_id") or "").strip()
    role = str(payload.get("role") or "").strip().lower()
    version_number = payload.get("version_number")
    if not family_id:
        return jsonify({'error': 'family_id is required.'}), 400
    try:
        version_number = int(version_number)
    except (TypeError, ValueError):
        return jsonify({'error': 'version_number must be an integer.'}), 400

    try:
        selection_payload = ChatAnswerService().select_family_version(
            user_id=user_id,
            conversation_id=conversation_id,
            family_id=family_id,
            role=role,
            version_number=version_number,
        )
        return jsonify({
            **selection_payload,
            'conversation_messages': get_conversation_messages(user_id, conversation_id),
        }), 200
    except ChatAnswerServiceError as exc:
        return jsonify({
            'error': exc.message,
            'details': exc.to_dict(),
        }), exc.status_code
    except Exception as exc:
        logger.exception("Conversation version selection failed conversation_id=%s", conversation_id)
        error_payload = {'error': 'Unable to switch message version right now.'}
        if app.debug:
            error_payload['details'] = {
                'exception_type': type(exc).__name__,
                'message': str(exc),
            }
        return jsonify(error_payload), 500


@app.route('/api/documents/<document_id>/parser-results', methods=['GET'])
def api_document_parser_results(document_id):
    user_id = session.get("user_id")
    if not user_id:
        return jsonify({'error': 'You must be logged in to view parser results.'}), 401

    conversation_id = (request.args.get("conversation_id") or "").strip() or None
    document_result = get_document_parser_result(
        user_id=user_id, document_id=document_id, conversation_id=conversation_id,
    )
    if not document_result:
        return jsonify({'error': 'Document not found.'}), 404

    return jsonify(_build_parser_review_payload(document_result)), 200


@app.route('/api/documents/<document_id>/parser-review', methods=['POST'])
def api_save_document_parser_review(document_id):
    user_id = session.get("user_id")
    if not user_id:
        return jsonify({'error': 'You must be logged in to edit parser results.'}), 401

    conversation_id = (request.args.get("conversation_id") or "").strip() or None
    payload = request.get_json(silent=True) or {}
    edited_blocks = payload.get("blocks")
    if not isinstance(edited_blocks, list) or not edited_blocks:
        return jsonify({'error': 'Edited blocks are required.'}), 400

    document_result = get_document_parser_result(
        user_id=user_id,
        document_id=document_id,
        conversation_id=conversation_id,
    )
    if not document_result:
        return jsonify({'error': 'Document not found.'}), 404

    parser_result = document_result.get("parser_result") or {}
    existing_blocks = {
        str(block.get("block_id")): deepcopy(block)
        for block in (parser_result.get("document_blocks") or [])
        if block.get("block_id")
    }
    if not existing_blocks:
        return jsonify({'error': 'No editable parser blocks are available for this document.'}), 400

    diagram_details_by_block = {
        str(detail.get("block_id")): deepcopy(detail)
        for detail in (parser_result.get("diagram_block_details") or [])
        if detail.get("block_id")
    }
    original_block_snapshots = {
        block_id: _build_review_persist_snapshot(
            block,
            diagram_details_by_block.get(block_id),
        )
        for block_id, block in existing_blocks.items()
    }

    requested_block_ids: set[str] = set()
    review_timestamp = datetime.now(timezone.utc).isoformat()
    for edited_block in edited_blocks:
        block_id = str((edited_block or {}).get("block_id") or "").strip()
        existing_block = existing_blocks.get(block_id)
        if not existing_block:
            continue

        block_type = str(existing_block.get("block_type") or "").lower()
        normalized = dict(existing_block.get("normalized_content") or {})

        if block_type == "text":
            text_value = _normalize_review_text(
                (edited_block.get("normalized_content") or {}).get("text_content")
                or edited_block.get("display_text")
                or ""
            )
            current_text_value = _normalize_review_text(
                normalized.get("text_content") or normalized.get("normalized_text") or existing_block.get("display_text") or ""
            )
            if text_value == current_text_value:
                continue
            requested_block_ids.add(block_id)
            source_metadata = dict(existing_block.get("source_metadata") or {})
            source_metadata["manual_review"] = {
                "updated_at": review_timestamp,
                "updated_from": "document_parser_results",
            }
            existing_block["source_metadata"] = source_metadata
            normalized["text_content"] = text_value
            normalized["normalized_text"] = text_value
            existing_block["display_text"] = text_value
        elif block_type == "table":
            edited_normalized = edited_block.get("normalized_content") or {}
            title = _normalize_inline_text(edited_normalized.get("title"))
            caption = _normalize_review_text(
                edited_normalized.get("caption")
                or edited_block.get("caption_text")
                or ""
            )
            matrix = _normalize_matrix(edited_normalized.get("matrix") or [])
            footnotes = [
                _normalize_review_text(item)
                for item in (edited_normalized.get("footnotes") or [])
                if _normalize_review_text(item)
            ]
            current_title = _normalize_inline_text(normalized.get("title"))
            current_caption = _normalize_review_text(normalized.get("caption") or existing_block.get("caption_text") or "")
            current_matrix = _normalize_matrix(normalized.get("matrix") or [])
            current_footnotes = [
                _normalize_review_text(item)
                for item in (normalized.get("footnotes") or [])
                if _normalize_review_text(item)
            ]
            if title == current_title and caption == current_caption and matrix == current_matrix and footnotes == current_footnotes:
                continue
            requested_block_ids.add(block_id)
            source_metadata = dict(existing_block.get("source_metadata") or {})
            source_metadata["manual_review"] = {
                "updated_at": review_timestamp,
                "updated_from": "document_parser_results",
            }
            existing_block["source_metadata"] = source_metadata
            normalized["title"] = title
            normalized["caption"] = caption
            normalized["matrix"] = matrix
            normalized["footnotes"] = footnotes
            existing_block["caption_text"] = normalized.get("caption") or None
            existing_block["display_text"] = existing_block["caption_text"] or existing_block.get("display_text")
        elif block_type == "diagram":
            edited_normalized = edited_block.get("normalized_content") or {}
            caption_text = _normalize_review_text(
                edited_block.get("caption_text")
                or existing_block.get("caption_text")
                or existing_block.get("display_text")
                or ""
            )
            normalized_diagram_fields = _normalize_diagram_review_fields(
                edited_normalized.get("visual_description"),
                edited_normalized.get("question_answerable_facts") or [],
            )
            visual_description = normalized_diagram_fields["visual_description"]
            current_diagram_detail = dict(diagram_details_by_block.get(block_id) or {})
            current_normalized_diagram_fields = _normalize_diagram_review_fields(
                current_diagram_detail.get("visual_description") or normalized.get("visual_description") or "",
                current_diagram_detail.get("question_answerable_facts") or normalized.get("question_answerable_facts") or [],
            )
            current_visual_description = current_normalized_diagram_fields["visual_description"]
            current_caption_text = _normalize_review_text(
                existing_block.get("caption_text") or existing_block.get("display_text") or ""
            )
            current_ocr_text = current_diagram_detail.get("ocr_text") or normalized.get("ocr_text") or []
            if not isinstance(current_ocr_text, list):
                current_ocr_text = []
            current_semantic_links = current_diagram_detail.get("semantic_links") or normalized.get("semantic_links") or []
            if not isinstance(current_semantic_links, list):
                current_semantic_links = []
            current_question_answerable_facts = current_normalized_diagram_fields["question_answerable_facts"]
            current_storage_path = str(current_diagram_detail.get("storage_path") or edited_normalized.get("storage_path") or "")
            next_ocr_text = edited_normalized.get("ocr_text") or []
            if not isinstance(next_ocr_text, list):
                next_ocr_text = []
            next_semantic_links = edited_normalized.get("semantic_links") or []
            if not isinstance(next_semantic_links, list):
                next_semantic_links = []
            next_question_answerable_facts = normalized_diagram_fields["question_answerable_facts"]
            next_storage_path = str(edited_normalized.get("storage_path") or "")
            if (
                caption_text == current_caption_text
                and visual_description == current_visual_description
                and next_ocr_text == current_ocr_text
                and next_semantic_links == current_semantic_links
                and next_question_answerable_facts == current_question_answerable_facts
                and next_storage_path == current_storage_path
            ):
                continue
            requested_block_ids.add(block_id)
            source_metadata = dict(existing_block.get("source_metadata") or {})
            source_metadata["manual_review"] = {
                "updated_at": review_timestamp,
                "updated_from": "document_parser_results",
            }
            existing_block["source_metadata"] = source_metadata
            normalized["visual_description"] = visual_description
            normalized["question_answerable_facts"] = next_question_answerable_facts
            existing_block["caption_text"] = caption_text or None
            existing_block["display_text"] = caption_text or existing_block.get("display_text")

            diagram_detail = dict(diagram_details_by_block.get(block_id) or {})
            if not diagram_detail:
                diagram_detail = {
                        "block_id": block_id,
                        "diagram_kind": normalized.get("diagram_kind") or existing_block.get("subtype") or "unknown",
                        "ocr_text": next_ocr_text,
                        "semantic_links": next_semantic_links,
                        "question_answerable_facts": next_question_answerable_facts,
                        "storage_path": edited_normalized.get("storage_path") or "",
                        "vision_status": "completed",
                    }
            diagram_detail["visual_description"] = visual_description
            diagram_detail["question_answerable_facts"] = next_question_answerable_facts
            diagram_detail["vision_status"] = "completed"
            diagram_details_by_block[block_id] = diagram_detail
        else:
            continue

        existing_block["normalized_content"] = normalized
    if not requested_block_ids:
        return jsonify({'error': 'No matching editable blocks were provided.'}), 400

    _refresh_review_block_content(existing_blocks, diagram_details_by_block)
    directly_changed_block_ids = {
        block_id
        for block_id in requested_block_ids
        if _build_review_persist_snapshot(
            existing_blocks.get(block_id) or {},
            diagram_details_by_block.get(block_id),
        ) != original_block_snapshots.get(block_id)
    }
    if not directly_changed_block_ids:
        return jsonify({'error': 'No parser review changes were detected.'}), 400

    impacted_block_ids = _get_review_impacted_block_ids(existing_blocks, directly_changed_block_ids)
    impacted_blocks = {block_id: existing_blocks[block_id] for block_id in impacted_block_ids if block_id in existing_blocks}
    _refresh_review_block_content(impacted_blocks, diagram_details_by_block)

    modified_block_ids = {
        block_id
        for block_id in impacted_block_ids
        if _build_review_persist_snapshot(
            existing_blocks.get(block_id) or {},
            diagram_details_by_block.get(block_id),
        ) != original_block_snapshots.get(block_id)
    }
    modified_count = len(modified_block_ids)
    if modified_count == 0:
        return jsonify({'error': 'No parser review changes were detected.'}), 400

    sorted_blocks = sorted(existing_blocks.values(), key=_review_block_sort_key)
    markdown_output = _build_review_markdown(sorted_blocks, diagram_details_by_block)

    segment_text_updates = {}
    for block in sorted_blocks:
        block_id = str(block.get("block_id") or "")
        if block_id not in modified_block_ids:
            continue
        block_type = str(block.get("block_type") or "").lower()
        normalized = block.get("normalized_content") or {}
        if block_type == "text":
            segment_text = _normalize_review_text(normalized.get("normalized_text") or block.get("display_text") or "")
        elif block_type == "table":
            segment_text = _normalize_review_text(normalized.get("linearized_text") or normalized.get("retrieval_text") or "")
        elif block_type == "diagram":
            segment_text = _normalize_review_text(normalized.get("visual_description") or block.get("caption_text") or "")
        else:
            segment_text = ""

        if not segment_text:
            continue
        for segment_id in _collect_segment_ids_from_block(block):
            segment_text_updates[segment_id] = segment_text

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            for block in sorted_blocks:
                block_id = str(block.get("block_id") or "")
                if block_id not in modified_block_ids:
                    continue
                cur.execute(
                    """
                    UPDATE document_blocks
                    SET
                        subtype = %s,
                        normalized_content = %s::jsonb,
                        display_text = %s,
                        caption_text = %s,
                        source_metadata = %s::jsonb,
                        embedding_status = %s,
                        processing_status = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE document_id = %s
                      AND block_id = %s
                    """,
                    (
                        block.get("subtype"),
                        Json(block.get("normalized_content") or {}),
                        block.get("display_text"),
                        block.get("caption_text"),
                        Json(block.get("source_metadata") or {}),
                        block.get("embedding_status"),
                        block.get("processing_status"),
                        document_id,
                        block_id,
                    ),
                )

            if _relation_exists(cur, "diagram_block_details"):
                for block_id, detail in diagram_details_by_block.items():
                    if str(block_id) not in modified_block_ids:
                        continue
                    block = existing_blocks.get(block_id) or {}
                    normalized = block.get("normalized_content") or {}
                    cur.execute(
                        """
                        INSERT INTO diagram_block_details (
                            block_id,
                            image_asset_id,
                            diagram_kind,
                            image_region,
                            ocr_text,
                            visual_description,
                            semantic_links,
                            question_answerable_facts,
                            vision_status,
                            last_analyzed_at,
                            updated_at
                        )
                        VALUES (
                            %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s::jsonb, %s::jsonb, 'completed', NOW(), NOW()
                        )
                        ON CONFLICT (block_id)
                        DO UPDATE SET
                            image_asset_id = COALESCE(EXCLUDED.image_asset_id, diagram_block_details.image_asset_id),
                            diagram_kind = EXCLUDED.diagram_kind,
                            image_region = EXCLUDED.image_region,
                            ocr_text = EXCLUDED.ocr_text,
                            visual_description = EXCLUDED.visual_description,
                            semantic_links = EXCLUDED.semantic_links,
                            question_answerable_facts = EXCLUDED.question_answerable_facts,
                            vision_status = 'completed',
                            last_analyzed_at = NOW(),
                            updated_at = NOW()
                        """,
                        (
                            block_id,
                            normalized.get("image_asset_id"),
                            detail.get("diagram_kind") or normalized.get("diagram_kind") or block.get("subtype") or "unknown",
                            Json(normalized.get("image_region") or {}),
                            Json(detail.get("ocr_text") or normalized.get("ocr_text") or []),
                            detail.get("visual_description"),
                            Json(detail.get("semantic_links") or normalized.get("semantic_links") or []),
                            Json(detail.get("question_answerable_facts") or []),
                        ),
                    )

            for segment_id, segment_text in segment_text_updates.items():
                cur.execute(
                    """
                    UPDATE document_extraction_segments
                    SET
                        text = %s,
                        metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb
                    WHERE document_id = %s
                      AND segment_id = %s
                    """,
                    (
                        segment_text,
                        Json({"manual_review": {"updated_at": review_timestamp}}),
                        document_id,
                        segment_id,
                    ),
                )

            extraction_metadata = dict(parser_result.get("metadata") or {})
            extraction_metadata["markdown_output"] = markdown_output
            extraction_metadata["review"] = {
                "updated_at": review_timestamp,
                "updated_from": "document_parser_results",
                "modified_blocks": modified_count,
            }
            cur.execute(
                """
                UPDATE document_extractions
                SET
                    metadata = %s::jsonb,
                    updated_at = CURRENT_TIMESTAMP
                WHERE document_id = %s
                """,
                (
                    Json(extraction_metadata),
                    document_id,
                ),
            )
            enqueue_document_summary_job(
                cur,
                document_id=str(document_id),
                conversation_id=conversation_id,
                parser_version=(parser_result.get("parser_version") or "1.0.0"),
                document_blocks=sorted_blocks,
            )
        conn.commit()
        _schedule_embedding_autorun(f"parser_review:{document_id}")
        _schedule_summary_autorun(f"parser_review:{document_id}")
    except Exception:
        if conn is not None:
            conn.rollback()
        return jsonify({'error': 'Unable to save reviewed parser content right now.'}), 500
    finally:
        if conn is not None:
            conn.close()

    refreshed_result = get_document_parser_result(
        user_id=user_id,
        document_id=document_id,
        conversation_id=conversation_id,
    )
    if not refreshed_result:
        return jsonify({'error': 'Document not found after saving review.'}), 404

    response_payload = _build_parser_review_payload(refreshed_result)
    response_payload["diagram_analysis_usage"] = get_diagram_analysis_usage_summary()
    return jsonify(response_payload), 200


def get_document_file_record(user_id, document_id, conversation_id=None) -> dict | None:
    if not user_id or not document_id:
        return None

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            if conversation_id:
                cur.execute(
                    """
                    SELECT d.document_id, d.stored_filename, d.storage_path, d.file_extension, d.mime_type
                    FROM conversations c
                    JOIN conversation_documents cd ON cd.conversation_id = c.conversation_id
                    JOIN documents d              ON d.document_id       = cd.document_id
                    WHERE c.user_id         = %s
                      AND c.conversation_id = %s
                      AND d.document_id     = %s
                      AND d.is_deleted      = FALSE
                    LIMIT 1
                    """,
                    (user_id, conversation_id, document_id),
                )
            else:
                cur.execute(
                    """
                    SELECT document_id, stored_filename, storage_path, file_extension, mime_type
                    FROM documents
                    WHERE user_id     = %s
                      AND document_id = %s
                      AND is_deleted  = FALSE
                    LIMIT 1
                    """,
                    (user_id, document_id),
                )

            row = cur.fetchone()
            if not row:
                return None

            upload_path = ""
            if row[2] and row[1]:
                upload_path = f"{Path(str(row[2])).name}/{row[1]}"

            return {
                "document_id": str(row[0]),
                "stored_filename": row[1] or "",
                "storage_path": row[2] or "",
                "file_extension": row[3] or "",
                "mime_type": row[4] or "",
                "upload_path": upload_path,
            }
    except Exception:
        return None
    finally:
        if conn is not None:
            conn.close()

    document_result["parser_result"]["document_blocks"] = sorted_blocks
    document_result["parser_result"]["diagram_block_details"] = list(diagram_details_by_block.values())
    document_result["parser_result"]["metadata"] = {
        **(parser_result.get("metadata") or {}),
        "markdown_output": markdown_output,
        "review": {
            "updated_at": review_timestamp,
            "updated_from": "document_parser_results",
            "modified_blocks": modified_count,
        },
    }
    review_document = _build_parser_review_payload(document_result)
    review_document["diagram_analysis_usage"] = get_diagram_analysis_usage_summary()
    return jsonify(review_document), 200


@app.route('/api/documents/<document_id>/diagram-analysis', methods=['POST'])
def api_run_selected_diagram_analysis(document_id):
    user_id = session.get("user_id")
    if not user_id:
        return jsonify({'error': 'You must be logged in to analyze diagrams.'}), 401

    conversation_id = (request.args.get("conversation_id") or "").strip() or None
    payload = request.get_json(silent=True) or {}
    selected_block_ids = [
        str(block_id).strip()
        for block_id in (payload.get("block_ids") or [])
        if str(block_id).strip()
    ]
    if not selected_block_ids:
        return jsonify({'error': 'At least one diagram must be selected.'}), 400

    document_result = get_document_parser_result(
        user_id=user_id,
        document_id=document_id,
        conversation_id=conversation_id,
    )
    if not document_result:
        return jsonify({'error': 'Document not found.'}), 404

    parser_result = document_result.get("parser_result") or {}
    diagram_block_ids = {
        str(block.get("block_id"))
        for block in (parser_result.get("document_blocks") or [])
        if str(block.get("block_type") or "").lower() == "diagram" and block.get("block_id")
    }
    invalid_block_ids = [block_id for block_id in selected_block_ids if block_id not in diagram_block_ids]
    if invalid_block_ids:
        return jsonify({'error': 'Some selected blocks are not valid diagram blocks.', 'invalid_block_ids': invalid_block_ids}), 400

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            if not _diagram_vision_schema_ready(cur):
                return jsonify({'error': 'Diagram analysis tables are not ready yet.'}), 400

            prompt_profiles = get_prompt_profiles_for_user(cur, user_id)
            analysis_result = run_diagram_analysis_for_document(
                cur,
                document_id=document_id,
                block_ids=selected_block_ids,
                force_analyze=True,
                prompt_override=prompt_profiles.get(PROMPT_TYPE_VISION, ''),
            )
            analyzed_block_ids = analysis_result.get("analyzed_block_ids") or []
            failed_block_ids = analysis_result.get("failed_block_ids") or []
            exhausted_block_ids = analysis_result.get("exhausted_block_ids") or []
            all_models_exhausted = bool(analysis_result.get("all_models_exhausted"))

            if selected_block_ids and not analyzed_block_ids:
                last_error = _load_latest_diagram_analysis_error(cur, selected_block_ids)
                conn.commit()
                if last_error:
                    payload, status_code = _build_diagram_analysis_failure_payload(RuntimeError(last_error))
                else:
                    payload, status_code = ({
                        'error': 'No selected diagrams could be analyzed right now.',
                        'error_type': 'analysis_failed',
                        'fallback_action': 'copy_image_and_prompt' if all_models_exhausted else None,
                        'usage': get_diagram_analysis_usage_summary(),
                        'requested_block_ids': selected_block_ids,
                        'failed_block_ids': failed_block_ids,
                        'exhausted_block_ids': exhausted_block_ids,
                    }, 503)
                return jsonify(payload), status_code
        conn.commit()
    except DiagramVisionThrottleError as exc:
        if conn:
            conn.rollback()
        return jsonify({
            'error': str(exc),
            'usage': get_diagram_analysis_usage_summary(),
        }), 429
    except Exception as exc:
        if conn:
            conn.rollback()
        logger.exception("Selected diagram analysis failed for document_id=%s blocks=%s", document_id, selected_block_ids)
        payload, status_code = _build_diagram_analysis_failure_payload(exc)
        return jsonify(payload), status_code
    finally:
        if conn:
            conn.close()

    refreshed_result = get_document_parser_result(
        user_id=user_id,
        document_id=document_id,
        conversation_id=conversation_id,
    )
    if not refreshed_result:
        return jsonify({'error': 'Document not found after analysis.'}), 404

    response_payload = _build_parser_review_payload(refreshed_result)
    response_payload["diagram_analysis_usage"] = get_diagram_analysis_usage_summary()
    response_payload["requested_block_ids"] = selected_block_ids
    response_payload["analyzed_block_ids"] = analyzed_block_ids
    response_payload["failed_block_ids"] = failed_block_ids
    response_payload["exhausted_block_ids"] = exhausted_block_ids
    response_payload["failure_reason_by_block"] = analysis_result.get("failure_reason_by_block") or {}
    response_payload["all_models_exhausted"] = all_models_exhausted

    if failed_block_ids:
        response_payload["error"] = (
            "Some selected diagrams were analyzed, but the remaining diagrams could not be processed because all compatible vision models are temporarily unavailable."
            if all_models_exhausted
            else "Some selected diagrams could not be analyzed."
        )
        response_payload["error_type"] = "partial_quota_exhaustion" if all_models_exhausted else "partial_analysis_failed"
        response_payload["fallback_action"] = "copy_image_and_prompt" if all_models_exhausted else None
        return jsonify(response_payload), 429 if all_models_exhausted else 207

    return jsonify(response_payload), 200


# ===========================================================================
# 10. FILE SERVING ROUTES
# ===========================================================================

@app.route('/uploads/<path:filename>')
def uploaded_file(filename):
    user_id = session.get("user_id")
    file_path = _resolve_authorized_upload_path(user_id, filename)
    if file_path is None:
        abort(404)

    user_root = (UPLOADS_DIR / str(user_id)).resolve()
    relative_path = file_path.relative_to(user_root)
    return send_from_directory(user_root, str(relative_path).replace("\\", "/"), as_attachment=False)


@app.route('/uploads/preview/<path:filename>')
def uploaded_file_preview(filename):
    user_id = session.get("user_id")
    file_path = _resolve_authorized_upload_path(user_id, filename)
    if file_path is None:
        abort(404)

    suffix = file_path.suffix.lower()
    if suffix == ".pdf":
        user_root = (UPLOADS_DIR / str(user_id)).resolve()
        relative_path = file_path.relative_to(user_root)
        return send_from_directory(user_root, str(relative_path).replace("\\", "/"), as_attachment=False)
    if suffix != ".docx":
        abort(415, description="Preview is only supported for PDF and DOCX files.")

    preview_pdf_path = get_preview_pdf_path(file_path)
    needs_convert    = (
        not preview_pdf_path.exists()
        or preview_pdf_path.stat().st_mtime < file_path.stat().st_mtime
    )
    if needs_convert:
        if not convert_docx_to_pdf(file_path, preview_pdf_path):
            abort(500, description="Could not convert DOCX to PDF for preview.")

    return send_from_directory(PREVIEW_DIR, preview_pdf_path.name, as_attachment=False)


# ===========================================================================
# ENTRY POINT
# ===========================================================================

if __name__ == '__main__':
    debug_mode = True
    if not debug_mode or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        _start_embedding_retry_poller()
        _start_summary_retry_poller()
    app.run(debug=debug_mode)
