from flask import Flask, render_template, jsonify, request, send_from_directory, abort, session, redirect, url_for
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path
from threading import Lock, Thread
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin, urlencode, quote
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
from services.extraction_store import (
    build_extraction_payload,
    build_pending_extraction_payload,
    save_document_extraction,
    get_document_extraction as fetch_document_extraction,
    get_conversation_extractions as fetch_conversation_extractions,
)
from services.chat_answer_service import ChatAnswerService, ChatAnswerServiceError
from services.retrieval_service import RetrievalService, RetrievalServiceError
from services.quota_router import (
    TASK_TYPE_DIAGRAM_VISION,
    get_quota_project_id,
    get_task_models,
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
    ".pdf", ".doc", ".docx", ".ppt", ".pptx",
    ".png", ".jpg", ".jpeg", ".webp",
}

STRONG_PASSWORD_REGEX = re.compile(
    r"^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[^A-Za-z\d]).{8,}$"
)
PASSWORD_POLICY_ERROR = (
    "Password must be at least 8 characters and include uppercase, "
    "lowercase, number, and special character."
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


def _embedding_retry_poller_loop() -> None:
    while True:
        try:
            _schedule_embedding_autorun("retry_poller")
        except Exception:
            logger.exception("Embedding retry poller loop failed")
        time.sleep(EMBEDDING_RETRY_POLL_SECONDS)


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


def _build_diagram_analysis_usage_summary(cur) -> dict:
    del cur
    project_id = get_quota_project_id()
    ordered_models = get_task_models(TASK_TYPE_DIAGRAM_VISION, fallback_model=os.getenv("GEMINI_VISION_MODEL", "gemini-3-flash"))
    provider_order = get_diagram_vision_provider_order()
    primary_provider = get_primary_diagram_vision_provider()
    usage_state = load_usage_state(project_id=project_id, model_names=ordered_models)
    now_utc = datetime.now(timezone.utc)

    model_statuses = []
    earliest_reset_at: datetime | None = None
    available_models: list[str] = []

    for model_name in ordered_models:
        windows = usage_state.get(model_name, {})
        blocked_windows = [
            {
                "window_type": window.window_type,
                "used_count": window.used_count,
                "reset_at": window.reset_at.isoformat() if window.reset_at else None,
                "last_error_at": window.last_error_at.isoformat() if window.last_error_at else None,
                "last_error_code": window.last_error_code,
            }
            for window in windows.values()
            if window.is_exhausted
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
            }
        )

    preferred_model = ordered_models[0] if ordered_models else os.getenv("GEMINI_VISION_MODEL", "gemini-3-flash")
    active_model = available_models[0] if available_models else None
    hover_parts = [f"{status['model_name']}: {status['status_label']}" for status in model_statuses]
    if provider_order:
        hover_parts.insert(0, f"Providers: {' -> '.join(provider_order)}")
    if earliest_reset_at is not None:
        hover_parts.append(f"Earliest reset: {earliest_reset_at.isoformat()}")

    return {
        "provider": primary_provider,
        "project_id": project_id,
        "preferred_model": preferred_model,
        "active_model": active_model,
        "all_models_exhausted": not bool(available_models),
        "earliest_reset_at": earliest_reset_at.isoformat() if earliest_reset_at else None,
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
            "preferred_model": os.getenv("GEMINI_VISION_MODEL", "gemini-3-flash"),
            "active_model": None,
            "all_models_exhausted": False,
            "earliest_reset_at": None,
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


def build_external_url(path: str) -> str:
    app_base_url = os.getenv("APP_BASE_URL", "").strip()
    if app_base_url:
        return urljoin(app_base_url.rstrip("/") + "/", path.lstrip("/"))
    return urljoin(request.url_root, path.lstrip("/"))


def _compose_upload_conversation_title(file_names: list[str]) -> str:
    return "New conversation"


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
    return {
        "user_id":        str(user_row[0]),
        "username":       user_row[1],
        "email":          user_row[2],
        "created_at":     user_row[3].isoformat() if user_row[3] else None,
        "email_verified": bool(user_row[4]),
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


def _serialize_conversation_message(row) -> dict:
    selected_document_ids = row[5] if isinstance(row[5], list) else []
    retrieval_payload = row[6] if isinstance(row[6], dict) else None
    return {
        "message_id":          str(row[0]),
        "conversation_id":     str(row[1]),
        "user_id":             str(row[2]),
        "role":                row[3] or "",
        "message_text":        row[4] or "",
        "selected_document_ids": [str(item) for item in selected_document_ids],
        "retrieval_payload":   retrieval_payload,
        "model_provider":      row[7] or "",
        "model_name":          row[8] or "",
        "prompt_version":      row[9] or "",
        "reply_to_message_id": str(row[10]) if row[10] else None,
        "created_at":          row[11].isoformat() if row[11] else "",
    }


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
                SELECT user_id, username, email, created_at, email_verified
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
                ORDER BY cm.created_at ASC, cm.message_id ASC
                """,
                (conversation_id, user_id),
            )
            rows = cur.fetchall()
        return [_serialize_conversation_message(row) for row in rows]
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
            visual_description = _normalize_review_text(
                (detail or {}).get("visual_description")
                or normalized.get("visual_description")
                or ""
            )
            normalized["visual_description"] = visual_description or None
            if detail is not None:
                detail["visual_description"] = visual_description or None
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


def _build_parser_review_payload(document_result: dict) -> dict:
    document_result = deepcopy(document_result or {})
    parser_result = document_result.get("parser_result") or {}
    blocks = sorted(parser_result.get("document_blocks") or [], key=_review_block_sort_key)
    block_assets = parser_result.get("block_assets") or []
    diagram_details = parser_result.get("diagram_block_details") or []

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
            "preview_anchor": {
                "page_index": block.get("source_unit_index"),
                "bbox": block_bbox or source_location.get("bbox") or {},
            },
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
            item["normalized_content"] = {
                "diagram_kind": normalized.get("diagram_kind") or detail.get("diagram_kind") or block.get("subtype"),
                "visual_description": detail.get("visual_description") or normalized.get("visual_description") or "",
                "ocr_text": detail.get("ocr_text") or normalized.get("ocr_text") or [],
                "question_answerable_facts": detail.get("question_answerable_facts") or [],
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
                "page_index": block.get("source_unit_index"),
                "bbox": (
                    block_bbox
                    or ((detail.get("image_region") or {}).get("bbox") or {})
                    or source_location.get("bbox")
                    or {}
                ),
            }

        review_blocks.append(item)

    metadata = parser_result.get("metadata") or {}
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
        return configured
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

    return render_template(
        'chat.html',
        active_page                 = 'chat',
        current_conversation_id     = current_conversation_id,
        conversation_title          = conversation_title,
        highlight_new_conversation  = highlight_new_conversation,
        conversation_documents      = conversation_documents,
        conversation_messages       = conversation_messages,
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
    return render_template('flashcards.html', active_page='study')


@app.route('/mindmap')
def mindmap():
    return render_template('mindmap.html', active_page='study')


# ===========================================================================
# 8. AUTH API ROUTES
# ===========================================================================

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
                RETURNING user_id, username, email, created_at, email_verified
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
            cur.execute("SELECT auth_provider FROM users WHERE user_id = %s", (user_id,))
            row = cur.fetchone()
            if not row:
                session.pop("user_id", None)
                return jsonify({'error': 'User not found.'}), 404
            if row[0] != 'local':
                return jsonify({'error': 'This account uses a different sign-in method.'}), 400

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
                SELECT user_id, username, email, created_at, email_verified, auth_provider
                FROM users WHERE email = %s
                """,
                (email,),
            )
            user_row = cur.fetchone()

            if user_row and user_row[5] != 'google':
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
        },
        "_destination":      destination,   # kept for rollback cleanup; stripped before response
    }


def _run_document_parse_job(job: dict) -> None:
    document_id = job["document_id"]
    destination = Path(job["destination"])
    mime_type = job.get("mime_type")
    original_filename = job.get("original_filename")
    conversation_id = job.get("conversation_id")

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
        conn.commit()
        _schedule_embedding_autorun(f"document_parse:{document_id}")
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
    except Exception:
        logger.exception("Conversation retrieval failed conversation_id=%s", conversation_id)
        return jsonify({'error': 'Unable to retrieve context right now.'}), 500


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
    query = str(payload.get("query") or "").strip()
    if not query:
        return jsonify({'error': 'query is required.'}), 400

    try:
        response_payload = ChatAnswerService().answer_conversation_query(
            user_id=user_id,
            conversation_id=conversation_id,
            query=query,
            document_ids=payload.get("document_ids"),
            k=payload.get("k"),
            include_filtered=payload.get("include_filtered"),
        )
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
    except Exception:
        logger.exception("Conversation message generation failed conversation_id=%s", conversation_id)
        return jsonify({'error': 'Unable to send your message right now.'}), 500


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
            visual_description = _normalize_review_text(edited_normalized.get("visual_description"))
            current_diagram_detail = dict(diagram_details_by_block.get(block_id) or {})
            current_visual_description = _normalize_review_text(normalized.get("visual_description"))
            current_caption_text = _normalize_review_text(
                existing_block.get("caption_text") or existing_block.get("display_text") or ""
            )
            current_ocr_text = current_diagram_detail.get("ocr_text") or normalized.get("ocr_text") or []
            if not isinstance(current_ocr_text, list):
                current_ocr_text = []
            current_semantic_links = current_diagram_detail.get("semantic_links") or normalized.get("semantic_links") or []
            if not isinstance(current_semantic_links, list):
                current_semantic_links = []
            current_question_answerable_facts = current_diagram_detail.get("question_answerable_facts") or []
            if not isinstance(current_question_answerable_facts, list):
                current_question_answerable_facts = []
            current_storage_path = str(current_diagram_detail.get("storage_path") or edited_normalized.get("storage_path") or "")
            next_ocr_text = edited_normalized.get("ocr_text") or []
            if not isinstance(next_ocr_text, list):
                next_ocr_text = []
            next_semantic_links = edited_normalized.get("semantic_links") or []
            if not isinstance(next_semantic_links, list):
                next_semantic_links = []
            next_question_answerable_facts = edited_normalized.get("question_answerable_facts") or []
            if not isinstance(next_question_answerable_facts, list):
                next_question_answerable_facts = []
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
            normalized["visual_description"] = visual_description or None
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
            diagram_detail["visual_description"] = visual_description or None
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
        conn.commit()
        _schedule_embedding_autorun(f"parser_review:{document_id}")
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

            analysis_result = run_diagram_analysis_for_document(
                cur,
                document_id=document_id,
                block_ids=selected_block_ids,
                force_analyze=True,
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
    app.run(debug=debug_mode)
