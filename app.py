from flask import Flask, render_template, jsonify, request, send_from_directory, abort, session, redirect, url_for
from db import get_db_connection
from email_service import send_email
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import subprocess
import psycopg2
from psycopg2 import errors
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timedelta, timezone
import hashlib
import secrets
import os
import re
from urllib.parse import urljoin, urlencode, quote
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
import json
import logging
import mimetypes
import time

from services.document_parser import parse_document
from services.extraction_store import (
    build_extraction_payload,
    build_pending_extraction_payload,
    save_document_extraction,
    get_document_extraction as fetch_document_extraction,
    get_conversation_extractions as fetch_conversation_extractions,
)


# ===========================================================================
# 1. APP SETUP & CONFIGURATION
# ===========================================================================

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret-change-me")
logger = logging.getLogger(__name__)
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

            extraction_payload = (
                fetch_document_extraction(cur, document_id=document_id, conversation_id=conversation_id)
                or build_pending_extraction_payload(document_id=document_id)
            )

            return {
                "document_id":       str(document_row[0]),
                "original_filename": document_row[1] or "",
                "file_extension":    document_row[2] or "",
                "mime_type":         document_row[3] or "",
                "parser_result":     extraction_payload,
            }
    except Exception:
        return None
    finally:
        if conn is not None:
            conn.close()


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

    if current_conversation_id and not conversation_exists_for_user(user_id, current_conversation_id):
        return render_template(
            "conversation_not_found.html",
            active_page="chat",
            requested_conversation_id=current_conversation_id,
        ), 404

    conversation_documents       = get_conversation_documents(user_id, current_conversation_id)

    return render_template(
        'chat.html',
        active_page                 = 'chat',
        current_conversation_id     = current_conversation_id,
        highlight_new_conversation  = highlight_new_conversation,
        conversation_documents      = conversation_documents,
    )


@app.route('/documents/<document_id>/parser-results')
def document_parser_results(document_id):
    user_id = session.get("user_id")
    if not user_id:
        return redirect(url_for("dashboard"))

    conversation_id = (request.args.get("conversation_id") or "").strip() or None
    document_result = get_document_parser_result(
        user_id         = user_id,
        document_id     = document_id,
        conversation_id = conversation_id,
    )
    if not document_result:
        abort(404)

    return render_template(
        "document_parser_results.html",
        active_page  = "chat",
        parser_document = document_result,
        parser_json     = json.dumps(document_result, indent=2),
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


def _insert_document_and_mark_pending(cur, user_id, user_upload_dir, incoming_file, original_name, extension) -> dict:
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
        },
        "_destination":      destination,   # kept for rollback cleanup; stripped before response
    }


def _run_document_parse_job(job: dict) -> None:
    document_id = job["document_id"]
    destination = Path(job["destination"])
    mime_type = job.get("mime_type")
    original_filename = job.get("original_filename")

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
            )
            save_document_extraction(
                cur,
                document_id=document_id,
                extraction_payload=extraction_payload,
            )
        conn.commit()
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
                    cur, user_id, user_upload_dir, incoming_file, original_name, extension
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
                    cur, user_id, user_upload_dir, incoming_file, original_name, extension
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

    return jsonify(document_result), 200


@app.route('/api/documents/<document_id>/reparse', methods=['POST'])
def api_reparse_document(document_id):
    user_id = session.get("user_id")
    if not user_id:
        return jsonify({'error': 'You must be logged in to reparse documents.'}), 401

    data            = request.get_json(silent=True) or {}
    raw_conv_id     = request.args.get("conversation_id") or data.get("conversation_id")
    conversation_id = str(raw_conv_id).strip() if raw_conv_id is not None else None

    persist_raw        = data.get("persist_extraction", False)
    persist_extraction = (
        persist_raw.strip().lower() in {"1", "true", "yes", "on"}
        if isinstance(persist_raw, str)
        else bool(persist_raw)
    )

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            if conversation_id:
                cur.execute(
                    """
                    SELECT d.document_id, d.original_filename, d.file_extension,
                           d.mime_type, d.stored_filename, d.storage_path
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
                    SELECT document_id, original_filename, file_extension,
                           mime_type, stored_filename, storage_path
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
                return jsonify({'error': 'Document not found.'}), 404

            stored_filename = document_row[4] or ""
            storage_path    = document_row[5] or ""
            if not stored_filename:
                return jsonify({'error': 'Stored file name is missing for this document.'}), 400

            file_path = (Path(storage_path) / stored_filename) if storage_path else (UPLOADS_DIR / user_id / stored_filename)
            if not file_path.exists() or not file_path.is_file():
                return jsonify({'error': 'Document file is missing on disk.'}), 404

            parser_result      = parse_document(file_path=file_path, document_id=document_row[0],
                                                mime_type=document_row[3] or None,
                                                original_filename=document_row[1] or stored_filename)
            extraction_payload = build_extraction_payload(document_id=document_row[0], parser_result=parser_result)

            if persist_extraction:
                save_document_extraction(cur, document_id=document_row[0], extraction_payload=extraction_payload)

        if persist_extraction:
            conn.commit()
        else:
            conn.rollback()

        return jsonify({
            "document_id":       str(document_row[0]),
            "original_filename": document_row[1] or "",
            "file_extension":    document_row[2] or "",
            "mime_type":         document_row[3] or "",
            "parser_result":     extraction_payload,
            "persisted":         persist_extraction,
        }), 200
    except Exception:
        if conn is not None:
            conn.rollback()
        return jsonify({'error': 'Unable to reparse document right now.'}), 500
    finally:
        if conn is not None:
            conn.close()


# ===========================================================================
# 10. FILE SERVING ROUTES
# ===========================================================================

@app.route('/uploads/<path:filename>')
def uploaded_file(filename):
    file_path = UPLOADS_DIR / filename
    if not file_path.exists() or not file_path.is_file():
        abort(404)
    return send_from_directory(UPLOADS_DIR, filename, as_attachment=False)


@app.route('/uploads/preview/<path:filename>')
def uploaded_file_preview(filename):
    file_path = UPLOADS_DIR / filename
    if not file_path.exists() or not file_path.is_file():
        abort(404)

    suffix = file_path.suffix.lower()
    if suffix == ".pdf":
        return send_from_directory(UPLOADS_DIR, filename, as_attachment=False)
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
    app.run(debug=True)
