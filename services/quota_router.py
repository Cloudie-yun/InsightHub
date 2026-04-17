from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from db import get_db_connection


TASK_TYPE_EMBEDDING = "embedding"
TASK_TYPE_DIAGRAM_VISION = "diagram_vision"
TASK_TYPE_TEXT = "text"

WINDOW_TYPE_RPM = "rpm"
WINDOW_TYPE_RPD = "rpd"
WINDOW_TYPE_TPM = "tpm"
WINDOW_TYPES = (WINDOW_TYPE_RPM, WINDOW_TYPE_RPD, WINDOW_TYPE_TPM)

DEFAULT_GEMINI_EMBED_MODELS = ["gemini-embedding-001", "gemini-embedding-002"]
DEFAULT_GEMINI_VISION_MODELS = ["gemini-2.5-flash", "gemini-3-flash", "gemini-3.1-flash-lite", "gemini-2.5-flash-lite"]
DEFAULT_GEMINI_TEXT_MODELS = ["gemini-2.5-flash", "gemini-3-flash", "gemini-3.1-flash-lite", "gemini-2.5-flash-lite"]
DEFAULT_RPD_RESET_TIMEZONE = "America/Los_Angeles"
DEFAULT_QUOTA_DISPLAY_TIMEZONE = "America/Los_Angeles"
DEFAULT_MODEL_LIMITS = {
    "gemini-2.5-flash": {"provider": "gemini", "rpm_limit": 5, "tpm_limit": 250_000, "rpd_limit": 20},
    "gemini-embedding-001": {"provider": "gemini", "rpm_limit": 100, "tpm_limit": 30_000, "rpd_limit": 1_000},
    "gemini-3-flash": {"provider": "gemini", "rpm_limit": 5, "tpm_limit": 250_000, "rpd_limit": 20},
    "gemini-3.1-flash-lite": {"provider": "gemini", "rpm_limit": 15, "tpm_limit": 250_000, "rpd_limit": 500},
    "gemini-2.5-flash-lite": {"provider": "gemini", "rpm_limit": 10, "tpm_limit": 250_000, "rpd_limit": 20},
    "gemini-embedding-002": {"provider": "gemini", "rpm_limit": 100, "tpm_limit": 30_000, "rpd_limit": 1_000},
}


class QuotaRouterError(RuntimeError):
    pass


@dataclass
class ModelQuotaWindow:
    model_name: str
    window_type: str
    used_count: int
    reset_at: datetime
    last_error_at: datetime | None
    last_error_code: str | None

    @property
    def is_active(self) -> bool:
        return self.reset_at > datetime.now(timezone.utc)

    @property
    def is_exhausted(self) -> bool:
        return self.is_active and bool(self.last_error_code)


@dataclass
class ModelQuotaLimit:
    model_name: str
    provider: str
    rpm_limit: int | None
    tpm_limit: int | None
    rpd_limit: int | None
    is_active: bool = True

    def limit_for(self, window_type: str) -> int | None:
        return {
            WINDOW_TYPE_RPM: self.rpm_limit,
            WINDOW_TYPE_TPM: self.tpm_limit,
            WINDOW_TYPE_RPD: self.rpd_limit,
        }.get(window_type)


def get_quota_project_id() -> str:
    return (os.environ.get("QUOTA_PROJECT_ID") or "default").strip() or "default"


def get_rpd_reset_timezone() -> ZoneInfo:
    raw_name = (os.environ.get("QUOTA_RPD_RESET_TIMEZONE") or DEFAULT_RPD_RESET_TIMEZONE).strip()
    try:
        return ZoneInfo(raw_name)
    except Exception:
        return ZoneInfo(DEFAULT_RPD_RESET_TIMEZONE)


def get_quota_display_timezone() -> ZoneInfo:
    raw_name = (os.environ.get("QUOTA_DISPLAY_TIMEZONE") or DEFAULT_QUOTA_DISPLAY_TIMEZONE).strip()
    try:
        return ZoneInfo(raw_name)
    except Exception:
        return ZoneInfo(DEFAULT_QUOTA_DISPLAY_TIMEZONE)


def format_quota_timestamp(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(get_quota_display_timezone()).isoformat()


def get_task_models(task_type: str, *, fallback_model: str | None = None) -> list[str]:
    env_map = {
        TASK_TYPE_EMBEDDING: ("GEMINI_EMBED_MODELS", DEFAULT_GEMINI_EMBED_MODELS),
        TASK_TYPE_DIAGRAM_VISION: ("GEMINI_VISION_MODELS", DEFAULT_GEMINI_VISION_MODELS),
        TASK_TYPE_TEXT: ("GEMINI_TEXT_MODELS", DEFAULT_GEMINI_TEXT_MODELS),
    }
    env_name, default_models = env_map.get(task_type, ("", []))
    configured = _parse_model_list(os.environ.get(env_name), default=default_models)
    if fallback_model and fallback_model not in configured:
        configured.append(fallback_model)
    return configured


def pick_available_model(
    task_type: str,
    *,
    project_id: str | None = None,
    fallback_model: str | None = None,
    excluded_models: list[str] | None = None,
) -> str:
    project_id = project_id or get_quota_project_id()
    models = get_task_models(task_type, fallback_model=fallback_model)
    excluded = {str(model).strip() for model in (excluded_models or []) if str(model).strip()}
    limits = load_model_limits(model_names=models)
    states = load_usage_state(project_id=project_id, model_names=models)

    for model in models:
        if model in excluded:
            continue
        model_windows = states.get(model, {})
        if _is_model_exhausted(model_windows=model_windows, model_limit=limits.get(model)):
            continue
        return model

    raise QuotaRouterError(f"No compatible model is currently available for task_type={task_type}.")


def load_model_limits(*, model_names: list[str]) -> dict[str, ModelQuotaLimit]:
    if not model_names:
        return {}

    limits: dict[str, ModelQuotaLimit] = {}
    for model_name in model_names:
        default_limit = _build_limit_from_defaults(model_name)
        if default_limit is not None:
            limits[model_name] = default_limit

    conn = get_db_connection()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    model_name,
                    provider,
                    rpm_limit,
                    tpm_limit,
                    rpd_limit,
                    is_active
                FROM quota_limits
                WHERE model_name = ANY(%s)
                """,
                (model_names,),
            )
            rows = cur.fetchall()
    except Exception:
        return limits
    finally:
        conn.close()

    for row in rows:
        limits[row[0]] = ModelQuotaLimit(
            model_name=row[0],
            provider=str(row[1] or "gemini"),
            rpm_limit=_coerce_optional_int(row[2]),
            tpm_limit=_coerce_optional_int(row[3]),
            rpd_limit=_coerce_optional_int(row[4]),
            is_active=bool(row[5]),
        )
    return limits


def load_usage_state(*, project_id: str, model_names: list[str]) -> dict[str, dict[str, ModelQuotaWindow]]:
    if not model_names:
        return {}

    conn = get_db_connection()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    model_name,
                    window_type,
                    used_count,
                    reset_at,
                    last_error_at,
                    last_error_code
                FROM quota_state
                WHERE project_id = %s
                  AND model_name = ANY(%s)
                """,
                (project_id, model_names),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    state: dict[str, dict[str, ModelQuotaWindow]] = {model: {} for model in model_names}
    now = datetime.now(timezone.utc)
    for row in rows:
        window = ModelQuotaWindow(
            model_name=row[0],
            window_type=row[1],
            used_count=int(row[2] or 0),
            reset_at=row[3],
            last_error_at=row[4],
            last_error_code=row[5],
        )
        if window.reset_at <= now:
            continue
        state.setdefault(window.model_name, {})[window.window_type] = window
    return state


def record_model_success(
    *,
    project_id: str | None,
    model_name: str,
    request_count: int = 1,
    token_count: int | None = None,
    response_headers: dict[str, Any] | None = None,
) -> None:
    project_id = project_id or get_quota_project_id()
    headers = _normalize_headers(response_headers)
    now = datetime.now(timezone.utc)
    updates = [
        _build_success_update(window_type=WINDOW_TYPE_RPM, now=now, increment=request_count, headers=headers),
        _build_success_update(window_type=WINDOW_TYPE_RPD, now=now, increment=request_count, headers=headers),
    ]
    if token_count is not None:
        updates.append(_build_success_update(window_type=WINDOW_TYPE_TPM, now=now, increment=token_count, headers=headers))

    _upsert_quota_rows(project_id=project_id, model_name=model_name, updates=updates)


def record_quota_failure(
    *,
    project_id: str | None,
    model_name: str,
    error_code: str,
    retry_after_seconds: float | None,
    response_headers: dict[str, Any] | None = None,
) -> None:
    project_id = project_id or get_quota_project_id()
    headers = _normalize_headers(response_headers)
    now = datetime.now(timezone.utc)
    cooldowns = {
        WINDOW_TYPE_RPM: _resolve_reset_at(window_type=WINDOW_TYPE_RPM, now=now, headers=headers, retry_after_seconds=retry_after_seconds),
        WINDOW_TYPE_RPD: _resolve_reset_at(window_type=WINDOW_TYPE_RPD, now=now, headers=headers, retry_after_seconds=retry_after_seconds),
        WINDOW_TYPE_TPM: _resolve_reset_at(window_type=WINDOW_TYPE_TPM, now=now, headers=headers, retry_after_seconds=retry_after_seconds),
    }
    updates = [
        {
            "window_type": window_type,
            "used_count": 1,
            "reset_at": reset_at,
            "last_error_at": now,
            "last_error_code": error_code,
        }
        for window_type, reset_at in cooldowns.items()
    ]
    _upsert_quota_rows(project_id=project_id, model_name=model_name, updates=updates)


def extract_response_headers(details: dict[str, Any] | None) -> dict[str, str]:
    if not isinstance(details, dict):
        return {}
    response_headers = details.get("response_headers")
    if not isinstance(response_headers, dict):
        return {}
    return {str(key): str(value) for key, value in response_headers.items()}


def classify_quota_error(*, status_code: int | None, message: str, details: dict[str, Any] | None = None) -> str | None:
    haystack = " ".join(
        part for part in [
            str(status_code or ""),
            str(message or ""),
            str((details or {}).get("response_body") or ""),
        ]
        if part
    ).lower()
    if status_code == 429:
        return "rate_limit"
    if any(marker in haystack for marker in ("resource_exhausted", "quota", "rate limit", "too many requests")):
        return "quota_exhausted"
    return None


def _upsert_quota_rows(*, project_id: str, model_name: str, updates: list[dict[str, Any]]) -> None:
    conn = get_db_connection()
    try:
        with conn, conn.cursor() as cur:
            for update in updates:
                cur.execute(
                    """
                    INSERT INTO quota_state (
                        project_id,
                        model_name,
                        window_type,
                        used_count,
                        reset_at,
                        last_error_at,
                        last_error_code,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ON CONFLICT (project_id, model_name, window_type)
                    DO UPDATE SET
                        used_count = CASE
                            WHEN EXCLUDED.last_error_code IS NOT NULL THEN EXCLUDED.used_count
                            WHEN quota_state.reset_at > CURRENT_TIMESTAMP
                                 AND quota_state.last_error_code IS NULL
                            THEN quota_state.used_count + EXCLUDED.used_count
                            ELSE EXCLUDED.used_count
                        END,
                        reset_at = EXCLUDED.reset_at,
                        last_error_at = EXCLUDED.last_error_at,
                        last_error_code = EXCLUDED.last_error_code,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (
                        project_id,
                        model_name,
                        update["window_type"],
                        max(0, int(update["used_count"])),
                        update["reset_at"],
                        update.get("last_error_at"),
                        update.get("last_error_code"),
                    ),
                )
    finally:
        conn.close()


def _build_success_update(
    *,
    window_type: str,
    now: datetime,
    increment: int,
    headers: dict[str, str],
) -> dict[str, Any]:
    reset_at = _resolve_reset_at(window_type=window_type, now=now, headers=headers, retry_after_seconds=None)
    return {
        "window_type": window_type,
        "used_count": max(0, increment),
        "reset_at": reset_at,
        "last_error_at": None,
        "last_error_code": None,
    }


def _build_limit_from_defaults(model_name: str) -> ModelQuotaLimit | None:
    payload = DEFAULT_MODEL_LIMITS.get(str(model_name or "").strip())
    if not payload:
        return None
    return ModelQuotaLimit(
        model_name=str(model_name).strip(),
        provider=str(payload.get("provider") or "gemini"),
        rpm_limit=_coerce_optional_int(payload.get("rpm_limit")),
        tpm_limit=_coerce_optional_int(payload.get("tpm_limit")),
        rpd_limit=_coerce_optional_int(payload.get("rpd_limit")),
        is_active=True,
    )


def _coerce_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _is_model_exhausted(*, model_windows: dict[str, ModelQuotaWindow], model_limit: ModelQuotaLimit | None) -> bool:
    if any(window.is_exhausted for window in model_windows.values()):
        return True
    if model_limit is None or not model_limit.is_active:
        return False
    for window_type, window in model_windows.items():
        limit_value = model_limit.limit_for(window_type)
        if limit_value is None:
            continue
        if window.is_active and window.used_count >= limit_value:
            return True
    return False


def _normalize_headers(headers: dict[str, Any] | None) -> dict[str, str]:
    if not isinstance(headers, dict):
        return {}
    return {str(key).strip().lower(): str(value).strip() for key, value in headers.items() if str(key).strip()}


def _resolve_reset_at(
    *,
    window_type: str,
    now: datetime,
    headers: dict[str, str],
    retry_after_seconds: float | None,
) -> datetime:
    header_candidates = {
        WINDOW_TYPE_RPM: ["x-ratelimit-reset-requests", "retry-after"],
        WINDOW_TYPE_RPD: ["x-ratelimit-reset-requests", "retry-after"],
        WINDOW_TYPE_TPM: ["x-ratelimit-reset-tokens", "retry-after"],
    }.get(window_type, ["retry-after"])

    for header_name in header_candidates:
        parsed = _parse_reset_value(headers.get(header_name), now=now)
        if parsed is not None:
            return parsed

    if retry_after_seconds is not None:
        return now + timedelta(seconds=max(0.0, retry_after_seconds))

    if window_type == WINDOW_TYPE_RPD:
        reset_timezone = get_rpd_reset_timezone()
        local_now = now.astimezone(reset_timezone)
        next_local_day = local_now + timedelta(days=1)
        return datetime(
            next_local_day.year,
            next_local_day.month,
            next_local_day.day,
            tzinfo=reset_timezone,
        ).astimezone(timezone.utc)

    return now + timedelta(minutes=1)


def _parse_reset_value(raw_value: str | None, *, now: datetime) -> datetime | None:
    value = str(raw_value or "").strip()
    if not value:
        return None

    try:
        return now + timedelta(seconds=max(0.0, float(value)))
    except (TypeError, ValueError):
        pass

    duration_match = re.match(r"^(\d+)(ms|s|m|h)$", value.lower())
    if duration_match:
        amount = float(duration_match.group(1))
        unit = duration_match.group(2)
        multiplier = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0}[unit]
        return now + timedelta(seconds=amount * multiplier)

    try:
        absolute_seconds = float(value)
        if absolute_seconds > 1_000_000:
            return datetime.fromtimestamp(absolute_seconds, tz=timezone.utc)
    except (TypeError, ValueError, OSError, OverflowError):
        pass

    return None


def summarize_usage_state(project_id: str | None = None) -> dict[str, Any]:
    project_id = project_id or get_quota_project_id()
    conn = get_db_connection()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    model_name,
                    window_type,
                    used_count,
                    reset_at,
                    last_error_at,
                    last_error_code
                FROM quota_state
                WHERE project_id = %s
                ORDER BY model_name ASC, window_type ASC
                """,
                (project_id,),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    return {
        "project_id": project_id,
        "display_timezone": str(get_quota_display_timezone()),
        "rows": [
            {
                "model_name": row[0],
                "window_type": row[1],
                "used_count": int(row[2] or 0),
                "reset_at": row[3].isoformat() if row[3] else None,
                "reset_at_display": format_quota_timestamp(row[3]),
                "last_error_at": row[4].isoformat() if row[4] else None,
                "last_error_at_display": format_quota_timestamp(row[4]),
                "last_error_code": row[5],
            }
            for row in rows
        ],
    }


def _parse_model_list(raw_value: str | None, *, default: list[str]) -> list[str]:
    if raw_value is None:
        return list(default)
    models = [item.strip() for item in str(raw_value).split(",") if item.strip()]
    return models or list(default)
