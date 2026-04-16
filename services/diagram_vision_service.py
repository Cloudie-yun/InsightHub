from __future__ import annotations

import base64
import email.utils
import json
import mimetypes
import os
import re
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from json import JSONDecodeError
from pathlib import Path
from typing import Any
from urllib import error, request

from psycopg2.extras import Json

from services.quota_router import (
    TASK_TYPE_DIAGRAM_VISION,
    QuotaRouterError,
    classify_quota_error,
    get_quota_project_id,
    get_task_models,
    pick_available_model,
    record_model_success,
    record_quota_failure,
)


GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta"
PROMPT_VERSION = "diagram_v1"
VISION_GATE_PROMPT_VERSION = "diagram_gate_v1"
DIAGRAM_VISION_MIN_SCORE = float(os.environ.get("DIAGRAM_VISION_MIN_SCORE", "0.45"))
GEMINI_VISION_MAX_OUTPUT_TOKENS = int(os.environ.get("GEMINI_VISION_MAX_OUTPUT_TOKENS", "8192"))
DIAGRAM_ASSETS_BASE_DIR = Path(
    os.environ.get("DIAGRAM_ASSETS_BASE_DIR", str(Path.cwd() / "uploads"))
)


def _get_default_gemini_model() -> str:
    return os.environ.get("GEMINI_VISION_MODEL", "gemini-3-flash")


def _get_max_diagram_vision_model_attempts() -> int:
    return max(1, int(os.environ.get("DIAGRAM_VISION_MODEL_MAX_ATTEMPTS", "3")))


def _read_limit_env(name: str, default: int | None = None) -> int | None:
    raw_value = str(os.environ.get(name, "") or "").strip()
    if not raw_value:
        return default
    try:
        return max(0, int(raw_value))
    except (TypeError, ValueError):
        return default

_POSITIVE_DIAGRAM_KEYWORDS = {
    "architecture",
    "workflow",
    "pipeline",
    "framework",
    "system",
    "process",
    "flowchart",
    "chart",
    "graph",
    "plot",
    "scatter",
    "histogram",
    "distribution",
    "trend",
    "axis",
    "component",
    "module",
    "network",
    "circuit",
    "diagram",
}
_REFERENCE_CONTEXT_KEYWORDS = {
    "figure",
    "fig.",
    "shown",
    "illustrates",
    "depicts",
    "overview",
    "architecture",
    "workflow",
    "pipeline",
    "results",
    "comparison",
    "performance",
    "trend",
}
_NEGATIVE_IMAGE_KEYWORDS = {
    "logo",
    "icon",
    "avatar",
    "portrait",
    "author",
    "headshot",
    "photo",
    "photograph",
    "cover",
    "decoration",
    "ornament",
}

DIAGRAM_RESPONSE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "diagram_type",
        "title_or_caption",
        "ocr_text",
        "summary",
        "key_entities",
        "relationships",
        "question_answerable_facts",
        "confidence",
    ],
    "properties": {
        "diagram_type": {
            "type": "string",
            "enum": [
                "flowchart",
                "chart",
                "table_like_figure",
                "architecture_diagram",
                "scientific_figure",
                "screenshot",
                "image",
                "unknown",
            ],
        },
        "title_or_caption": {"type": ["string", "null"]},
        "ocr_text": {"type": "array", "items": {"type": "string"}},
        "summary": {"type": "string"},
        "key_entities": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["name", "type"],
                "properties": {
                    "name": {"type": "string"},
                    "type": {
                        "type": "string",
                        "enum": ["node", "axis", "label", "box", "legend", "component", "arrow", "unknown"],
                    },
                },
            },
        },
        "relationships": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["source", "relation", "target"],
                "properties": {
                    "source": {"type": "string"},
                    "relation": {
                        "type": "string",
                        "enum": [
                            "points_to",
                            "connected_to",
                            "contains",
                            "compares_with",
                            "shows",
                            "influences",
                            "depends_on",
                            "unknown",
                        ],
                    },
                    "target": {"type": "string"},
                },
            },
        },
        "question_answerable_facts": {"type": "array", "items": {"type": "string"}},
        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
    },
}


def _is_truthy_env(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _parse_retry_after(value: str | None) -> float | None:
    raw_value = str(value or "").strip()
    if not raw_value:
        return None

    try:
        return max(0.0, float(raw_value))
    except (TypeError, ValueError):
        pass

    try:
        parsed_dt = email.utils.parsedate_to_datetime(raw_value)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None

    if parsed_dt is None:
        return None
    if parsed_dt.tzinfo is None:
        parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)

    return max(0.0, (parsed_dt - datetime.now(timezone.utc)).total_seconds())


def _serialize_response_headers(headers: Any) -> dict[str, str]:
    if not headers or not hasattr(headers, "items"):
        return {}
    selected_headers: dict[str, str] = {}
    for key, value in headers.items():
        normalized_key = str(key or "").strip()
        if not normalized_key:
            continue
        selected_headers[normalized_key] = str(value or "").strip()[:200]
    return selected_headers


@dataclass
class DiagramVisionInput:
    block_id: str
    image_path: str
    caption_text: str | None
    nearby_text: str | None
    image_asset_id: str | None = None
    diagram_kind: str = "figure"


@dataclass
class DiagramVisionDecision:
    score: float
    should_analyze: bool
    reasons: list[str]


class DiagramVisionThrottleError(RuntimeError):
    pass


@dataclass
class DiagramVisionRequestError(RuntimeError):
    message: str
    status_code: int | None = None
    retry_after_seconds: float | None = None
    details: dict[str, Any] | None = None

    def __str__(self) -> str:
        return self.message


def _should_retry_with_another_model(exc: Exception) -> bool:
    if isinstance(exc, DiagramVisionRequestError):
        return True
    if isinstance(exc, ValueError):
        message = str(exc or "").lower()
        return any(
            marker in message
            for marker in (
                "gemini returned no json text",
                "gemini response was truncated",
                "gemini returned invalid json text",
            )
        )
    return False


class ThrottledVisionQueue:
    def __init__(self, rpm_limit: int | None = None, daily_limit: int | None = None) -> None:
        self.rpm_limit = rpm_limit
        self.daily_limit = daily_limit
        self.request_times: deque[float] = deque()
        self.daily_count = 0
        self.daily_reset = time.time() + 86400
        self.lock = threading.Lock()

    def _reset_daily_window_if_needed(self, now: float) -> None:
        if now > self.daily_reset:
            self.daily_count = 0
            self.daily_reset = now + 86400

    def wait_if_needed(self) -> None:
        while True:
            sleep_time = 0.0
            with self.lock:
                now = time.time()
                self._reset_daily_window_if_needed(now)

                if self.daily_limit is not None and self.daily_count >= self.daily_limit:
                    wait = max(0.0, self.daily_reset - now)
                    raise DiagramVisionThrottleError(
                        f"Daily Gemini vision limit reached. Resets in {wait / 3600:.1f} hours"
                    )

                while self.request_times and now - self.request_times[0] >= 60:
                    self.request_times.popleft()

                if self.rpm_limit is not None and len(self.request_times) >= self.rpm_limit:
                    oldest = self.request_times[0]
                    sleep_time = max(0.0, 60 - (now - oldest) + 0.5)
                else:
                    self.request_times.append(now)
                    self.daily_count += 1
                    return

            if sleep_time > 0:
                print(f"[THROTTLE] Waiting {sleep_time:.1f}s for Gemini vision quota")
                time.sleep(sleep_time)


VISION_QUEUE = ThrottledVisionQueue(
    rpm_limit=_read_limit_env("GEMINI_VISION_RPM_LIMIT"),
    daily_limit=_read_limit_env("GEMINI_VISION_RPD_LIMIT"),
)


def build_diagram_prompt(*, caption: str | None, nearby_text: str | None) -> str:
    caption = (caption or "").strip()
    nearby_text = (nearby_text or "").strip()
    return f"""
You are analyzing a diagram extracted from an academic or technical document.

Goal:
Produce structured information that helps a document-grounded chatbot answer questions about this figure accurately.

Instructions:
1. Identify the figure type.
2. Read only the most useful visible labels or text inside the figure. Deduplicate repeated labels.
3. Explain what the figure shows in a short factual summary of 2-4 sentences.
4. Extract key entities such as nodes, labels, components, legends, arrows, or axes. Keep only the most important ones.
5. Extract explicit relationships shown by arrows, lines, containment, comparison, or dependency. Keep only the clearest relationships.
6. Extract concise facts that a chatbot can directly reuse in question answering.
7. Do not invent information that is not visible or reasonably supported by the caption/context.
8. Keep the response compact:
   - `ocr_text`: at most 25 items
   - `key_entities`: at most 15 items
   - `relationships`: at most 15 items
   - `question_answerable_facts`: at most 12 items
9. Return JSON only.

Caption:
{caption if caption else "null"}

Nearby document context:
{nearby_text if nearby_text else "null"}
""".strip()


def score_diagram_for_vision(item: DiagramVisionInput) -> DiagramVisionDecision:
    caption = (item.caption_text or "").strip().lower()
    nearby_text = (item.nearby_text or "").strip().lower()
    combined_text = f"{caption}\n{nearby_text}".strip()
    score = 0.0
    reasons: list[str] = []

    kind = str(item.diagram_kind or "").strip().lower()
    if kind in {"chart", "figure"}:
        score += 0.22
        reasons.append(f"diagram kind '{kind}' is usually useful for vision QA")
    elif kind == "image":
        score -= 0.18
        reasons.append("diagram kind is generic image")
    elif kind and kind != "unknown":
        score += 0.12
        reasons.append(f"diagram kind '{kind}' indicates structured content")

    if caption:
        score += 0.10
        reasons.append("caption is available")
        if len(caption.split()) >= 5:
            score += 0.08
            reasons.append("caption has descriptive detail")

    positive_hits = sorted(keyword for keyword in _POSITIVE_DIAGRAM_KEYWORDS if keyword in combined_text)
    if positive_hits:
        score += min(0.28, 0.07 * len(positive_hits))
        reasons.append(f"diagram keywords found: {', '.join(positive_hits[:4])}")

    reference_hits = sorted(keyword for keyword in _REFERENCE_CONTEXT_KEYWORDS if keyword in nearby_text)
    if reference_hits:
        score += min(0.18, 0.06 * len(reference_hits))
        reasons.append(f"nearby text references figure semantics: {', '.join(reference_hits[:3])}")

    negative_hits = sorted(keyword for keyword in _NEGATIVE_IMAGE_KEYWORDS if keyword in combined_text)
    if negative_hits:
        score -= min(0.40, 0.12 * len(negative_hits))
        reasons.append(f"decorative/photo-like keywords found: {', '.join(negative_hits[:4])}")

    if caption and re.fullmatch(r"(figure|fig\.?)\s*\d+[a-z]?:?", caption, flags=re.IGNORECASE):
        score -= 0.10
        reasons.append("caption is only a bare figure label")

    if not caption and not nearby_text:
        score -= 0.12
        reasons.append("no caption or nearby context available")

    bounded_score = round(max(0.0, min(1.0, score)), 3)
    return DiagramVisionDecision(
        score=bounded_score,
        should_analyze=bounded_score >= DIAGRAM_VISION_MIN_SCORE,
        reasons=reasons or ["no strong evidence that vision analysis would add value"],
    )


def _guess_mime_type(path: str) -> str:
    mime, _ = mimetypes.guess_type(path)
    return mime or "image/png"


def _encode_file_base64(path: str) -> str:
    with open(path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode("utf-8")


def _clean_response_text(raw_text: str) -> str:
    text = (raw_text or "").strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    return text


def _extract_json_object_text(raw_text: str) -> str:
    text = _clean_response_text(raw_text)

    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end >= start:
        return text[start : end + 1]
    return text


def _normalize_doubled_quotes(text: str) -> str:
    if text.count('""') < 4:
        return text
    return text.replace('""', '"')


def _repair_json_text(text: str) -> str:
    repaired: list[str] = []
    in_string = False
    escape_next = False
    length = len(text)
    index = 0

    while index < length:
        char = text[index]

        if escape_next:
            repaired.append(char)
            escape_next = False
            index += 1
            continue

        if char == "\\":
            repaired.append(char)
            escape_next = True
            index += 1
            continue

        if char == '"':
            if in_string:
                look_ahead = index + 1
                while look_ahead < length and text[look_ahead] in " \t\r\n":
                    look_ahead += 1
                next_char = text[look_ahead] if look_ahead < length else ""
                if next_char and next_char not in {",", "}", "]", ":"}:
                    repaired.append('\\"')
                    index += 1
                    continue
            in_string = not in_string
            repaired.append(char)
            index += 1
            continue

        if in_string and char == "\n":
            repaired.append("\\n")
            index += 1
            continue

        if in_string and char == "\r":
            repaired.append("\\r")
            index += 1
            continue

        if in_string and char == "\t":
            repaired.append("\\t")
            index += 1
            continue

        repaired.append(char)
        index += 1

    return "".join(repaired)


def _parse_gemini_json_text(raw_text: str) -> tuple[dict[str, Any], dict[str, Any]]:
    parse_attempts = [
        ("direct_json", lambda text: _clean_response_text(text)),
        (
            "normalized_object",
            lambda text: _normalize_doubled_quotes(_extract_json_object_text(text)),
        ),
        (
            "repaired_json",
            lambda text: _repair_json_text(_normalize_doubled_quotes(_extract_json_object_text(text))),
        ),
    ]

    attempt_errors: list[str] = []
    last_preview = ""

    for attempt_number, (attempt_name, transform) in enumerate(parse_attempts, start=1):
        candidate = transform(raw_text)
        last_preview = candidate[:500]
        try:
            parsed = json.loads(candidate)
            return parsed, {
                "attempt_count": attempt_number,
                "attempt_name": attempt_name,
                "attempt_errors": attempt_errors,
                "normalized_preview": last_preview,
            }
        except JSONDecodeError as exc:
            attempt_errors.append(f"attempt {attempt_number} ({attempt_name}): {exc}")

    raise ValueError(
        "Gemini returned invalid JSON text after 3 local parse attempts. "
        f"Attempt errors: {' | '.join(attempt_errors)}. "
        f"Response text preview: {last_preview}"
    )


def _extract_candidate_metadata(raw_response: dict[str, Any]) -> dict[str, Any]:
    candidate = (raw_response.get("candidates") or [{}])[0]
    usage_metadata = raw_response.get("usageMetadata") or {}
    return {
        "finish_reason": candidate.get("finishReason"),
        "safety_ratings": candidate.get("safetyRatings") or [],
        "avg_logprobs": candidate.get("avgLogprobs"),
        "usage_metadata": usage_metadata,
    }


def resolve_image_path(storage_path: str) -> str:
    raw_path = str(storage_path or "").strip()
    if not raw_path:
        raise FileNotFoundError("Diagram image storage_path is empty")

    original = Path(raw_path)
    candidate_paths: list[Path] = []

    if original.is_absolute():
        candidate_paths.append(original)
    else:
        candidate_paths.append((Path.cwd() / original).resolve(strict=False))
        candidate_paths.append((DIAGRAM_ASSETS_BASE_DIR / original).resolve(strict=False))

        normalized_parts = original.parts
        if normalized_parts and normalized_parts[0].lower() == "uploads":
            candidate_paths.append((Path.cwd() / Path(*normalized_parts)).resolve(strict=False))
            candidate_paths.append((DIAGRAM_ASSETS_BASE_DIR / Path(*normalized_parts[1:])).resolve(strict=False))

    checked: list[str] = []
    seen: set[str] = set()
    for candidate in candidate_paths:
        candidate_str = str(candidate)
        if candidate_str in seen:
            continue
        seen.add(candidate_str)
        checked.append(candidate_str)
        if candidate.exists():
            return candidate_str

    raise FileNotFoundError(
        "Diagram image not found for storage_path "
        f"{raw_path!r}. Checked: {', '.join(checked)}"
    )


class GeminiDiagramVisionService:
    def __init__(self, *, api_key: str, model: str | None = None, timeout_seconds: int = 60) -> None:
        self.api_key = api_key
        self.model = model or _get_default_gemini_model()
        self.timeout_seconds = timeout_seconds

    def request_analysis(self, item: DiagramVisionInput) -> dict[str, Any]:
        VISION_QUEUE.wait_if_needed()
        resolved_image_path = resolve_image_path(item.image_path)
        image_path = Path(resolved_image_path)

        payload = {
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {"text": build_diagram_prompt(caption=item.caption_text, nearby_text=item.nearby_text)},
                        {
                            "inlineData": {
                                "mimeType": _guess_mime_type(str(image_path)),
                                "data": _encode_file_base64(str(image_path)),
                            }
                        },
                    ],
                }
            ],
            "generationConfig": {
                "responseMimeType": "application/json",
                "responseJsonSchema": DIAGRAM_RESPONSE_SCHEMA,
                "temperature": 0.1,
                "topP": 0.95,
                "maxOutputTokens": GEMINI_VISION_MAX_OUTPUT_TOKENS,
            },
        }

        url = f"{GEMINI_API_BASE}/models/{self.model}:generateContent?key={self.api_key}"
        req = request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as response:
                response_headers = _serialize_response_headers(getattr(response, "headers", None))
                raw = json.loads(response.read().decode("utf-8"))
        except error.HTTPError as http_error:
            details = http_error.read().decode("utf-8", errors="ignore")
            response_headers = _serialize_response_headers(getattr(http_error, "headers", None))
            raise DiagramVisionRequestError(
                message=f"Gemini request failed: {http_error.code} {details}",
                status_code=http_error.code,
                retry_after_seconds=_parse_retry_after(response_headers.get("Retry-After")),
                details={
                    "response_body": details[:1000],
                    "response_headers": response_headers,
                },
            ) from http_error

        text = (
            raw.get("candidates", [{}])[0]
            .get("content", {})
            .get("parts", [{}])[0]
            .get("text", "")
            .strip()
        )
        if not text:
            raise ValueError("Gemini returned no JSON text")

        return {
            "provider_name": "gemini",
            "model_name": self.model,
            "prompt_version": PROMPT_VERSION,
            "request_payload": payload,
            "raw_response": raw,
            "response_text": text,
            "candidate_metadata": _extract_candidate_metadata(raw),
            "response_headers": response_headers,
        }

    def parse_analysis_result(self, analysis_result: dict[str, Any]) -> dict[str, Any]:
        response_text = analysis_result.get("response_text", "")
        candidate_metadata = analysis_result.get("candidate_metadata") or {}
        finish_reason = str(candidate_metadata.get("finish_reason") or "").strip().upper()

        if finish_reason == "MAX_TOKENS":
            raise ValueError(
                "Gemini response was truncated by maxOutputTokens before JSON completed. "
                f"finish_reason={finish_reason}. Response text preview: {response_text[:500]}"
            )

        parsed_output, parse_metadata = _parse_gemini_json_text(response_text)
        enriched_result = dict(analysis_result)
        enriched_result["parsed_output"] = parsed_output
        enriched_result["parse_metadata"] = parse_metadata
        return enriched_result

    def analyze(self, item: DiagramVisionInput) -> dict[str, Any]:
        captured_result = self.request_analysis(item)
        return self.parse_analysis_result(captured_result)


def save_diagram_analysis(cur, *, block_id: str, image_asset_id: str | None, diagram_kind: str, analysis_result: dict[str, Any]) -> None:
    parsed = analysis_result["parsed_output"]
    analysis_run_id = str(uuid.uuid4())

    cur.execute(
        """
        INSERT INTO diagram_block_analysis_runs (
            analysis_run_id,
            block_id,
            provider_name,
            model_name,
            prompt_version,
            request_payload,
            raw_response,
            parsed_output,
            status,
            completed_at
        )
        VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, 'completed', NOW())
        """,
        (
            analysis_run_id,
            block_id,
            analysis_result["provider_name"],
            analysis_result["model_name"],
            analysis_result["prompt_version"],
            Json(analysis_result["request_payload"]),
            Json(analysis_result["raw_response"]),
            Json(parsed),
        ),
    )

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
            vision_confidence,
            provider_name,
            model_name,
            prompt_version,
            last_analyzed_at,
            updated_at
        )
        VALUES (
            %s, %s, %s, '{}'::jsonb, %s::jsonb, %s, %s::jsonb, %s::jsonb,
            'completed', %s, %s, %s, %s, NOW(), NOW()
        )
        ON CONFLICT (block_id)
        DO UPDATE SET
            image_asset_id = EXCLUDED.image_asset_id,
            diagram_kind = EXCLUDED.diagram_kind,
            ocr_text = EXCLUDED.ocr_text,
            visual_description = EXCLUDED.visual_description,
            semantic_links = EXCLUDED.semantic_links,
            question_answerable_facts = EXCLUDED.question_answerable_facts,
            vision_status = EXCLUDED.vision_status,
            vision_confidence = EXCLUDED.vision_confidence,
            provider_name = EXCLUDED.provider_name,
            model_name = EXCLUDED.model_name,
            prompt_version = EXCLUDED.prompt_version,
            last_analyzed_at = EXCLUDED.last_analyzed_at,
            updated_at = NOW()
        """,
        (
            block_id,
            image_asset_id,
            parsed.get("diagram_type") or diagram_kind or "unknown",
            Json(parsed.get("ocr_text", [])),
            parsed.get("summary"),
            Json(parsed.get("relationships", [])),
            Json(parsed.get("question_answerable_facts", [])),
            parsed.get("confidence"),
            analysis_result["provider_name"],
            analysis_result["model_name"],
            analysis_result["prompt_version"],
        ),
    )

    cur.execute(
        """
        UPDATE document_blocks
        SET
            normalized_content = COALESCE(normalized_content, '{}'::jsonb) || %s::jsonb,
            display_text = COALESCE(caption_text, display_text),
            updated_at = NOW()
        WHERE block_id = %s
        """,
        (
            Json(
                {
                    "vision": parsed,
                    "visual_description": parsed.get("summary"),
                    "ocr_text": parsed.get("ocr_text", []),
                    "semantic_links": parsed.get("relationships", []),
                    "vision_status": "completed",
                }
            ),
            block_id,
        ),
    )


def save_diagram_analysis_failure(
    cur,
    *,
    block_id: str,
    provider_name: str,
    model_name: str,
    prompt_version: str,
    request_payload: dict[str, Any],
    raw_response: dict[str, Any] | None = None,
    error_message: str,
) -> None:
    analysis_run_id = str(uuid.uuid4())

    cur.execute(
        """
        INSERT INTO diagram_block_analysis_runs (
            analysis_run_id,
            block_id,
            provider_name,
            model_name,
            prompt_version,
            request_payload,
            raw_response,
            status,
            error_message,
            completed_at
        )
        VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, 'failed', %s, NOW())
        """,
        (
            analysis_run_id,
            block_id,
            provider_name,
            model_name,
            prompt_version,
            Json(request_payload),
            Json(raw_response or {}),
            error_message,
        ),
    )

    cur.execute(
        """
        INSERT INTO diagram_block_details (
            block_id,
            vision_status,
            provider_name,
            model_name,
            prompt_version,
            updated_at
        )
        VALUES (%s, 'failed', %s, %s, %s, NOW())
        ON CONFLICT (block_id)
        DO UPDATE SET
            vision_status = 'failed',
            provider_name = EXCLUDED.provider_name,
            model_name = EXCLUDED.model_name,
            prompt_version = EXCLUDED.prompt_version,
            updated_at = NOW()
        """,
        (block_id, provider_name, model_name, prompt_version),
    )

    cur.execute(
        """
        UPDATE document_blocks
        SET
            normalized_content = COALESCE(normalized_content, '{}'::jsonb) || %s::jsonb,
            updated_at = NOW()
        WHERE block_id = %s
        """,
        (Json({"vision_status": "failed", "vision_error": error_message}), block_id),
    )


def save_diagram_analysis_skipped(
    cur,
    *,
    block_id: str,
    image_asset_id: str | None,
    diagram_kind: str,
    decision: DiagramVisionDecision,
) -> None:
    cur.execute(
        """
        INSERT INTO diagram_block_details (
            block_id,
            image_asset_id,
            diagram_kind,
            vision_status,
            vision_confidence,
            vision_gate_score,
            vision_gate_reasons,
            prompt_version,
            updated_at
        )
        VALUES (%s, %s, %s, 'skipped', %s, %s, %s::jsonb, %s, NOW())
        ON CONFLICT (block_id)
        DO UPDATE SET
            image_asset_id = EXCLUDED.image_asset_id,
            diagram_kind = EXCLUDED.diagram_kind,
            vision_status = 'skipped',
            vision_confidence = EXCLUDED.vision_confidence,
            vision_gate_score = EXCLUDED.vision_gate_score,
            vision_gate_reasons = EXCLUDED.vision_gate_reasons,
            prompt_version = EXCLUDED.prompt_version,
            updated_at = NOW()
        """,
        (
            block_id,
            image_asset_id,
            diagram_kind or "unknown",
            decision.score,
            decision.score,
            Json(decision.reasons),
            VISION_GATE_PROMPT_VERSION,
        ),
    )

    cur.execute(
        """
        UPDATE document_blocks
        SET
            normalized_content = COALESCE(normalized_content, '{}'::jsonb) || %s::jsonb,
            updated_at = NOW()
        WHERE block_id = %s
        """,
        (
            Json(
                {
                    "vision_status": "skipped",
                    "vision_gate_score": decision.score,
                    "vision_gate_reasons": decision.reasons,
                }
            ),
            block_id,
        ),
    )

def debug_diagram_pipeline(cur, document_id: str, api_key: str):
    items = fetch_pending_diagram_inputs(cur, document_id=document_id)
    print(f"Found {len(items)} pending diagram blocks")
    
    for item in items:
        try:
            resolved_path = resolve_image_path(item.image_path)
        except FileNotFoundError as exc:
            resolved_path = f"[missing] {exc}"
        print(f"\n--- Block: {item.block_id} ---")
        print(f"  image_path : {item.image_path}")
        print(f"  resolved   : {resolved_path}")
        print(f"  path exists: {Path(resolved_path).exists() if not resolved_path.startswith('[missing]') else False}")
        print(f"  caption    : {item.caption_text!r}")
        print(f"  nearby_text: {(item.nearby_text or '')[:80]!r}")
        
        decision = score_diagram_for_vision(item)
        print(f"  score      : {decision.score} (threshold: {DIAGRAM_VISION_MIN_SCORE})")
        print(f"  analyze?   : {decision.should_analyze}")
        print(f"  reasons    : {decision.reasons}")
        
def fetch_pending_diagram_inputs(
    cur,
    *,
    document_id: str,
    context_char_limit: int = 2000,
    block_ids: list[str] | None = None,
    statuses: list[str] | None = None,
) -> list[DiagramVisionInput]:
    normalized_block_ids = [str(block_id).strip() for block_id in (block_ids or []) if str(block_id).strip()]
    effective_statuses = list(statuses or ["pending_vision_analysis", "failed"])
    if statuses is None and _is_truthy_env(os.environ.get("DIAGRAM_VISION_INCLUDE_SKIPPED", "0")):
        effective_statuses.append("skipped")
    effective_statuses = [str(status).strip() for status in effective_statuses if str(status).strip()]

    cur.execute(
        f"""
        WITH ordered_blocks AS (
            SELECT
                db.block_id,
                db.block_type,
                db.display_text,
                db.caption_text,
                db.subtype,
                db.reading_order,
                db.source_unit_index,
                dba.block_asset_id,
                dba.storage_path,
                COALESCE(dbd.vision_status, 'pending_vision_analysis') AS vision_status,
                LAG(db.display_text) OVER (ORDER BY db.source_unit_index ASC, db.reading_order ASC NULLS LAST) AS prev_text,
                LEAD(db.display_text) OVER (ORDER BY db.source_unit_index ASC, db.reading_order ASC NULLS LAST) AS next_text
            FROM document_blocks db
            LEFT JOIN document_block_assets dba
                ON dba.block_id = db.block_id
               AND dba.asset_role = 'diagram_crop'
            LEFT JOIN diagram_block_details dbd
                ON dbd.block_id = db.block_id
            WHERE db.document_id = %s
              AND db.block_type = 'diagram'
              {"AND db.block_id = ANY(%s::uuid[])" if normalized_block_ids else ""}
        )
        SELECT
            block_id,
            block_asset_id,
            storage_path,
            caption_text,
            subtype,
            LEFT(CONCAT_WS('\n', prev_text, next_text), %s)
        FROM ordered_blocks
        WHERE COALESCE(storage_path, '') <> ''
          AND vision_status = ANY(%s)
        ORDER BY source_unit_index ASC, reading_order ASC NULLS LAST
        """,
        (
            (document_id, normalized_block_ids, context_char_limit, effective_statuses)
            if normalized_block_ids
            else (document_id, context_char_limit, effective_statuses)
        ),
    )

    results = []
    for block_id, block_asset_id, storage_path, caption_text, subtype, nearby_text in cur.fetchall():
        results.append(
            DiagramVisionInput(
                block_id=str(block_id),
                image_asset_id=str(block_asset_id) if block_asset_id else None,
                image_path=storage_path,
                caption_text=caption_text,
                nearby_text=nearby_text,
                diagram_kind=subtype or "figure",
            )
        )
    return results


def _empty_analysis_result() -> dict[str, Any]:
    return {
        "analyzed_block_ids": [],
        "failed_block_ids": [],
        "exhausted_block_ids": [],
        "failure_reason_by_block": {},
        "all_models_exhausted": False,
        "last_error": "",
    }


def run_diagram_analysis_for_document(
    cur,
    *,
    document_id: str,
    api_key: str | None = None,
    block_ids: list[str] | None = None,
    force_analyze: bool = False,
) -> dict[str, Any]:
    api_key = api_key or os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is required for diagram analysis")

    project_id = get_quota_project_id()
    outcome = _empty_analysis_result()

    fetch_statuses = ["pending_vision_analysis", "failed", "skipped"] if force_analyze else None
    for item in fetch_pending_diagram_inputs(
        cur,
        document_id=document_id,
        block_ids=block_ids,
        statuses=fetch_statuses,
    ):
        decision = score_diagram_for_vision(item)
        print(f"[SCORE] block={item.block_id} score={decision.score} analyze={decision.should_analyze}")
        print(f"        reasons={decision.reasons}")
        print(f"[PATH] Checking: {item.image_path}")
        try:
            resolved_path = resolve_image_path(item.image_path)
            print(f"[PATH] Resolved: {resolved_path}")
            print(f"[PATH] Exists: {Path(resolved_path).exists()}")
        except FileNotFoundError as exc:
            print(f"[PATH] Resolution failed: {exc}")
        if not force_analyze and not decision.should_analyze:
            save_diagram_analysis_skipped(
                cur,
                block_id=item.block_id,
                image_asset_id=item.image_asset_id,
                diagram_kind=item.diagram_kind,
                decision=decision,
            )
            continue
        result: dict[str, Any] | None = None
        selected_model = _get_default_gemini_model()
        all_models_exhausted_for_block = False
        try:
            fallback_errors: list[str] = []
            attempted_models: list[str] = []
            default_model = _get_default_gemini_model()
            ordered_models = get_task_models(TASK_TYPE_DIAGRAM_VISION, fallback_model=default_model)
            max_attempts = min(_get_max_diagram_vision_model_attempts(), max(1, len(ordered_models)))
            while True:
                if len(attempted_models) >= max_attempts:
                    raise RuntimeError(
                        "Diagram analysis failed after trying "
                        f"{len(attempted_models)} model(s): {' | '.join(fallback_errors)}"
                    )
                try:
                    selected_model = pick_available_model(
                        TASK_TYPE_DIAGRAM_VISION,
                        project_id=project_id,
                        fallback_model=default_model,
                        excluded_models=attempted_models,
                    )
                except QuotaRouterError as exc:
                    all_models_exhausted_for_block = True
                    raise RuntimeError(
                        f"{exc} Last quota failure: {fallback_errors[-1]}" if fallback_errors else str(exc)
                    ) from exc

                attempted_models.append(selected_model)
                service = GeminiDiagramVisionService(api_key=api_key, model=selected_model)
                try:
                    result = service.request_analysis(item)
                    result = service.parse_analysis_result(result)
                    record_model_success(
                        project_id=project_id,
                        model_name=selected_model,
                        request_count=1,
                        response_headers=result.get("response_headers") or {},
                    )
                    break
                except Exception as exc:
                    quota_error_code = classify_quota_error(
                        status_code=getattr(exc, "status_code", None),
                        message=str(exc),
                        details=getattr(exc, "details", None),
                    )
                    if quota_error_code:
                        record_quota_failure(
                            project_id=project_id,
                            model_name=selected_model,
                            error_code=quota_error_code,
                            retry_after_seconds=getattr(exc, "retry_after_seconds", None),
                            response_headers=getattr(exc, "details", {}).get("response_headers", {}) if hasattr(exc, "details") else {},
                        )
                    elif not _should_retry_with_another_model(exc):
                        raise
                    fallback_errors.append(f"{selected_model}: {exc}")
                    continue
                fallback_errors.append(f"{selected_model}: {exc}")
            save_diagram_analysis(
                cur,
                block_id=item.block_id,
                image_asset_id=item.image_asset_id,
                diagram_kind=item.diagram_kind,
                analysis_result=result,
            )
            outcome["analyzed_block_ids"].append(item.block_id)
        except Exception as exc:
            failure_message = str(exc)
            outcome["failed_block_ids"].append(item.block_id)
            outcome["failure_reason_by_block"][item.block_id] = failure_message
            outcome["last_error"] = failure_message
            if all_models_exhausted_for_block or "No compatible model is currently available" in failure_message:
                outcome["exhausted_block_ids"].append(item.block_id)
                outcome["all_models_exhausted"] = True
            failure_payload = {}
            failure_raw_response = None
            if result:
                failure_payload = dict(result.get("request_payload") or {})
                failure_payload["_gemini_response_text"] = result.get("response_text", "")
                if result.get("candidate_metadata"):
                    failure_payload["_candidate_metadata"] = result.get("candidate_metadata")
                if result.get("parse_metadata"):
                    failure_payload["_parse_metadata"] = result.get("parse_metadata")
                failure_raw_response = result.get("raw_response")
            save_diagram_analysis_failure(
                cur,
                block_id=item.block_id,
                provider_name="gemini",
                model_name=(result or {}).get("model_name") or selected_model,
                prompt_version=PROMPT_VERSION,
                request_payload=failure_payload,
                raw_response=failure_raw_response,
                error_message=failure_message,
            )

    return outcome
