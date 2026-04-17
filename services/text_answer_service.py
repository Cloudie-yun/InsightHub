from __future__ import annotations

import ast
import email.utils
import json
import os
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from json import JSONDecodeError
from typing import Any
from urllib import error, request

try:
    from google import genai
    from google.genai.types import GenerateContentConfig, HttpOptions
except ImportError:  # pragma: no cover - optional until dependency is installed
    genai = None
    GenerateContentConfig = None
    HttpOptions = None

from services.quota_router import (
    TASK_TYPE_TEXT,
    QuotaRouterError,
    classify_quota_error,
    extract_response_headers,
    get_task_models,
    get_quota_project_id,
    pick_available_model,
    record_model_success,
    record_quota_failure,
)


GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta"
PROMPT_VERSION = "grounded_answer_v1"
NO_EVIDENCE_PROMPT_VERSION = "grounded_refusal_v1"
DEFAULT_MAX_OUTPUT_TOKENS = int(os.environ.get("GEMINI_TEXT_MAX_OUTPUT_TOKENS", "2048"))
MAX_CONVERSATION_CONTEXT_TURNS = 4

ANSWER_RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "answer_text": {"type": "string"},
        "citation_block_ids": {
            "type": "array",
            "items": {"type": "string"},
        },
        "confidence": {
            "type": "string",
            "enum": ["high", "partial", "insufficient"],
        },
    },
    "required": ["answer_text", "citation_block_ids", "confidence"],
}

DEFAULT_GROUNDED_ANSWER_PROMPT_TEMPLATE = """You are a grounded answer engine. You do not add pleasantries,
filler phrases, or background knowledge. You answer only from
the evidence chunks provided.

Answer style profile: {prompt_profile_label}
Answer goal: {prompt_profile_goal}
Preferred response shape: {prompt_profile_shape}
Hard length limit: {prompt_profile_max_sentences} sentences maximum.

{conversation_context_block}User query: {query}

Evidence chunks:
{evidence_chunks}

Rules:
1. Ground every claim in the evidence. Do not infer,
   extrapolate, or add background knowledge.
2. Use conversation context only to interpret follow-up intent
   such as 'simpler', 'more detail', or references to the
   previous answer -- never as factual evidence.
3. Prefer paraphrase over direct quotes. If you quote,
   use fewer than 15 words and mark with '...' if trimmed.
4. If chunks conflict, surface the conflict explicitly in
   answer_text rather than silently picking one.
5. Follow the answer style profile unless doing so would force
   unsupported detail -- in that case, fall back to 'default'.
6. If evidence is insufficient, set confidence to 'insufficient',
   set citation_block_ids to [], and explain the gap in
   answer_text with a suggested refinement.

Return JSON only -- no markdown fences, no preamble:
{{
  "answer_text": "...",
  "citation_block_ids": ["block_id_1", ...],
  "confidence": "high" | "partial" | "insufficient"
}}"""


def get_default_grounded_answer_prompt_template() -> str:
    return DEFAULT_GROUNDED_ANSWER_PROMPT_TEMPLATE

PROMPT_PROFILES = {
    "summary": {
        "label": "summary",
        "goal": "Produce a compact synthesis of the most important points from the evidence.",
        "shape": "1 short paragraph plus 3-5 bullet-like sentences if the material has multiple key points.",
        "max_sentences": 7,
    },
    "explanation": {
        "label": "explanation",
        "goal": "Explain the concept or process clearly, with enough depth to make the reasoning understandable.",
        "shape": "1-2 short paragraphs connecting cause, mechanism, and consequence when supported by evidence.",
        "max_sentences": 8,
    },
    "causal": {
        "label": "causal",
        "goal": "Answer why or how something happens, highlighting mechanisms and any uncertainty.",
        "shape": "1-2 short paragraphs with explicit cause-and-effect wording grounded in the evidence.",
        "max_sentences": 8,
    },
    "simplify": {
        "label": "simplify",
        "goal": "Restate the answer in simpler language for a non-expert without removing supported facts.",
        "shape": "2-5 short simple sentences using plain language and fewer technical terms.",
        "max_sentences": 5,
    },
    "detailed": {
        "label": "detailed",
        "goal": "Provide a fuller explanation including relevant supporting detail from the evidence.",
        "shape": "2 short paragraphs, or 1 short paragraph plus 3-5 concrete supporting points.",
        "max_sentences": 10,
    },
    "comparison": {
        "label": "comparison",
        "goal": "Compare relevant items, viewpoints, methods, or findings directly from the evidence.",
        "shape": "Short opening comparison statement followed by clearly separated comparison points.",
        "max_sentences": 8,
    },
    "definition": {
        "label": "definition",
        "goal": "Answer with a direct definition or characterization, then add the most relevant supporting detail.",
        "shape": "2-4 sentences: direct answer first, then concise support.",
        "max_sentences": 4,
    },
    "default": {
        "label": "default",
        "goal": "Answer the user directly and clearly using only the retrieved evidence.",
        "shape": "2-4 sentences for simple factual queries; up to 2 short paragraphs for multi-part questions.",
        "max_sentences": 6,
    },
}


def _is_truthy_env(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _get_default_vertex_text_model(default_model: str) -> str:
    return (
        str(os.environ.get("VERTEX_AI_TEXT_MODEL") or "").strip()
        or str(os.environ.get("VERTEX_AI_MODEL") or "").strip()
        or default_model
    )


def _is_vertex_ai_enabled() -> bool:
    return _is_truthy_env(os.environ.get("VERTEX_AI_ENABLED"))


def _get_vertex_ai_project() -> str:
    return (
        str(os.environ.get("VERTEX_AI_PROJECT") or "").strip()
        or str(os.environ.get("GOOGLE_CLOUD_PROJECT") or "").strip()
    )


def _get_vertex_ai_location() -> str:
    return str(os.environ.get("VERTEX_AI_LOCATION") or "global").strip() or "global"


def _get_text_provider_order(*, has_gemini_api_key: bool) -> list[str]:
    providers: list[str] = []
    if has_gemini_api_key:
        providers.append("gemini")
    if _is_vertex_ai_enabled():
        providers.append("vertex_ai")
    return providers


@dataclass
class TextAnswerServiceError(Exception):
    code: str
    message: str
    status_code: int = 503
    retryable: bool = True
    retry_after_seconds: float | None = None
    details: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "status_code": self.status_code,
            "retryable": self.retryable,
            "retry_after_seconds": self.retry_after_seconds,
            "details": self.details or {},
        }


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


def _normalize_smart_punctuation(text: str) -> str:
    replacements = {
        "\u201c": '"',
        "\u201d": '"',
        "\u2018": "'",
        "\u2019": "'",
        "\u2013": "-",
        "\u2014": "-",
        "\u2026": "...",
    }
    normalized = text
    for source, target in replacements.items():
        normalized = normalized.replace(source, target)
    return normalized


def _repair_json_text(text: str) -> str:
    repaired: list[str] = []
    in_string = False
    escape_next = False

    for char in text:
        if escape_next:
            repaired.append(char)
            escape_next = False
            continue

        if char == "\\":
            repaired.append(char)
            escape_next = True
            continue

        if char == '"':
            in_string = not in_string
            repaired.append(char)
            continue

        if in_string and char == "\n":
            repaired.append("\\n")
            continue

        if in_string and char == "\r":
            repaired.append("\\r")
            continue

        if in_string and char == "\t":
            repaired.append("\\t")
            continue

        repaired.append(char)

    return "".join(repaired)


def _strip_trailing_commas(text: str) -> str:
    return re.sub(r",(\s*[}\]])", r"\1", text)


def _extract_citation_block_ids(text: str) -> list[str]:
    ids: list[str] = []
    seen: set[str] = set()
    for match in re.finditer(r"block[_\s-]?id[s]?\s*[:=]\s*(.+)", text, flags=re.IGNORECASE):
        tail = match.group(1)
        for block_id in re.findall(r"[A-Za-z0-9._:-]+", tail):
            normalized = str(block_id).strip()
            if not normalized or normalized.lower() in {"block", "id", "ids"}:
                continue
            if normalized not in seen:
                seen.add(normalized)
                ids.append(normalized)
    return ids


def _extract_confidence_value(text: str) -> str:
    match = re.search(
        r"confidence\s*[:=]\s*[\"']?(high|partial|insufficient)[\"']?",
        text,
        flags=re.IGNORECASE,
    )
    return str(match.group(1)).lower() if match else ""


def _fallback_answer_payload(raw_text: str) -> dict[str, Any]:
    cleaned_text = _clean_response_text(raw_text)
    object_text = _extract_json_object_text(cleaned_text)
    answer_match = re.search(
        r"answer_text\s*[:=]\s*[\"'](?P<answer>.*?)(?<!\\)[\"']\s*(?:,|$)",
        object_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    answer_text = ""
    if answer_match:
        answer_text = answer_match.group("answer").strip()
    if not answer_text:
        answer_text = cleaned_text.strip()
    return {
        "answer_text": answer_text,
        "citation_block_ids": _extract_citation_block_ids(object_text),
        "confidence": _extract_confidence_value(object_text) or "partial",
    }


def _parse_gemini_json_text(raw_text: str) -> dict[str, Any]:
    normalized_text = _normalize_smart_punctuation(raw_text)
    parse_attempts = [
        lambda text: _clean_response_text(text),
        lambda text: _normalize_doubled_quotes(_extract_json_object_text(text)),
        lambda text: _strip_trailing_commas(_normalize_doubled_quotes(_extract_json_object_text(text))),
        lambda text: _repair_json_text(_strip_trailing_commas(_normalize_doubled_quotes(_extract_json_object_text(text)))),
    ]
    errors: list[str] = []
    preview = ""
    for transform in parse_attempts:
        candidate = transform(normalized_text)
        preview = candidate[:500]
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                return parsed
        except JSONDecodeError as exc:
            errors.append(str(exc))
        try:
            parsed = ast.literal_eval(candidate)
            if isinstance(parsed, dict):
                return parsed
        except (ValueError, SyntaxError) as exc:
            errors.append(str(exc))

    fallback_payload = _fallback_answer_payload(normalized_text)
    if str(fallback_payload.get("answer_text") or "").strip():
        return fallback_payload

    raise TextAnswerServiceError(
        code="invalid_provider_response",
        message="Gemini returned invalid grounded-answer JSON.",
        details={
            "parse_errors": errors,
            "response_preview": preview,
        },
    )


def _serialize_headers(headers: Any) -> dict[str, str]:
    if not headers:
        return {}
    items = list(headers.items()) if hasattr(headers, "items") else []
    selected_headers: dict[str, str] = {}
    allowed_names = {
        "retry-after",
        "x-ratelimit-limit-requests",
        "x-ratelimit-remaining-requests",
        "x-ratelimit-reset-requests",
        "x-ratelimit-limit-tokens",
        "x-ratelimit-remaining-tokens",
        "x-ratelimit-reset-tokens",
    }
    for key, value in items:
        normalized_key = str(key or "").strip()
        if not normalized_key or normalized_key.lower() not in allowed_names:
            continue
        selected_headers[normalized_key] = str(value or "").strip()[:200]
    return selected_headers


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


def _safe_model_dump(payload: Any) -> Any:
    if payload is None:
        return None
    model_dump = getattr(payload, "model_dump", None)
    if callable(model_dump):
        try:
            return model_dump()
        except TypeError:
            return model_dump(mode="json")
    to_json_dict = getattr(payload, "to_json_dict", None)
    if callable(to_json_dict):
        return to_json_dict()
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, (list, tuple)):
        return [_safe_model_dump(item) for item in payload]
    if isinstance(payload, (str, int, float, bool)):
        return payload
    return {"repr": repr(payload)}


def _extract_text_from_parts(parts: Any) -> str:
    extracted_text: list[str] = []
    for part in parts or []:
        if isinstance(part, dict):
            text = str(part.get("text") or "").strip()
        else:
            text = str(getattr(part, "text", "") or "").strip()
        if text:
            extracted_text.append(text)
    return "\n".join(extracted_text).strip()


def _extract_vertex_candidate_metadata(response: Any) -> dict[str, Any]:
    response_dict = _safe_model_dump(response) or {}
    candidates = response_dict.get("candidates") or []
    candidate = candidates[0] if candidates else {}
    finish_reason = candidate.get("finish_reason") or candidate.get("finishReason")
    return {
        "finish_reason": str(finish_reason or "").strip().split(".")[-1],
        "usage_metadata": response_dict.get("usage_metadata") or response_dict.get("usageMetadata") or {},
    }


class TextAnswerService:
    def __init__(self) -> None:
        self.api_key = (os.environ.get("GEMINI_API_KEY") or "").strip()
        self.default_model = str(os.environ.get("GEMINI_TEXT_MODEL") or "gemini-2.5-flash").strip()
        self.vertex_default_model = _get_default_vertex_text_model(self.default_model)
        self.vertex_project = _get_vertex_ai_project()
        self.vertex_location = _get_vertex_ai_location()
        self.max_output_tokens = max(256, DEFAULT_MAX_OUTPUT_TOKENS)
        self.temperature = float(os.environ.get("GEMINI_TEXT_TEMPERATURE", "0.2"))
        self.top_p = float(os.environ.get("GEMINI_TEXT_TOP_P", "0.95"))
        self.timeout_seconds = max(15, int(os.environ.get("GEMINI_TEXT_TIMEOUT_SECONDS", "60")))
        self.max_retries = max(1, int(os.environ.get("GEMINI_TEXT_MAX_RETRIES", "4")))
        self.backoff_base_seconds = max(
            0.5,
            float(os.environ.get("GEMINI_TEXT_BACKOFF_BASE_SECONDS", "2.0")),
        )

    def generate_grounded_answer(
        self,
        *,
        query: str,
        retrieval_payload: dict[str, Any],
        selected_document_ids: list[str],
        conversation_context: list[dict[str, str]] | None = None,
        user_prompt_override: str | None = None,
    ) -> dict[str, Any]:
        provider_order = _get_text_provider_order(has_gemini_api_key=bool(self.api_key))
        if not provider_order:
            raise TextAnswerServiceError(
                code="text_provider_unavailable",
                message="Grounded Q&A requires GEMINI_API_KEY or Vertex AI to be enabled with ADC and project configuration.",
                retryable=False,
                details={},
            )

        evidence_results = retrieval_payload.get("results") if isinstance(retrieval_payload, dict) else []
        evidence_results = evidence_results if isinstance(evidence_results, list) else []
        if not evidence_results:
            raise TextAnswerServiceError(
                code="no_evidence",
                message="No retrieval evidence is available for grounded answer generation.",
                status_code=400,
                retryable=False,
                details={},
            )

        project_id = get_quota_project_id()
        configured_models = get_task_models(TASK_TYPE_TEXT, fallback_model=self.default_model)
        attempted_models: list[str] = []
        last_error: TextAnswerServiceError | None = None

        while len(attempted_models) < len(configured_models):
            try:
                selected_model = pick_available_model(
                    TASK_TYPE_TEXT,
                    project_id=project_id,
                    fallback_model=self.default_model,
                    excluded_models=attempted_models,
                )
            except QuotaRouterError as exc:
                if last_error is not None:
                    raise last_error
                raise TextAnswerServiceError(
                    code="quota_router_unavailable",
                    message=str(exc),
                    retryable=True,
                    retry_after_seconds=None,
                    details={
                        "attempted_models": attempted_models,
                        "last_error": None,
                    },
                ) from exc

            attempted_models.append(selected_model)
            payload = self._build_payload(
                query=query,
                retrieval_payload=retrieval_payload,
                selected_document_ids=selected_document_ids,
                conversation_context=conversation_context or [],
                prompt_template=user_prompt_override or "",
            )
            prompt_profile = self._select_prompt_profile(
                query=query,
                conversation_context=conversation_context or [],
            )["label"]

            for provider_name in provider_order:
                try:
                    if provider_name == "gemini":
                        raw_response, response_headers, candidate_metadata = self._request_with_gemini(
                            model_name=selected_model,
                            payload=payload,
                        )
                    elif provider_name == "vertex_ai":
                        raw_response, response_headers, candidate_metadata = self._request_with_vertex_ai(
                            model_name=selected_model,
                            payload=payload,
                        )
                    else:
                        continue

                    parsed = self._parse_provider_payload(
                        provider_name=provider_name,
                        raw_response=raw_response,
                        candidate_metadata=candidate_metadata,
                    )
                    answer_text = str(parsed.get("answer_text") or "").strip()
                    if not answer_text:
                        raise TextAnswerServiceError(
                            code="empty_answer_text",
                            message="Grounded answer generation returned an empty answer.",
                            details={},
                        )
                    citation_block_ids = [
                        str(item).strip()
                        for item in (parsed.get("citation_block_ids") or [])
                        if str(item).strip()
                    ]
                    confidence = str(parsed.get("confidence") or "").strip().lower()
                    if confidence not in {"high", "partial", "insufficient"}:
                        confidence = "partial" if citation_block_ids else "insufficient"

                    if provider_name == "gemini":
                        record_model_success(
                            project_id=project_id,
                            model_name=selected_model,
                            request_count=1,
                            token_count=self._resolve_token_count(raw_response, candidate_metadata=candidate_metadata),
                            response_headers=response_headers,
                        )

                    return {
                        "answer_text": answer_text,
                        "citation_block_ids": citation_block_ids,
                        "model_provider": provider_name,
                        "model_name": selected_model if provider_name == "gemini" else (str(raw_response.get("modelVersion") or raw_response.get("model") or self.vertex_default_model).strip() or self.vertex_default_model),
                        "prompt_version": PROMPT_VERSION,
                        "prompt_profile": prompt_profile,
                        "confidence": confidence,
                        "response_headers": response_headers,
                    }
                except TextAnswerServiceError as exc:
                    last_error = exc
                    if provider_name == "gemini":
                        quota_error_code = classify_quota_error(
                            status_code=exc.status_code,
                            message=exc.message,
                            details=exc.details,
                        )
                        if quota_error_code:
                            record_quota_failure(
                                project_id=project_id,
                                model_name=selected_model,
                                error_code=quota_error_code,
                                retry_after_seconds=exc.retry_after_seconds,
                                response_headers=extract_response_headers(exc.details),
                            )

        if last_error is not None:
            raise last_error
        raise TextAnswerServiceError(
            code="text_provider_unavailable",
            message="Grounded Q&A is unavailable because no text provider could be used.",
            retryable=True,
            details={},
        )

    def _build_payload(
        self,
        *,
        query: str,
        retrieval_payload: dict[str, Any],
        selected_document_ids: list[str],
        conversation_context: list[dict[str, str]],
        prompt_template: str,
    ) -> dict[str, Any]:
        evidence_lines: list[str] = []
        for result in retrieval_payload.get("results") or []:
            source_metadata = result.get("source_metadata") if isinstance(result.get("source_metadata"), dict) else {}
            page_value = source_metadata.get("page") or source_metadata.get("page_number") or source_metadata.get("page_index")
            source_name = str(result.get("document_name") or result.get("document_id") or "Unknown source").strip()
            chunk_text = str(result.get("snippet") or "").strip()
            if page_value not in (None, ""):
                source_name = f"{source_name}, {page_value}"
            evidence_lines.append(
                f"[block_id: {result.get('block_id') or ''} | source: {source_name}] {chunk_text}"
            )

        prompt_profile = self._select_prompt_profile(
            query=query,
            conversation_context=conversation_context,
        )
        context_lines = self._format_conversation_context(conversation_context)
        context_block = (
            "Recent conversation context (for resolving follow-up intent only):\n"
            f"{chr(10).join(context_lines)}\n"
            "Do not treat the above as factual evidence.\n\n"
            if context_lines
            else ""
        )
        template = str(prompt_template or "").strip() or DEFAULT_GROUNDED_ANSWER_PROMPT_TEMPLATE
        try:
            prompt_text = template.format(
                prompt_profile_label=prompt_profile["label"],
                prompt_profile_goal=prompt_profile["goal"],
                prompt_profile_shape=prompt_profile["shape"],
                prompt_profile_max_sentences=prompt_profile["max_sentences"],
                conversation_context_block=context_block,
                query=query.strip(),
                evidence_chunks=chr(10).join(evidence_lines),
            )
        except KeyError:
            prompt_text = DEFAULT_GROUNDED_ANSWER_PROMPT_TEMPLATE.format(
                prompt_profile_label=prompt_profile["label"],
                prompt_profile_goal=prompt_profile["goal"],
                prompt_profile_shape=prompt_profile["shape"],
                prompt_profile_max_sentences=prompt_profile["max_sentences"],
                conversation_context_block=context_block,
                query=query.strip(),
                evidence_chunks=chr(10).join(evidence_lines),
            )
        return {
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {
                            "text": prompt_text,
                        },
                    ],
                },
            ],
            "generationConfig": {
                "responseMimeType": "application/json",
                "responseSchema": ANSWER_RESPONSE_SCHEMA,
                "temperature": self.temperature,
                "topP": self.top_p,
                "maxOutputTokens": self.max_output_tokens,
            },
        }

    def _request_with_gemini(
        self,
        *,
        model_name: str,
        payload: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, str], dict[str, Any]]:
        if not self.api_key:
            raise TextAnswerServiceError(
                code="missing_api_key",
                message="Gemini API key is missing. Set GEMINI_API_KEY.",
                retryable=False,
                details={},
            )
        url = f"{GEMINI_API_BASE}/models/{model_name}:generateContent?key={self.api_key}"
        raw_response, response_headers = self._post_json_with_backoff(url=url, payload=payload)
        candidate_metadata = (
            raw_response.get("candidates", [{}])[0]
            if isinstance(raw_response.get("candidates"), list) and raw_response.get("candidates")
            else {}
        )
        return raw_response, response_headers, candidate_metadata

    def _request_with_vertex_ai(
        self,
        *,
        model_name: str,
        payload: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, str], dict[str, Any]]:
        if genai is None or GenerateContentConfig is None or HttpOptions is None:
            raise TextAnswerServiceError(
                code="vertex_ai_unavailable",
                message="google-genai is not installed; Vertex AI grounded Q&A fallback is unavailable.",
                retryable=False,
                details={},
            )
        if not self.vertex_project:
            raise TextAnswerServiceError(
                code="vertex_ai_misconfigured",
                message="VERTEX_AI_PROJECT or GOOGLE_CLOUD_PROJECT is required for Vertex AI grounded Q&A.",
                retryable=False,
                details={},
            )

        client = genai.Client(
            vertexai=True,
            project=self.vertex_project,
            location=self.vertex_location,
            http_options=HttpOptions(api_version="v1"),
        )
        try:
            response = client.models.generate_content(
                model=model_name or self.vertex_default_model,
                contents=payload.get("contents") or [],
                config=GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=ANSWER_RESPONSE_SCHEMA,
                    temperature=self.temperature,
                    top_p=self.top_p,
                    max_output_tokens=self.max_output_tokens,
                ),
            )
        except Exception as exc:
            status_code = int(getattr(exc, "code", 0) or getattr(exc, "status_code", 0) or 0) or None
            retry_after_seconds = _parse_retry_after(
                getattr(exc, "response", None) and getattr(getattr(exc, "response", None), "headers", {}).get("Retry-After")
            )
            raise TextAnswerServiceError(
                code="vertex_ai_request_failed",
                message=f"Vertex AI request failed: {exc}",
                status_code=status_code or 503,
                retryable=bool(status_code == 429 or (status_code and 500 <= status_code < 600)),
                retry_after_seconds=retry_after_seconds,
                details={
                    "response_body": str(exc)[:1000],
                    "response_headers": {},
                    "exception_type": exc.__class__.__name__,
                },
            ) from exc

        response_dict = _safe_model_dump(response) or {}
        return response_dict, {}, _extract_vertex_candidate_metadata(response)

    def _parse_provider_payload(
        self,
        *,
        provider_name: str,
        raw_response: dict[str, Any],
        candidate_metadata: dict[str, Any],
    ) -> dict[str, Any]:
        response_text = ""
        candidates = raw_response.get("candidates") if isinstance(raw_response.get("candidates"), list) else []
        first_candidate = candidates[0] if candidates else {}
        if provider_name == "gemini":
            response_text = (
                first_candidate
                .get("content", {})
                .get("parts", [{}])[0]
                .get("text", "")
                .strip()
            )
        else:
            response_text = str(raw_response.get("text") or "").strip()
            if not response_text:
                response_text = _extract_text_from_parts(
                    (first_candidate.get("content") or {}).get("parts") or []
                )

        if not response_text:
            provider_label = "Vertex AI" if provider_name == "vertex_ai" else "Gemini"
            raise TextAnswerServiceError(
                code="empty_provider_response",
                message=f"{provider_label} returned no answer text.",
                details={"raw_response_keys": list(raw_response.keys())},
            )

        finish_reason = str(candidate_metadata.get("finish_reason") or "").strip().upper()
        if finish_reason == "MAX_TOKENS":
            provider_label = "Vertex AI" if provider_name == "vertex_ai" else "Gemini"
            raise TextAnswerServiceError(
                code="truncated_provider_response",
                message=f"{provider_label} response was truncated before JSON completed.",
                details={"response_preview": response_text[:500]},
            )

        return _parse_gemini_json_text(response_text)

    def _select_prompt_profile(
        self,
        *,
        query: str,
        conversation_context: list[dict[str, str]],
    ) -> dict[str, str]:
        haystack = " ".join(
            [
                str(item.get("content") or "").strip()
                for item in conversation_context[-2:]
                if isinstance(item, dict)
            ] + [str(query or "").strip()]
        ).lower()
        query_text = str(query or "").strip().lower()

        if any(marker in query_text for marker in ("summarize", "summary", "key points", "overview")):
            return PROMPT_PROFILES["summary"]
        if any(marker in query_text for marker in ("compare", "difference", "different", "versus", "vs")):
            return PROMPT_PROFILES["comparison"]
        if any(marker in query_text for marker in ("why ", "why does", "why is", "how does", "how did", "cause", "reason")):
            return PROMPT_PROFILES["causal"]
        if any(marker in haystack for marker in ("simpler", "simplify", "easier to understand", "plain english", "layman")):
            return PROMPT_PROFILES["simplify"]
        if any(marker in haystack for marker in ("more detail", "more detailed", "elaborate", "expand on", "go deeper")):
            return PROMPT_PROFILES["detailed"]
        if any(marker in query_text for marker in ("explain", "walk me through", "what does this mean")):
            return PROMPT_PROFILES["explanation"]
        if query_text.startswith(("what is", "define", "how is", "what does")):
            return PROMPT_PROFILES["definition"]
        return PROMPT_PROFILES["default"]

    def _format_conversation_context(self, conversation_context: list[dict[str, str]]) -> list[str]:
        formatted: list[str] = []
        for item in conversation_context[-MAX_CONVERSATION_CONTEXT_TURNS:]:
            if not isinstance(item, dict):
                continue
            role = str(item.get("role") or "").strip().lower()
            content = str(item.get("content") or "").strip()
            if role not in {"user", "assistant"} or not content:
                continue
            label = "User" if role == "user" else "Assistant"
            formatted.append(f"{label}: {content[:500]}")
        return formatted

    def _post_json(self, *, url: str, payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, str]]:
        req = request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as response:
                response_headers = _serialize_headers(getattr(response, "headers", None))
                raw = json.loads(response.read().decode("utf-8"))
                return raw, response_headers
        except error.HTTPError as http_error:
            details = http_error.read().decode("utf-8", errors="ignore")
            response_headers = _serialize_headers(getattr(http_error, "headers", None))
            retryable = http_error.code == 429 or 500 <= http_error.code < 600
            raise TextAnswerServiceError(
                code="provider_http_error",
                message=f"Gemini text request failed: {http_error.code}",
                status_code=http_error.code,
                retryable=retryable,
                retry_after_seconds=_parse_retry_after(response_headers.get("Retry-After")),
                details={
                    "response_body": details[:1000],
                    "response_headers": response_headers,
                },
            ) from http_error
        except error.URLError as exc:
            raise TextAnswerServiceError(
                code="provider_connection_error",
                message="Could not connect to Gemini text provider.",
                retryable=True,
                details={"reason": str(exc.reason)},
            ) from exc
        except TimeoutError as exc:
            raise TextAnswerServiceError(
                code="provider_timeout",
                message="Gemini text request timed out.",
                retryable=True,
                details={},
            ) from exc

    def _post_json_with_backoff(
        self,
        *,
        url: str,
        payload: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, str]]:
        last_error: TextAnswerServiceError | None = None
        for attempt in range(self.max_retries):
            try:
                return self._post_json(url=url, payload=payload)
            except TextAnswerServiceError as exc:
                last_error = exc
                if not exc.retryable or attempt >= self.max_retries - 1:
                    raise
                wait_seconds = exc.retry_after_seconds
                if wait_seconds is None:
                    wait_seconds = (self.backoff_base_seconds * (2**attempt)) + random.uniform(0.0, 1.0)
                time.sleep(max(0.0, wait_seconds))
        if last_error is not None:
            raise last_error
        raise TextAnswerServiceError(
            code="provider_retry_exhausted",
            message="Gemini text retries were exhausted.",
            details={},
        )

    def _extract_usage_metadata(
        self,
        raw_response: dict[str, Any],
        *,
        candidate_metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        usage_metadata = raw_response.get("usageMetadata") or raw_response.get("usage_metadata")
        if not isinstance(usage_metadata, dict) and isinstance(candidate_metadata, dict):
            usage_metadata = candidate_metadata.get("usage_metadata") or candidate_metadata.get("usageMetadata")
        return usage_metadata if isinstance(usage_metadata, dict) else {}

    def _resolve_token_count(
        self,
        raw_response: dict[str, Any],
        *,
        candidate_metadata: dict[str, Any] | None = None,
    ) -> int:
        usage_metadata = self._extract_usage_metadata(raw_response, candidate_metadata=candidate_metadata)
        total_token_count = usage_metadata.get("totalTokenCount")
        if isinstance(total_token_count, (int, float)):
            return max(0, int(total_token_count))

        prompt_token_count = usage_metadata.get("promptTokenCount")
        candidate_token_count = usage_metadata.get("candidatesTokenCount")
        if isinstance(prompt_token_count, (int, float)) or isinstance(candidate_token_count, (int, float)):
            return max(0, int(prompt_token_count or 0) + int(candidate_token_count or 0))

        return 0


def build_no_evidence_payload(*, retrieval_payload: dict[str, Any]) -> dict[str, Any]:
    returned_count = int(retrieval_payload.get("returned_count") or 0)
    strategy = str(retrieval_payload.get("strategy") or "retrieval").replace("_", " ")
    if returned_count > 0:
        message = (
            "I found some related material, but it does not provide enough direct support to answer confidently "
            "from the selected documents. Try narrowing the question or changing the selected sources."
        )
    else:
        message = (
            f"I could not find enough evidence in the selected documents to answer this question via {strategy}. "
            "Try selecting different documents or rephrasing the question."
        )
    return {
        "answer_text": message,
        "citation_block_ids": [],
        "model_provider": "",
        "model_name": "",
        "prompt_version": NO_EVIDENCE_PROMPT_VERSION,
        "prompt_profile": "default",
        "confidence": "insufficient",
        "response_headers": {},
    }
