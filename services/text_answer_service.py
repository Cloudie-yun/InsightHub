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

from services.quota_router import (
    TASK_TYPE_TEXT,
    QuotaRouterError,
    classify_quota_error,
    extract_response_headers,
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


class TextAnswerService:
    def __init__(self) -> None:
        self.api_key = (os.environ.get("GEMINI_API_KEY") or "").strip()
        self.default_model = str(os.environ.get("GEMINI_TEXT_MODEL") or "gemini-2.5-flash").strip()
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
    ) -> dict[str, Any]:
        if not self.api_key:
            raise TextAnswerServiceError(
                code="missing_api_key",
                message="Gemini API key is missing. Set GEMINI_API_KEY.",
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
        attempted_models: list[str] = []
        fallback_errors: list[TextAnswerServiceError] = []

        while True:
            try:
                selected_model = pick_available_model(
                    TASK_TYPE_TEXT,
                    project_id=project_id,
                    fallback_model=self.default_model,
                    excluded_models=attempted_models,
                )
            except QuotaRouterError as exc:
                last_error = fallback_errors[-1] if fallback_errors else None
                raise TextAnswerServiceError(
                    code="quota_router_unavailable",
                    message=str(exc),
                    retryable=True,
                    retry_after_seconds=last_error.retry_after_seconds if last_error else None,
                    details={
                        "attempted_models": attempted_models,
                        "last_error": last_error.to_dict() if last_error else None,
                    },
                ) from exc

            attempted_models.append(selected_model)
            payload = self._build_payload(
                query=query,
                retrieval_payload=retrieval_payload,
                selected_document_ids=selected_document_ids,
                conversation_context=conversation_context or [],
            )
            url = f"{GEMINI_API_BASE}/models/{selected_model}:generateContent?key={self.api_key}"

            try:
                raw_response, response_headers = self._post_json_with_backoff(url=url, payload=payload)
                response_text = (
                    raw_response.get("candidates", [{}])[0]
                    .get("content", {})
                    .get("parts", [{}])[0]
                    .get("text", "")
                    .strip()
                )
                if not response_text:
                    raise TextAnswerServiceError(
                        code="empty_provider_response",
                        message="Gemini returned no answer text.",
                        details={"raw_response_keys": list(raw_response.keys())},
                    )
                parsed = _parse_gemini_json_text(response_text)
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

                record_model_success(
                    project_id=project_id,
                    model_name=selected_model,
                    request_count=1,
                    token_count=self._resolve_token_count(raw_response),
                    response_headers=response_headers,
                )
                return {
                    "answer_text": answer_text,
                    "citation_block_ids": citation_block_ids,
                    "model_provider": "gemini",
                    "model_name": selected_model,
                    "prompt_version": PROMPT_VERSION,
                    "prompt_profile": self._select_prompt_profile(
                        query=query,
                        conversation_context=conversation_context or [],
                    )["label"],
                    "confidence": confidence,
                    "response_headers": response_headers,
                }
            except TextAnswerServiceError as exc:
                quota_error_code = classify_quota_error(
                    status_code=exc.status_code,
                    message=exc.message,
                    details=exc.details,
                )
                if not quota_error_code:
                    raise
                record_quota_failure(
                    project_id=project_id,
                    model_name=selected_model,
                    error_code=quota_error_code,
                    retry_after_seconds=exc.retry_after_seconds,
                    response_headers=extract_response_headers(exc.details),
                )
                fallback_errors.append(exc)

    def _build_payload(
        self,
        *,
        query: str,
        retrieval_payload: dict[str, Any],
        selected_document_ids: list[str],
        conversation_context: list[dict[str, str]],
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
            "Recent conversation context:\n"
            f"{chr(10).join(context_lines)}\n\n"
            if context_lines
            else ""
        )

        prompt_text = (
            "You are a grounded answer engine. You do not add pleasantries,\n"
            "filler phrases, or background knowledge. You answer only from\n"
            "the evidence chunks provided.\n\n"

            f"Answer style profile: {prompt_profile['label']}\n"
            f"Answer goal: {prompt_profile['goal']}\n"
            f"Preferred response shape: {prompt_profile['shape']}\n"
            f"Hard length limit: {prompt_profile['max_sentences']} sentences maximum.\n\n"

            # Only include this block if context exists
            + (
                "Recent conversation context (for resolving follow-up intent only):\n"
                f"{context_block}\n"
                "Do not treat the above as factual evidence.\n\n"
                if context_block else ""
            ) +

            f"User query: {query.strip()}\n\n"

            "Evidence chunks:\n"
            f"{chr(10).join(evidence_lines)}\n\n"

            "Rules:\n"
            "1. Ground every claim in the evidence. Do not infer,\n"
            "   extrapolate, or add background knowledge.\n"
            "2. Use conversation context only to interpret follow-up intent\n"
            "   such as 'simpler', 'more detail', or references to the\n"
            "   previous answer — never as factual evidence.\n"
            "3. Prefer paraphrase over direct quotes. If you quote,\n"
            "   use fewer than 15 words and mark with '...' if trimmed.\n"
            "4. If chunks conflict, surface the conflict explicitly in\n"
            "   answer_text rather than silently picking one.\n"
            "5. Follow the answer style profile unless doing so would force\n"
            "   unsupported detail — in that case, fall back to 'default'.\n"
            "6. If evidence is insufficient, set confidence to 'insufficient',\n"
            "   set citation_block_ids to [], and explain the gap in\n"
            "   answer_text with a suggested refinement.\n\n"

            "Return JSON only — no markdown fences, no preamble:\n"
            "{\n"
            '  "answer_text": "...",\n'
            '  "citation_block_ids": ["block_id_1", ...],\n'
            '  "confidence": "high" | "partial" | "insufficient"\n'
            "}"
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

    def _extract_usage_metadata(self, raw_response: dict[str, Any]) -> dict[str, Any]:
        usage_metadata = raw_response.get("usageMetadata")
        return usage_metadata if isinstance(usage_metadata, dict) else {}

    def _resolve_token_count(self, raw_response: dict[str, Any]) -> int:
        usage_metadata = self._extract_usage_metadata(raw_response)
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
