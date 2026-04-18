from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any
from urllib import error, request

from services.gemini_credentials import (
    GeminiApiCredential,
    build_quota_project_id_for_credential,
    load_gemini_api_credentials,
)
from services.quota_router import (
    QuotaRouterError,
    TASK_TYPE_TEXT,
    get_quota_project_id,
    execute_with_shared_quota_router,
    resolve_usage_token_count,
)


GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta"
DOCUMENT_SUMMARY_PROMPT_VERSION = "document_summary_v1"
CONVERSATION_SUMMARY_PROMPT_VERSION = "conversation_summary_v1"
CONVERSATION_TITLE_PROMPT_VERSION = "conversation_title_v1"
DEFAULT_SUMMARY_MODEL = str(
    os.environ.get("SUMMARY_TEXT_MODEL")
    or os.environ.get("GEMINI_TEXT_MODEL")
    or "gemini-2.5-flash"
).strip()
DEFAULT_TIMEOUT_SECONDS = int(os.environ.get("SUMMARY_TIMEOUT_SECONDS", "90"))
DEFAULT_DOCUMENT_MAX_OUTPUT_TOKENS = int(os.environ.get("DOCUMENT_SUMMARY_MAX_OUTPUT_TOKENS", "2048"))
DEFAULT_CONVERSATION_MAX_OUTPUT_TOKENS = int(os.environ.get("CONVERSATION_SUMMARY_MAX_OUTPUT_TOKENS", "2048"))
DEFAULT_TITLE_MAX_OUTPUT_TOKENS = int(os.environ.get("CONVERSATION_TITLE_MAX_OUTPUT_TOKENS", "256"))
DEFAULT_CHUNK_TARGET_TOKENS = int(os.environ.get("SUMMARY_CHUNK_TARGET_TOKENS", "18000"))
DEFAULT_MERGE_BATCH_TARGET_TOKENS = int(os.environ.get("SUMMARY_MERGE_BATCH_TARGET_TOKENS", "12000"))
DEFAULT_MAX_MODEL_ATTEMPTS = int(os.environ.get("SUMMARY_MAX_MODEL_ATTEMPTS", "2"))

DOCUMENT_SUMMARY_RESPONSE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["summary_text", "key_points", "topics", "title_hint"],
    "properties": {
        "summary_text": {"type": "string"},
        "key_points": {"type": "array", "items": {"type": "string"}},
        "topics": {"type": "array", "items": {"type": "string"}},
        "title_hint": {"type": "string"},
    },
}

CONVERSATION_SUMMARY_RESPONSE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["summary_text", "key_points", "topics"],
    "properties": {
        "summary_text": {"type": "string"},
        "key_points": {"type": "array", "items": {"type": "string"}},
        "topics": {"type": "array", "items": {"type": "string"}},
    },
}

CONVERSATION_TITLE_RESPONSE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["title"],
    "properties": {
        "title": {"type": "string"},
    },
}


def _estimated_token_count(value: str) -> int:
    normalized = str(value or "")
    return max(1, (len(normalized) + 3) // 4)


def _normalize_text(value: Any) -> str:
    text = str(value or "").replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    return "\n".join(line.strip() for line in text.split("\n")).strip()


def _normalize_string_list(values: Any, *, max_items: int, max_chars: int) -> list[str]:
    if not isinstance(values, list):
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for item in values:
        text = _normalize_text(item)
        if not text:
            continue
        if len(text) > max_chars:
            text = text[:max_chars].rsplit(" ", 1)[0].strip() or text[:max_chars].strip()
        dedupe_key = text.casefold()
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        normalized.append(text)
        if len(normalized) >= max_items:
            break
    return normalized


def _normalize_title(value: Any, *, max_chars: int = 120) -> str:
    title = re.sub(r"\s+", " ", str(value or "").replace("_", " ").replace("-", " ")).strip(" .:-")
    if len(title) > max_chars:
        title = title[:max_chars].rsplit(" ", 1)[0].strip() or title[:max_chars].strip()
    return title


def _parse_json_response_text(raw_text: str) -> dict[str, Any]:
    text = str(raw_text or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        parsed = json.loads(match.group(0))
        if isinstance(parsed, dict):
            return parsed
    raise ValueError("Gemini returned invalid JSON for summary generation.")


@dataclass
class SummaryServiceError(Exception):
    code: str
    message: str
    status_code: int = 503
    retryable: bool = True
    retry_after_seconds: float | None = None
    details: dict[str, Any] | None = None


class GeminiSummaryService:
    def __init__(
        self,
        *,
        api_key: str | None = None,
        default_model: str = DEFAULT_SUMMARY_MODEL,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
        chunk_target_tokens: int = DEFAULT_CHUNK_TARGET_TOKENS,
        merge_batch_target_tokens: int = DEFAULT_MERGE_BATCH_TARGET_TOKENS,
        max_model_attempts: int = DEFAULT_MAX_MODEL_ATTEMPTS,
    ) -> None:
        explicit_key = str(api_key or "").strip()
        self.gemini_credentials = load_gemini_api_credentials()
        if explicit_key:
            self.gemini_credentials = [credential for credential in self.gemini_credentials if credential.api_key == explicit_key] or []
        self.api_key = explicit_key or (self.gemini_credentials[0].api_key if self.gemini_credentials else "")
        if explicit_key and not self.gemini_credentials:
            self.gemini_credentials = [
                GeminiApiCredential(alias="gemini_key_inline", api_key=explicit_key)
            ]
        self.default_model = str(default_model or DEFAULT_SUMMARY_MODEL).strip() or DEFAULT_SUMMARY_MODEL
        self.timeout_seconds = max(10, int(timeout_seconds or DEFAULT_TIMEOUT_SECONDS))
        self.chunk_target_tokens = max(2000, int(chunk_target_tokens or DEFAULT_CHUNK_TARGET_TOKENS))
        self.merge_batch_target_tokens = max(1000, int(merge_batch_target_tokens or DEFAULT_MERGE_BATCH_TARGET_TOKENS))
        self.max_model_attempts = max(1, int(max_model_attempts or DEFAULT_MAX_MODEL_ATTEMPTS))

    def summarize_document(
        self,
        *,
        document_name: str,
        blocks: list[dict[str, Any]],
    ) -> dict[str, Any]:
        chunk_payloads = self._build_document_chunks(document_name=document_name, blocks=blocks)
        partials = [
            self._generate_document_summary_chunk(
                document_name=document_name,
                chunk_text=chunk_payload["text"],
                chunk_index=index + 1,
                chunk_count=len(chunk_payloads),
            )
            for index, chunk_payload in enumerate(chunk_payloads)
        ]
        merged = self._merge_document_summary_partials(document_name=document_name, partials=partials)
        merged["prompt_version"] = DOCUMENT_SUMMARY_PROMPT_VERSION
        return merged

    def summarize_conversation(
        self,
        *,
        conversation_id: str,
        documents: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if not documents:
            raise SummaryServiceError(
                code="conversation_summary_no_documents",
                message="No summarized documents are available for this conversation.",
                status_code=400,
                retryable=False,
            )

        batches = self._build_json_batches(
            items=[
                {
                    "document_id": str(item.get("document_id") or ""),
                    "document_name": str(item.get("document_name") or ""),
                    "title_hint": str(item.get("title_hint") or ""),
                    "summary_text": str(item.get("summary_text") or ""),
                    "key_points": item.get("key_points") or [],
                    "topics": item.get("topics") or [],
                }
                for item in documents
            ],
            target_tokens=self.merge_batch_target_tokens,
        )

        partials = [
            self._generate_conversation_summary_batch(
                batch_items=batch,
                batch_index=index + 1,
                batch_count=len(batches),
            )
            for index, batch in enumerate(batches)
        ]

        merged = self._merge_conversation_summary_partials(
            conversation_id=conversation_id,
            partials=partials,
        )
        title_result = self.generate_conversation_title(
            conversation_summary=merged["summary_text"],
            key_points=merged["key_points"],
            topics=merged["topics"],
        )
        merged["generated_title"] = title_result["title"]
        merged["title_prompt_version"] = CONVERSATION_TITLE_PROMPT_VERSION
        merged["title_provider_name"] = title_result.get("provider_name")
        merged["title_model_name"] = title_result.get("model_name")
        merged["title_token_count"] = title_result.get("token_count")
        merged["prompt_version"] = CONVERSATION_SUMMARY_PROMPT_VERSION
        return merged

    def generate_conversation_title(
        self,
        *,
        conversation_summary: str,
        key_points: list[str],
        topics: list[str],
    ) -> dict[str, Any]:
        prompt = "\n".join(
            [
                "Create a short, specific conversation title based only on this summary.",
                "Rules:",
                "- Maximum 8 words.",
                "- Avoid vague titles like 'Summary' or 'Discussion'.",
                "- Prefer the main subject, method, or outcome.",
                "",
                "Conversation summary:",
                _normalize_text(conversation_summary),
                "",
                "Key points:",
                json.dumps(_normalize_string_list(key_points, max_items=8, max_chars=180), ensure_ascii=True),
                "",
                "Topics:",
                json.dumps(_normalize_string_list(topics, max_items=8, max_chars=80), ensure_ascii=True),
            ]
        )
        response = self._generate_structured_json(
            prompt=prompt,
            schema=CONVERSATION_TITLE_RESPONSE_SCHEMA,
            max_output_tokens=DEFAULT_TITLE_MAX_OUTPUT_TOKENS,
        )
        return {
            "title": _normalize_title((response.get("payload") or {}).get("title")) or "Conversation Summary",
            "provider_name": response.get("provider_name"),
            "model_name": response.get("model_name"),
            "token_count": response.get("token_count"),
        }

    def _build_document_chunks(self, *, document_name: str, blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        chunks: list[dict[str, Any]] = []
        current_lines: list[str] = []
        current_tokens = 0

        for block in blocks:
            source_unit_index = block.get("source_unit_index")
            reading_order = block.get("reading_order")
            text = _normalize_text(block.get("text"))
            if not text:
                continue
            line = f"[source:{source_unit_index or '?'} order:{reading_order or '?'}] {text}"
            line_tokens = _estimated_token_count(line)
            if current_lines and current_tokens + line_tokens > self.chunk_target_tokens:
                chunks.append({"text": "\n\n".join(current_lines), "document_name": document_name})
                current_lines = []
                current_tokens = 0
            current_lines.append(line)
            current_tokens += line_tokens

        if current_lines:
            chunks.append({"text": "\n\n".join(current_lines), "document_name": document_name})

        return chunks or [{"text": f"[source:? order:?] {_normalize_text(document_name)}", "document_name": document_name}]

    def _build_json_batches(self, *, items: list[dict[str, Any]], target_tokens: int) -> list[list[dict[str, Any]]]:
        batches: list[list[dict[str, Any]]] = []
        current: list[dict[str, Any]] = []
        current_tokens = 0
        for item in items:
            item_text = json.dumps(item, ensure_ascii=True, separators=(",", ":"))
            item_tokens = _estimated_token_count(item_text)
            if current and current_tokens + item_tokens > target_tokens:
                batches.append(current)
                current = []
                current_tokens = 0
            current.append(item)
            current_tokens += item_tokens
        if current:
            batches.append(current)
        return batches or [[]]

    def _generate_document_summary_chunk(
        self,
        *,
        document_name: str,
        chunk_text: str,
        chunk_index: int,
        chunk_count: int,
    ) -> dict[str, Any]:
        prompt = "\n".join(
            [
                "Summarize this document content for later multi-document conversation synthesis.",
                "Return factual, compact JSON only.",
                "Rules:",
                "- Focus on the main claims, methods, findings, or data in the text.",
                "- Do not mention chunk numbers in the output.",
                "- Keep `summary_text` under 220 words.",
                "- Keep `key_points` to at most 8 items.",
                "- Keep `topics` to at most 8 short phrases.",
                "- Keep `title_hint` concise.",
                "",
                f"Document name: {document_name}",
                f"Chunk: {chunk_index} of {chunk_count}",
                "",
                "Document text:",
                chunk_text,
            ]
        )
        response = self._generate_structured_json(
            prompt=prompt,
            schema=DOCUMENT_SUMMARY_RESPONSE_SCHEMA,
            max_output_tokens=DEFAULT_DOCUMENT_MAX_OUTPUT_TOKENS,
        )
        payload = self._normalize_document_summary_payload(response.get("payload") or {})
        payload.update(
            {
                "provider_name": response.get("provider_name"),
                "model_name": response.get("model_name"),
                "token_count": response.get("token_count"),
            }
        )
        return payload

    def _merge_document_summary_partials(
        self,
        *,
        document_name: str,
        partials: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if len(partials) == 1:
            return partials[0]

        batches = self._build_json_batches(items=partials, target_tokens=self.merge_batch_target_tokens)
        merged_partials = []
        for batch in batches:
            prompt = "\n".join(
                [
                    "Merge these partial document summaries into one clean document summary.",
                    "Return JSON only and remove duplication.",
                    f"Document name: {document_name}",
                    "",
                    "Partial summaries:",
                    json.dumps(batch, ensure_ascii=True, indent=2),
                ]
            )
            response = self._generate_structured_json(
                prompt=prompt,
                schema=DOCUMENT_SUMMARY_RESPONSE_SCHEMA,
                max_output_tokens=DEFAULT_DOCUMENT_MAX_OUTPUT_TOKENS,
            )
            payload = self._normalize_document_summary_payload(response.get("payload") or {})
            payload.update(
                {
                    "provider_name": response.get("provider_name"),
                    "model_name": response.get("model_name"),
                    "token_count": response.get("token_count"),
                }
            )
            merged_partials.append(payload)

        return self._merge_document_summary_partials(document_name=document_name, partials=merged_partials)

    def _generate_conversation_summary_batch(
        self,
        *,
        batch_items: list[dict[str, Any]],
        batch_index: int,
        batch_count: int,
    ) -> dict[str, Any]:
        prompt = "\n".join(
            [
                "Combine these document summaries into a single conversation-level summary.",
                "Return JSON only.",
                "Rules:",
                "- Capture the shared themes and the distinct contributions of the documents.",
                "- Keep `summary_text` under 260 words.",
                "- Keep `key_points` to at most 10 items.",
                "- Keep `topics` to at most 10 short phrases.",
                "",
                f"Batch: {batch_index} of {batch_count}",
                "",
                "Document summaries:",
                json.dumps(batch_items, ensure_ascii=True, indent=2),
            ]
        )
        response = self._generate_structured_json(
            prompt=prompt,
            schema=CONVERSATION_SUMMARY_RESPONSE_SCHEMA,
            max_output_tokens=DEFAULT_CONVERSATION_MAX_OUTPUT_TOKENS,
        )
        payload = self._normalize_conversation_summary_payload(response.get("payload") or {})
        payload.update(
            {
                "provider_name": response.get("provider_name"),
                "model_name": response.get("model_name"),
                "token_count": response.get("token_count"),
            }
        )
        return payload

    def _merge_conversation_summary_partials(
        self,
        *,
        conversation_id: str,
        partials: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if len(partials) == 1:
            return partials[0]

        batches = self._build_json_batches(items=partials, target_tokens=self.merge_batch_target_tokens)
        merged_partials = []
        for batch in batches:
            prompt = "\n".join(
                [
                    "Merge these partial conversation summaries into one final conversation summary.",
                    "Return JSON only and remove duplication.",
                    f"Conversation ID: {conversation_id}",
                    "",
                    "Partial conversation summaries:",
                    json.dumps(batch, ensure_ascii=True, indent=2),
                ]
            )
            response = self._generate_structured_json(
                prompt=prompt,
                schema=CONVERSATION_SUMMARY_RESPONSE_SCHEMA,
                max_output_tokens=DEFAULT_CONVERSATION_MAX_OUTPUT_TOKENS,
            )
            payload = self._normalize_conversation_summary_payload(response.get("payload") or {})
            payload.update(
                {
                    "provider_name": response.get("provider_name"),
                    "model_name": response.get("model_name"),
                    "token_count": response.get("token_count"),
                }
            )
            merged_partials.append(payload)

        return self._merge_conversation_summary_partials(conversation_id=conversation_id, partials=merged_partials)

    def _normalize_document_summary_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {
            "summary_text": _normalize_text(payload.get("summary_text"))[:2400],
            "key_points": _normalize_string_list(payload.get("key_points"), max_items=8, max_chars=240),
            "topics": _normalize_string_list(payload.get("topics"), max_items=8, max_chars=80),
            "title_hint": _normalize_title(payload.get("title_hint"), max_chars=120),
        }

    def _normalize_conversation_summary_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {
            "summary_text": _normalize_text(payload.get("summary_text"))[:3200],
            "key_points": _normalize_string_list(payload.get("key_points"), max_items=10, max_chars=240),
            "topics": _normalize_string_list(payload.get("topics"), max_items=10, max_chars=80),
        }

    def _generate_structured_json(
        self,
        *,
        prompt: str,
        schema: dict[str, Any],
        max_output_tokens: int,
    ) -> dict[str, Any]:
        if not self.gemini_credentials:
            raise SummaryServiceError(
                code="missing_gemini_api_key",
                message="GEMINI_API_KEY or GEMINI_API_KEYS is required for summary generation.",
                status_code=503,
                retryable=False,
            )

        last_error: Exception | None = None
        base_project_id = get_quota_project_id()
        routed = None
        for credential in self.gemini_credentials:
            scoped_project_id = build_quota_project_id_for_credential(base_project_id, credential.alias)
            try:
                routed = execute_with_shared_quota_router(
                    TASK_TYPE_TEXT,
                    provider_order=["gemini"],
                    fallback_model=self.default_model,
                    project_id=scoped_project_id,
                    max_model_attempts=self.max_model_attempts,
                    execute_provider=lambda model_name, provider_name, api_key=credential.api_key: self._execute_provider(
                        model_name=model_name,
                        provider_name=provider_name,
                        prompt=prompt,
                        schema=schema,
                        max_output_tokens=max_output_tokens,
                        api_key=api_key,
                    ),
                    should_retry_with_another_model=self._should_retry_with_another_model,
                )
                break
            except Exception as exc:
                last_error = exc
                continue

        if routed is None:
            if isinstance(last_error, QuotaRouterError):
                raise SummaryServiceError(
                    code="summary_model_unavailable",
                    message=str(last_error),
                    status_code=503,
                    retryable=True,
                    retry_after_seconds=300,
                ) from last_error
            if isinstance(last_error, SummaryServiceError):
                raise last_error
            if last_error is not None:
                raise SummaryServiceError(
                    code="summary_provider_failed",
                    message=str(last_error),
                    status_code=503,
                    retryable=True,
                ) from last_error
            raise SummaryServiceError(
                code="summary_model_unavailable",
                message="No compatible Gemini API key is currently available for summary generation.",
                status_code=503,
                retryable=True,
                retry_after_seconds=300,
            )
        return {
            "payload": routed.payload or {},
            "provider_name": routed.provider_name,
            "model_name": routed.provider_model_name,
            "token_count": routed.token_count,
        }

    def _execute_provider(
        self,
        *,
        model_name: str,
        provider_name: str,
        prompt: str,
        schema: dict[str, Any],
        max_output_tokens: int,
        api_key: str,
    ) -> dict[str, Any]:
        if provider_name != "gemini":
            raise SummaryServiceError(
                code="unsupported_provider",
                message=f"Unsupported summary provider: {provider_name}",
                status_code=400,
                retryable=False,
            )

        payload = {
            "contents": [
                {
                    "role": "user",
                    "parts": [{"text": prompt}],
                }
            ],
            "generationConfig": {
                "responseMimeType": "application/json",
                "responseJsonSchema": schema,
                "temperature": 0.2,
                "maxOutputTokens": max_output_tokens,
            },
        }
        url = f"{GEMINI_API_BASE}/models/{model_name}:generateContent?key={api_key}"
        req = request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as response:
                response_headers = dict(getattr(response, "headers", {}) or {})
                raw = json.loads(response.read().decode("utf-8"))
        except error.HTTPError as http_error:
            details = http_error.read().decode("utf-8", errors="ignore")
            raise SummaryServiceError(
                code="gemini_request_failed",
                message=f"Gemini summary request failed: {http_error.code}",
                status_code=http_error.code,
                retryable=http_error.code in {408, 409, 429, 500, 502, 503, 504},
                details={"response_body": details[:2000]},
            ) from http_error
        except Exception as exc:
            raise SummaryServiceError(
                code="gemini_request_failed",
                message=f"Gemini summary request failed: {exc}",
                status_code=503,
                retryable=True,
            ) from exc

        response_text = (
            raw.get("candidates", [{}])[0]
            .get("content", {})
            .get("parts", [{}])[0]
            .get("text", "")
        )
        if not response_text:
            raise SummaryServiceError(
                code="empty_summary_response",
                message="Gemini returned an empty summary response.",
                status_code=502,
                retryable=True,
                details={"raw_response": raw},
            )

        try:
            parsed_payload = _parse_json_response_text(response_text)
        except Exception as exc:
            raise SummaryServiceError(
                code="invalid_summary_json",
                message=str(exc),
                status_code=502,
                retryable=True,
                details={"response_text_preview": response_text[:1000]},
            ) from exc

        return {
            "payload": parsed_payload,
            "response_headers": response_headers,
            "provider_model_name": model_name,
            "token_count": resolve_usage_token_count(raw),
        }

    @staticmethod
    def _should_retry_with_another_model(exc: Exception) -> bool:
        if isinstance(exc, SummaryServiceError):
            if exc.retryable:
                return True
            if exc.status_code == 404:
                return True
        return False
