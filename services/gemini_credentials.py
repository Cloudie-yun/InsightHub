from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass


@dataclass(frozen=True)
class GeminiApiCredential:
    alias: str
    api_key: str


def load_gemini_api_credentials() -> list[GeminiApiCredential]:
    raw_pool = str(os.environ.get("GEMINI_API_KEYS") or "").strip()
    raw_single = str(os.environ.get("GEMINI_API_KEY") or "").strip()
    raw_values = []
    if raw_pool:
        raw_values.extend(re.split(r"[\r\n,]+", raw_pool))
    if raw_single:
        raw_values.append(raw_single)

    credentials: list[GeminiApiCredential] = []
    seen_keys: set[str] = set()
    for index, raw_value in enumerate(raw_values, start=1):
        api_key = str(raw_value or "").strip()
        if not api_key or api_key in seen_keys:
            continue
        seen_keys.add(api_key)
        digest = hashlib.sha256(api_key.encode("utf-8")).hexdigest()[:8]
        credentials.append(
            GeminiApiCredential(
                alias=f"gemini_key_{index}_{digest}",
                api_key=api_key,
            )
        )
    return credentials


def build_quota_project_id_for_credential(base_project_id: str, credential_alias: str | None = None) -> str:
    normalized_base = str(base_project_id or "").strip() or "default"
    normalized_alias = str(credential_alias or "").strip()
    if not normalized_alias:
        return normalized_base
    return f"{normalized_base}::cred::{normalized_alias}"
