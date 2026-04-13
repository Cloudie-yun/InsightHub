from __future__ import annotations

import time
from typing import Callable

import httpx

from .client import MinerUError, api_headers, build_client, request_with_retry
from .constants import MINERU_API_ROOT, POLL_INTERVAL_SECONDS, POLL_TIMEOUT_SECONDS, MAX_RETRIES


def request_upload_url(
    token: str,
    filename: str,
    *,
    model_version: str = "vlm",
    language: str = "en",
    enable_formula: bool = True,
    enable_table: bool = True,
    is_ocr: bool = False,
    data_id: str | None = None,
) -> tuple[str, str]:
    payload = {
        "files": [{
            "name": filename,
            "data_id": data_id or filename,
        }],
        "model_version": model_version,
        "language": language,
        "enable_formula": enable_formula,
        "enable_table": enable_table,
        "is_ocr": is_ocr,
    }

    resp = request_with_retry(
        "POST",
        f"{MINERU_API_ROOT}/file-urls/batch",
        headers=api_headers(token),
        json=payload,
        _timeout_seconds=60,
    )
    body = resp.json()
    if body.get("code") != 0:
        raise MinerUError(f"Failed to get upload URL: {body.get('msg', 'unknown error')}")

    data = body.get("data") or {}
    batch_id = data.get("batch_id")
    file_urls = data.get("file_urls") or []

    if not batch_id:
        raise MinerUError("MinerU response missing batch_id")
    if not file_urls or not file_urls[0]:
        raise MinerUError("MinerU response missing presigned upload URL")
    return batch_id, file_urls[0]


def upload_file(put_url: str, file_bytes: bytes, content_type: str | None = None) -> None:
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with build_client(300, follow_redirects=False) as client:
                headers = {}
                if content_type:
                    headers["Content-Type"] = content_type
                response = client.put(put_url, content=file_bytes, headers=headers)
                response.raise_for_status()
                return
        except (httpx.ConnectError, httpx.ReadError, httpx.WriteError, httpx.RemoteProtocolError, httpx.TimeoutException) as exc:
            last_exc = exc
            if attempt == MAX_RETRIES:
                break
            time.sleep(min(2 * attempt, 5))
        except httpx.HTTPStatusError as exc:
            body_preview = (exc.response.text or "")[:500]
            raise MinerUError(f"Upload failed with HTTP status {exc.response.status_code}: {body_preview}")

    raise MinerUError(f"Upload failed after {MAX_RETRIES} attempts: {last_exc}")


def _poll_until_done(
    fetch_status: Callable[[], dict],
    get_state: Callable[[dict], str],
    get_done_url: Callable[[dict], str | None],
    get_failed_message: Callable[[dict], str],
    on_progress: Callable[[dict, int], None] | None = None,
    *,
    timeout_seconds: int = POLL_TIMEOUT_SECONDS,
    interval_seconds: int = POLL_INTERVAL_SECONDS,
) -> str:
    deadline = time.monotonic() + timeout_seconds
    attempt = 0

    while time.monotonic() < deadline:
        attempt += 1
        payload = fetch_status()
        state = get_state(payload).lower()

        if on_progress:
            on_progress(payload, attempt)

        if state == "done":
            done_url = get_done_url(payload)
            if not done_url:
                raise MinerUError("Done state received, but no zip URL was returned")
            return done_url

        if state == "failed":
            raise MinerUError(get_failed_message(payload))

        time.sleep(interval_seconds)

    raise MinerUError(f"Timed out after {timeout_seconds}s waiting for MinerU result")


def poll_batch(
    token: str,
    batch_id: str,
    on_progress: Callable[[dict, int], None] | None = None,
) -> str:
    def _fetch_status() -> dict:
        response = request_with_retry(
            "GET",
            f"{MINERU_API_ROOT}/extract-results/batch/{batch_id}",
            headers=api_headers(token),
            _timeout_seconds=60,
        )
        body = response.json()
        if body.get("code") != 0:
            raise MinerUError(f"Poll error: {body.get('msg', 'unknown error')}")
        data = body.get("data") or {}
        extract_result = data.get("extract_result")
        if isinstance(extract_result, list):
            result = extract_result[0] if extract_result else {}
        elif isinstance(extract_result, dict):
            result = extract_result
        else:
            result = {}
        return {
            "data": data,
            "result": result,
        }

    def _get_state(payload: dict) -> str:
        result = payload.get("result") or {}
        if not result:
            return "waiting"
        return str(result.get("state") or "waiting")

    def _get_done_url(payload: dict) -> str | None:
        data = payload.get("data") or {}
        result = payload.get("result") or {}
        return (
            result.get("full_zip_url")
            or result.get("zip_url")
            or data.get("full_zip_url")
            or data.get("zip_url")
        )

    def _get_failed_message(payload: dict) -> str:
        result = payload.get("result") or {}
        err = result.get("err_msg") or result.get("msg") or "unknown error"
        return f"Extraction failed: {err}"

    return _poll_until_done(
        _fetch_status,
        _get_state,
        _get_done_url,
        _get_failed_message,
        on_progress=on_progress,
    )
