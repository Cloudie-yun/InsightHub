from __future__ import annotations

import io
import json
import os
import re
import time
import zipfile
from pathlib import Path
from typing import Callable

import httpx

from services.parsers.utils import (
    clean_extracted_text,
    encode_image_bytes,
    table_to_markdown,
)

# Official API root, not a task endpoint.
_MINERU_API_ROOT = "https://mineru.net/api/v4"
_POLL_INTERVAL = 5
_POLL_TIMEOUT = 900
_MAX_RETRIES = 3

_IMAGE_MIME_BY_EXT = {
    "png": "image/png",
    "jpg": "image/jpeg",
    "jpeg": "image/jpeg",
    "webp": "image/webp",
    "gif": "image/gif",
    "bmp": "image/bmp",
}

ProgressCallback = Callable[[dict], None]


# ===========================================================================
# 1. ERRORS / HELPERS
# ===========================================================================

class MinerUError(Exception):
    pass


def _api_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "*/*",
    }


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default

def _build_client(timeout_seconds: int) -> httpx.Client:
    return httpx.Client(
        timeout=httpx.Timeout(timeout_seconds),
        follow_redirects=True,
        http2=False,
        trust_env=False,
    )


def _emit_progress(progress_callback: ProgressCallback | None, **payload) -> None:
    if not progress_callback:
        return
    try:
        progress_callback(payload)
    except Exception:
        # Progress updates are best-effort and should not break parsing.
        return

def _request_with_retry(method: str, url: str, **kwargs) -> httpx.Response:
    last_exc = None
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            timeout_seconds = kwargs.pop("_timeout_seconds", 120)
            with _build_client(timeout_seconds) as client:
                response = client.request(method, url, **kwargs)
                response.raise_for_status()
                return response
        except (httpx.ConnectError, httpx.ReadError, httpx.WriteError, httpx.RemoteProtocolError, httpx.TimeoutException) as exc:
            last_exc = exc
            if attempt == _MAX_RETRIES:
                break
            time.sleep(min(2 * attempt, 5))
        except httpx.HTTPStatusError:
            raise

    raise MinerUError(f"Network request failed after {_MAX_RETRIES} attempts: {last_exc}")

# ===========================================================================
# 2. BATCH UPLOAD FLOW
# Official docs:
#   POST /api/v4/file-urls/batch
#   GET  /api/v4/extract-results/batch/{batch_id}
# ===========================================================================

def _request_upload_url(
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
    """
    Ask MinerU for a presigned upload URL.

    Returns:
        (batch_id, presigned_put_url)
    """
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

    resp = _request_with_retry(
        "POST",
        f"{_MINERU_API_ROOT}/file-urls/batch",
        headers=_api_headers(token),
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


def _upload_file(put_url: str, file_bytes: bytes, content_type: str | None = None) -> None:
    """
    Upload raw bytes to MinerU's presigned storage URL.
    No Bearer token should be sent here.
    """
    last_exc = None

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            # Presigned object-storage URLs are signature-sensitive.
            # Avoid redirects and avoid extra headers unless explicitly required.
            with httpx.Client(
                timeout=httpx.Timeout(300),
                follow_redirects=False,
                http2=False,
                trust_env=False,
            ) as client:
                headers = {}
                if content_type:
                    headers["Content-Type"] = content_type
                resp = client.put(
                    put_url,
                    content=file_bytes,
                    headers=headers,
                )
                resp.raise_for_status()
                return
        except (httpx.ConnectError, httpx.ReadError, httpx.WriteError, httpx.RemoteProtocolError, httpx.TimeoutException) as exc:
            last_exc = exc
            if attempt == _MAX_RETRIES:
                break
            time.sleep(min(2 * attempt, 5))
        except httpx.HTTPStatusError as exc:
            # Signature errors are deterministic. Retrying with the same request
            # does not help and can consume quota.
            body_preview = (exc.response.text or "")[:500]
            raise MinerUError(
                f"Upload failed with HTTP status {exc.response.status_code}: {body_preview}"
            )

    raise MinerUError(f"Upload failed after {_MAX_RETRIES} attempts: {last_exc}")



def _poll_batch(
    token: str,
    batch_id: str,
    progress_callback: ProgressCallback | None = None,
) -> str:
    deadline = time.monotonic() + _POLL_TIMEOUT
    last_state = None
    last_extracted_pages = None
    last_total_pages = None

    while time.monotonic() < deadline:
        try:
            resp = _request_with_retry(
                "GET",
                f"{_MINERU_API_ROOT}/extract-results/batch/{batch_id}",
                headers=_api_headers(token),
                _timeout_seconds=60,
            )
            body = resp.json()
        except Exception:
            time.sleep(_POLL_INTERVAL)
            continue

        if body.get("code") != 0:
            raise MinerUError(f"Poll error: {body.get('msg', 'unknown error')}")

        data = body.get("data") or {}
        extract_result = data.get("extract_result")

        if not extract_result:
            _emit_progress(
                progress_callback,
                stage="extracting",
                message="Waiting for MinerU extraction result.",
                provider="mineru",
                provider_state="waiting",
                batch_id=batch_id,
                force=False,
            )
            time.sleep(_POLL_INTERVAL)
            continue

        if isinstance(extract_result, list):
            result = extract_result[0] if extract_result else {}
        elif isinstance(extract_result, dict):
            result = extract_result
        else:
            result = {}

        state = str(result.get("state", "")).lower()
        err_msg = result.get("err_msg") or result.get("msg") or ""

        progress = result.get("extract_progress") or {}
        extracted_pages = progress.get("extracted_pages")
        total_pages = progress.get("total_pages")
        start_time = progress.get("start_time")

        state_changed = state != last_state
        pages_changed = (
            extracted_pages != last_extracted_pages
            or total_pages != last_total_pages
        )

        if state_changed or pages_changed:
            if extracted_pages is not None and total_pages is not None:
                message = (
                    f"MinerU extracting, {extracted_pages}/{total_pages} pages processed."
                )
            else:
                message = f"MinerU extraction state: {state or 'unknown'}"

            _emit_progress(
                progress_callback,
                stage="extracting",
                message=message,
                provider="mineru",
                provider_state=state or "unknown",
                batch_id=batch_id,
                extracted_pages=extracted_pages,
                total_pages=total_pages,
                start_time=start_time,
                force=True,
            )

            last_state = state
            last_extracted_pages = extracted_pages
            last_total_pages = total_pages

        if state == "done":
            zip_url = (
                result.get("full_zip_url")
                or result.get("zip_url")
                or data.get("full_zip_url")
                or data.get("zip_url")
            )
            if not zip_url:
                raise MinerUError("Done state received, but no zip URL was returned")
            return zip_url

        if state == "failed":
            raise MinerUError(
                f"Extraction failed: {err_msg or 'unknown error'}"
            )

        time.sleep(_POLL_INTERVAL)

    raise MinerUError(f"Timed out after {_POLL_TIMEOUT}s waiting for batch {batch_id}")


def _download_zip(zip_url: str) -> zipfile.ZipFile:
    resp = _request_with_retry(
        "GET",
        zip_url,
        _timeout_seconds=300,
    )
    return zipfile.ZipFile(io.BytesIO(resp.content))

# ===========================================================================
# 3. OPTIONAL DIRECT URL FLOW
# Official docs:
#   POST /api/v4/extract/task
#   GET  /api/v4/extract/task/{task_id}
# ===========================================================================

def _create_direct_task(
    token: str,
    file_url: str,
    *,
    model_version: str = "vlm",
    language: str = "en",
    enable_formula: bool = True,
    enable_table: bool = True,
    is_ocr: bool = False,
    data_id: str | None = None,
) -> str:
    payload = {
        "url": file_url,
        "model_version": model_version,
        "language": language,
        "enable_formula": enable_formula,
        "enable_table": enable_table,
        "is_ocr": is_ocr,
    }
    if data_id:
        payload["data_id"] = data_id

    with httpx.Client(timeout=30, follow_redirects=True) as client:
        resp = client.post(
            f"{_MINERU_API_ROOT}/extract/task",
            headers=_api_headers(token),
            json=payload,
        )
        resp.raise_for_status()
        body = resp.json()

    if body.get("code") != 0:
        raise MinerUError(f"Failed to create task: {body.get('msg', 'unknown error')}")

    data = body.get("data") or {}
    task_id = data.get("task_id")
    if not task_id:
        raise MinerUError("MinerU response missing task_id")
    return task_id


def _poll_direct_task(token: str, task_id: str) -> str:
    deadline = time.monotonic() + _POLL_TIMEOUT

    with httpx.Client(timeout=30, follow_redirects=True) as client:
        while time.monotonic() < deadline:
            resp = client.get(
                f"{_MINERU_API_ROOT}/extract/task/{task_id}",
                headers=_api_headers(token),
            )
            resp.raise_for_status()
            body = resp.json()

            if body.get("code") != 0:
                raise MinerUError(f"Poll error: {body.get('msg', 'unknown error')}")

            data = body.get("data") or {}
            state = str(data.get("state", "")).lower()

            if state == "done":
                zip_url = data.get("full_zip_url") or data.get("zip_url")
                if not zip_url:
                    raise MinerUError("Done state received, but no zip URL was returned")
                return zip_url

            if state == "failed":
                raise MinerUError(f"Extraction failed for task {task_id}")

            time.sleep(_POLL_INTERVAL)

    raise MinerUError(f"Timed out after {_POLL_TIMEOUT}s waiting for task {task_id}")


# ===========================================================================
# 4. ZIP PARSING
# ===========================================================================

def _parse_zip(zf: zipfile.ZipFile, source_path: str) -> list[dict]:
    """
    Parse MinerU result zip into your internal segment schema.

    Preference order:
      1. content_list.json
      2. any .md file
    """
    names = zf.namelist()

    content_list_name = next((n for n in names if n.endswith("content_list.json")), None)
    if content_list_name:
        return _parse_content_list(zf, content_list_name, names)

    md_name = next((n for n in names if n.lower().endswith(".md")), None)
    if md_name:
        md_text = zf.read(md_name).decode("utf-8", errors="replace")
        return _parse_markdown_fallback(md_text, source_path)

    return []


def _parse_content_list(zf: zipfile.ZipFile, content_list_name: str, all_names: list[str]) -> list[dict]:
    raw = zf.read(content_list_name).decode("utf-8", errors="replace")
    items = json.loads(raw)

    if not isinstance(items, list):
        return []

    image_lookup: dict[str, str] = {}
    for name in all_names:
        lower = name.lower()
        ext = lower.rsplit(".", 1)[-1] if "." in lower else ""
        if ext not in _IMAGE_MIME_BY_EXT:
            continue
        try:
            img_bytes = zf.read(name)
            mime = _IMAGE_MIME_BY_EXT[ext]
            data_uri = encode_image_bytes(img_bytes, mime)
            image_lookup[name] = data_uri
            image_lookup[Path(name).name] = data_uri
        except Exception:
            continue

    segments: list[dict] = []
    seg_counter: dict[int, int] = {}

    for item in items:
        if not isinstance(item, dict):
            continue

        page_index = _safe_int(item.get("page_idx", 0), 0) + 1
        seg_counter[page_index] = seg_counter.get(page_index, 0) + 1
        seg_num = seg_counter[page_index]
        seg_id = f"mineru-page-{page_index}-{seg_num}"

        item_type = str(item.get("type") or "text").lower()
        bbox = item.get("bbox") or []

        if item_type in {"text", "title", "interleaved_title"}:
            text = clean_extracted_text(item.get("text") or "")
            if not text:
                continue
            role = "heading" if item_type in {"title", "interleaved_title"} else "paragraph"
            segments.append({
                "segment_id": seg_id,
                "source_type": "paragraph",
                "source_index": page_index,
                "block_index": seg_num,
                "paragraph_index": seg_num,
                "text": text,
                "metadata": {
                    "parser_adapter": "mineru",
                    "bbox": bbox,
                    "char_count": len(text),
                    "role": role,
                },
            })
            continue

        if item_type == "table":
            html = item.get("html") or ""
            md = _html_table_to_markdown(html) if html else ""
            if not md:
                md = clean_extracted_text(item.get("text") or "")
            if not md:
                continue

            segments.append({
                "segment_id": seg_id,
                "source_type": "table",
                "source_index": page_index,
                "block_index": None,
                "paragraph_index": None,
                "text": md,
                "metadata": {
                    "parser_adapter": "mineru",
                    "bbox": bbox,
                    "char_count": len(md),
                },
            })
            continue

        if item_type in {"image", "figure"}:
            img_path = item.get("img_path") or ""
            data_uri = image_lookup.get(img_path) or image_lookup.get(Path(img_path).name)
            caption = clean_extracted_text(item.get("text") or "")

            segments.append({
                "segment_id": seg_id,
                "source_type": "image",
                "source_index": page_index,
                "block_index": None,
                "paragraph_index": None,
                "text": caption,
                "metadata": {
                    "parser_adapter": "mineru",
                    "bbox": bbox,
                    "char_count": len(caption),
                    "data_uri": data_uri,
                },
            })
            continue

        text = clean_extracted_text(item.get("text") or "")
        if text:
            segments.append({
                "segment_id": seg_id,
                "source_type": "paragraph",
                "source_index": page_index,
                "block_index": seg_num,
                "paragraph_index": seg_num,
                "text": text,
                "metadata": {
                    "parser_adapter": "mineru",
                    "bbox": bbox,
                    "char_count": len(text),
                    "role": "paragraph",
                    "mineru_type": item_type,
                },
            })

    return segments


def _html_table_to_markdown(html: str) -> str:
    html = (html or "").strip()
    if not html:
        return ""

    try:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")
        rows = []
        for tr in soup.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            row = [clean_extracted_text(cell.get_text(" ", strip=True)) for cell in cells]
            if row:
                rows.append(row)

        if rows:
            return table_to_markdown(rows, has_header=True)
    except Exception:
        pass

    rows = []
    tr_matches = re.findall(r"<tr\b.*?>(.*?)</tr>", html, flags=re.I | re.S)
    for tr_html in tr_matches:
        cell_matches = re.findall(r"<t[hd]\b.*?>(.*?)</t[hd]>", tr_html, flags=re.I | re.S)
        row = []
        for cell_html in cell_matches:
            cell_text = re.sub(r"<[^>]+>", " ", cell_html)
            cell_text = clean_extracted_text(cell_text)
            row.append(cell_text)
        if row:
            rows.append(row)

    return table_to_markdown(rows, has_header=True) if rows else ""


def _parse_markdown_fallback(md_text: str, source_path: str) -> list[dict]:
    segments: list[dict] = []
    counter = 0

    for block in re.split(r"\n{2,}", (md_text or "").strip()):
        text = clean_extracted_text(block.strip())
        if not text:
            continue

        counter += 1
        role = "heading" if re.match(r"^#{1,6}\s", text) else "paragraph"
        if role == "heading":
            text = re.sub(r"^#{1,6}\s+", "", text)

        segments.append({
            "segment_id": f"mineru-fallback-{counter}",
            "source_type": "paragraph",
            "source_index": 1,
            "block_index": counter,
            "paragraph_index": counter,
            "text": text,
            "metadata": {
                "parser_adapter": "mineru_md_fallback",
                "char_count": len(text),
                "role": role,
                "source_path": source_path,
            },
        })

    return segments

# ===========================================================================
# 5. PUBLIC ENTRY POINTS
# ===========================================================================

def parse_pdf_via_mineru_upload(
    file_path: str | Path,
    token: str,
    progress_callback: ProgressCallback | None = None,
    *,
    model_version: str = "vlm",
    language: str = "en",
    enable_formula: bool = True,
    enable_table: bool = True,
    is_ocr: bool = False,
) -> dict:
    """
    Upload local file -> MinerU -> parse zip -> return your normal schema.
    """
    file_path = Path(file_path)
    metadata: dict = {
        "source_path": str(file_path),
        "parser": "mineru_api",
        "page_count": 0,
        "mode": "upload_batch",
        "model_version": model_version,
    }

    if not file_path.exists() or not file_path.is_file():
        return {
            "segments": [],
            "metadata": metadata,
            "errors": [{
                "code": "file_not_found",
                "message": f"File not found: {file_path}",
            }],
        }

    if file_path.suffix.lower() != ".pdf":
        return {
            "segments": [],
            "metadata": metadata,
            "errors": [{
                "code": "invalid_file_type",
                "message": "Only PDF is supported for MinerU parsing.",
            }],
        }

    if file_path.stat().st_size <= 0:
        return {
            "segments": [],
            "metadata": metadata,
            "errors": [{
                "code": "empty_file",
                "message": "Uploaded file is empty.",
            }],
        }

    try:
        _emit_progress(
            progress_callback,
            stage="reading_file",
            message="Reading local file for MinerU upload.",
            provider="mineru",
            provider_state="preparing",
            force=True,
        )
        file_bytes = file_path.read_bytes()

        _emit_progress(
            progress_callback,
            stage="requesting_upload_url",
            message="Requesting upload URL from MinerU.",
            provider="mineru",
            provider_state="requesting_upload_url",
            force=True,
        )
        batch_id, put_url = _request_upload_url(
            token,
            file_path.name,
            model_version=model_version,
            language=language,
            enable_formula=enable_formula,
            enable_table=enable_table,
            is_ocr=is_ocr,
            data_id=file_path.name,
        )
        metadata["batch_id"] = batch_id
        _emit_progress(
            progress_callback,
            stage="uploading",
            message="Uploading file to MinerU.",
            provider="mineru",
            provider_state="uploading",
            batch_id=batch_id,
            force=True,
        )

        _upload_file(put_url, file_bytes, content_type=None)

        _emit_progress(
            progress_callback,
            stage="extracting",
            message="Upload complete. MinerU is extracting the document.",
            provider="mineru",
            provider_state="pending",
            batch_id=batch_id,
            force=True,
        )
        zip_url = _poll_batch(token, batch_id, progress_callback=progress_callback)
        metadata["zip_url"] = zip_url

        _emit_progress(
            progress_callback,
            stage="parsing_result",
            message="Parsing MinerU extraction output.",
            provider="mineru",
            provider_state="parsing_result",
            batch_id=batch_id,
            force=True,
        )
        zf = _download_zip(zip_url)
        segments = _parse_zip(zf, str(file_path))

        if segments:
            page_indices = [s["source_index"] for s in segments if s.get("source_index")]
            metadata["page_count"] = max(page_indices) if page_indices else 0

        return {
            "segments": segments,
            "metadata": metadata,
            "errors": [],
        }

    except MinerUError as exc:
        _emit_progress(
            progress_callback,
            stage="failed",
            message=f"MinerU API error: {exc}",
            provider="mineru",
            provider_state="failed",
            batch_id=metadata.get("batch_id"),
            force=True,
        )
        return {
            "segments": [],
            "metadata": metadata,
            "errors": [{
                "code": "mineru_api_error",
                "message": str(exc),
            }],
        }
    except httpx.HTTPError as exc:
        _emit_progress(
            progress_callback,
            stage="failed",
            message=f"MinerU HTTP error: {exc}",
            provider="mineru",
            provider_state="failed",
            batch_id=metadata.get("batch_id"),
            force=True,
        )
        return {
            "segments": [],
            "metadata": metadata,
            "errors": [{
                "code": "http_error",
                "message": str(exc),
            }],
        }
    except Exception as exc:
        _emit_progress(
            progress_callback,
            stage="failed",
            message=f"Unexpected MinerU error: {exc}",
            provider="mineru",
            provider_state="failed",
            batch_id=metadata.get("batch_id"),
            force=True,
        )
        return {
            "segments": [],
            "metadata": metadata,
            "errors": [{
                "code": "parse_error",
                "message": str(exc),
            }],
        }


def parse_pdf_via_mineru_url(
    file_url: str,
    token: str,
    *,
    source_path: str | None = None,
    model_version: str = "vlm",
    language: str = "en",
    enable_formula: bool = True,
    enable_table: bool = True,
    is_ocr: bool = False,
) -> dict:
    """
    Direct URL mode, if your file is already publicly accessible.
    """
    metadata: dict = {
        "source_path": source_path or file_url,
        "parser": "mineru_api",
        "page_count": 0,
        "mode": "direct_task",
        "model_version": model_version,
    }

    try:
        task_id = _create_direct_task(
            token,
            file_url,
            model_version=model_version,
            language=language,
            enable_formula=enable_formula,
            enable_table=enable_table,
            is_ocr=is_ocr,
            data_id=source_path or file_url,
        )
        metadata["task_id"] = task_id

        zip_url = _poll_direct_task(token, task_id)
        metadata["zip_url"] = zip_url

        zf = _download_zip(zip_url)
        segments = _parse_zip(zf, source_path or file_url)

        if segments:
            page_indices = [s["source_index"] for s in segments if s.get("source_index")]
            metadata["page_count"] = max(page_indices) if page_indices else 0

        return {
            "segments": segments,
            "metadata": metadata,
            "errors": [],
        }

    except MinerUError as exc:
        return {
            "segments": [],
            "metadata": metadata,
            "errors": [{
                "code": "mineru_api_error",
                "message": str(exc),
            }],
        }
    except httpx.HTTPError as exc:
        return {
            "segments": [],
            "metadata": metadata,
            "errors": [{
                "code": "http_error",
                "message": str(exc),
            }],
        }
    except Exception as exc:
        return {
            "segments": [],
            "metadata": metadata,
            "errors": [{
                "code": "parse_error",
                "message": str(exc),
            }],
        }


def parse_pdf_with_mineru(
    file_path: str | Path,
    token: str | None = None,
    progress_callback: ProgressCallback | None = None,
) -> dict:
    """
    Main wrapper for local files.
    Reads token from MINERU_API_KEY if not passed explicitly.
    """
    resolved_token = (token or os.getenv("MINERU_API_KEY", "")).strip()
    if not resolved_token:
        _emit_progress(
            progress_callback,
            stage="failed",
            message="MINERU_API_KEY is not set.",
            provider="mineru",
            provider_state="failed",
            force=True,
        )
        return {
            "segments": [],
            "metadata": {
                "source_path": str(file_path),
                "parser": "mineru_api",
            },
            "errors": [{
                "code": "missing_mineru_api_key",
                "message": "MINERU_API_KEY is not set.",
            }], 
        }

    return parse_pdf_via_mineru_upload(
        file_path,
        resolved_token,
        progress_callback=progress_callback,
    )
