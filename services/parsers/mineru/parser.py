from __future__ import annotations

import os
from pathlib import Path

import httpx

from .api import poll_batch, request_upload_url, upload_file
from .client import MinerUError, ProgressCallback, ProgressEmitter
from .zip_parser import download_zip, parse_zip

MINERU_SUPPORTED_EXTENSIONS = frozenset({".pdf", ".ppt", ".pptx", ".doc", ".docx"})


def parse_document_via_mineru_upload(
    file_path: str | Path,
    token: str,
    progress_callback: ProgressCallback | None = None,
    document_id: str | None = None,
    original_filename: str | None = None,
    *,
    model_version: str = "vlm",
    language: str = "en",
    enable_formula: bool = True,
    enable_table: bool = True,
    is_ocr: bool = False,
) -> dict:
    file_path = Path(file_path)
    progress = ProgressEmitter(progress_callback, provider="mineru")

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
            "assets": [],
            "references": [],
            "metadata": metadata,
            "errors": [{"code": "file_not_found", "message": f"File not found: {file_path}"}],
        }
    file_extension = file_path.suffix.lower()
    if file_extension not in MINERU_SUPPORTED_EXTENSIONS:
        supported_extensions = ", ".join(sorted(MINERU_SUPPORTED_EXTENSIONS))
        return {
            "segments": [],
            "assets": [],
            "references": [],
            "metadata": metadata,
            "errors": [{
                "code": "invalid_file_type",
                "message": f"MinerU parsing supports: {supported_extensions}.",
            }],
        }
    if file_path.stat().st_size <= 0:
        return {
            "segments": [],
            "assets": [],
            "references": [],
            "metadata": metadata,
            "errors": [{"code": "empty_file", "message": "Uploaded file is empty."}],
        }

    try:
        progress.emit("reading_file", "Reading local file for MinerU upload.", provider_state="preparing", force=True)
        file_bytes = file_path.read_bytes()

        progress.emit(
            "requesting_upload_url",
            "Requesting upload URL from MinerU.",
            provider_state="requesting_upload_url",
            force=True,
        )
        batch_id, put_url = request_upload_url(
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

        progress.emit("uploading", "Uploading file to MinerU.", provider_state="uploading", batch_id=batch_id, force=True)
        upload_file(put_url, file_bytes, content_type=None)

        progress.emit(
            "extracting",
            "Upload complete. MinerU is extracting the document.",
            provider_state="pending",
            batch_id=batch_id,
            force=True,
        )

        def _on_batch_progress(payload: dict, attempt: int) -> None:
            result = payload.get("result") or {}
            state = str(result.get("state") or "waiting")
            progress_obj = result.get("extract_progress") or {}
            extracted_pages = progress_obj.get("extracted_pages")
            total_pages = progress_obj.get("total_pages")
            if extracted_pages is not None and total_pages is not None:
                message = f"MinerU extracting, {extracted_pages}/{total_pages} pages processed."
            else:
                message = f"MinerU extraction state: {state}"
            progress.emit(
                "extracting",
                message,
                provider_state=state,
                batch_id=batch_id,
                extracted_pages=extracted_pages,
                total_pages=total_pages,
                poll_attempt=attempt,
                force=True,
            )

        zip_url = poll_batch(token, batch_id, on_progress=_on_batch_progress)
        metadata["zip_url"] = zip_url

        progress.emit(
            "parsing_result",
            "Parsing MinerU extraction output.",
            provider_state="parsing_result",
            batch_id=batch_id,
            force=True,
        )
        zip_file = download_zip(zip_url)
        asset_output_dir = file_path.parent / ".extracted" / str(document_id or file_path.stem) / "assets"
        zip_result = parse_zip(
            zip_file,
            str(file_path),
            document_id=str(document_id) if document_id is not None else None,
            asset_output_dir=asset_output_dir,
        )
        segments = zip_result.get("segments", [])

        if segments:
            page_indices = [segment.get("source_index") for segment in segments if segment.get("source_index")]
            metadata["page_count"] = max(page_indices) if page_indices else 0
        metadata = {
            **(zip_result.get("metadata") or {}),
            **metadata,
        }
        if original_filename:
            metadata["original_filename"] = original_filename

        return {
            "segments": segments,
            "assets": zip_result.get("assets", []),
            "references": zip_result.get("references", []),
            "metadata": metadata,
            "errors": zip_result.get("errors", []),
        }
    except MinerUError as exc:
        progress.emit("failed", f"MinerU API error: {exc}", provider_state="failed", batch_id=metadata.get("batch_id"), force=True)
        return {
            "segments": [],
            "assets": [],
            "references": [],
            "metadata": metadata,
            "errors": [{"code": "mineru_api_error", "message": str(exc)}],
        }
    except httpx.HTTPError as exc:
        progress.emit("failed", f"MinerU HTTP error: {exc}", provider_state="failed", batch_id=metadata.get("batch_id"), force=True)
        return {
            "segments": [],
            "assets": [],
            "references": [],
            "metadata": metadata,
            "errors": [{"code": "http_error", "message": str(exc)}],
        }
    except Exception as exc:
        progress.emit("failed", f"Unexpected MinerU error: {exc}", provider_state="failed", batch_id=metadata.get("batch_id"), force=True)
        return {
            "segments": [],
            "assets": [],
            "references": [],
            "metadata": metadata,
            "errors": [{"code": "parse_error", "message": str(exc)}],
        }
def parse_document_with_mineru(
    file_path: str | Path,
    token: str | None = None,
    progress_callback: ProgressCallback | None = None,
    document_id: str | None = None,
    original_filename: str | None = None,
) -> dict:
    progress = ProgressEmitter(progress_callback, provider="mineru")
    resolved_token = (token or os.getenv("MINERU_API_KEY", "")).strip()
    if not resolved_token:
        progress.emit("failed", "MINERU_API_KEY is not set.", provider_state="failed", force=True)
        return {
            "segments": [],
            "assets": [],
            "references": [],
            "metadata": {
                "source_path": str(file_path),
                "parser": "mineru_api",
            },
            "errors": [{
                "code": "missing_mineru_api_key",
                "message": "MINERU_API_KEY is not set.",
            }],
        }

    return parse_document_via_mineru_upload(
        file_path,
        resolved_token,
        progress_callback=progress_callback,
        document_id=document_id,
        original_filename=original_filename,
    )
