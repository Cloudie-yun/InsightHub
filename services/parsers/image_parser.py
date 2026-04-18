from __future__ import annotations

from pathlib import Path
import hashlib
import mimetypes
import re


def _compute_content_hash(file_path: Path) -> str:
    digest = hashlib.sha256()
    with file_path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _build_caption_from_name(filename: str) -> str:
    stem = Path(filename).stem
    cleaned = re.sub(r"[_-]+", " ", stem)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def parse_image_document(file_path, *, original_filename: str | None = None) -> dict:
    source_path = Path(file_path)
    original_name = str(original_filename or source_path.name)
    mime_type = mimetypes.guess_type(original_name)[0] or mimetypes.guess_type(str(source_path))[0] or "image/png"
    caption_text = _build_caption_from_name(original_name)
    metadata = {
        "parser": "image_upload",
        "source_path": str(source_path),
        "original_filename": original_name,
        "auto_vision_requested": True,
    }

    try:
        file_size = source_path.stat().st_size
        content_hash = _compute_content_hash(source_path)
        asset = {
            "asset_id": "image-asset-1",
            "asset_type": "image",
            "storage_path": str(source_path),
            "upload_path": "",
            "mime_type": mime_type,
            "byte_size": file_size,
            "content_hash": content_hash,
            "source_index": 1,
            "metadata": {
                "caption_text": caption_text,
                "parser_adapter": "image",
                "source_anchor_key": "image:1",
                "original_filename": original_name,
            },
        }
        metadata.update(
            {
                "asset_count": 1,
                "mime_type": mime_type,
                "character_count": 0,
            }
        )
        return {
            "segments": [],
            "assets": [asset],
            "references": [],
            "metadata": metadata,
            "errors": [],
        }
    except Exception as exc:
        return {
            "segments": [],
            "assets": [],
            "references": [],
            "metadata": metadata,
            "errors": [
                {
                    "code": "image_parse_error",
                    "message": "Unable to prepare image upload for vision analysis.",
                    "details": {
                        "exception": str(exc),
                        "source_path": str(source_path),
                    },
                }
            ],
        }
