from pathlib import Path
import mimetypes
import re

from services.parsers.mineru import MINERU_SUPPORTED_EXTENSIONS, parse_document_with_mineru
from services.parsers.text_parser import parse_text_document
from services.parsers.image_parser import parse_image_document

EXTENSION_TO_TYPE = {
    ".pdf": "pdf",
    ".ppt": "ppt",
    ".doc": "doc",
    ".docx": "docx",
    ".pptx": "pptx",
    ".txt": "txt",
    ".png": "png",
    ".jpg": "jpg",
    ".jpeg": "jpeg",
    ".webp": "webp",
}

MIME_TO_TYPE = {
    "application/pdf": "pdf",
    "application/vnd.ms-powerpoint": "ppt",
    "application/msword": "doc",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": "pptx",
    "text/plain": "txt",
    "image/png": "png",
    "image/jpeg": "jpg",
    "image/webp": "webp",
}

LOCAL_TEXT_TYPES = {"txt"}
LOCAL_IMAGE_TYPES = {"png", "jpg", "jpeg", "webp"}


def _segment_sort_key(segment):
    def _safe_int(value, default=0):
        if value is None:
            return default
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    source_type_order = {
        "page": 0,
        "slide": 1,
        "paragraph": 2,
        "table": 3,
        "formula": 4,
        "image": 5,
    }
    return (
        source_type_order.get(segment.get("source_type"), 99),
        _safe_int(segment.get("source_index"), 0),
        _safe_int(segment.get("block_index"), 0),
        _safe_int(segment.get("paragraph_index"), 0),
        segment.get("segment_id", ""),
    )


def _detect_heading_level(text: str) -> int:
    compact = " ".join((text or "").split())
    if not compact:
        return 2

    # Academic numbering hierarchy:
    # 1.Introduction -> H1
    # 1.1 Motivation -> H2
    # 1.1.1 Details -> H3
    if re.match(r"^\d+\.\d+\.\d+", compact):
        return 3
    if re.match(r"^\d+\.\d+", compact):
        return 2
    if re.match(r"^\d+", compact):
        return 1

    word_count = len(compact.split())
    if compact.isupper() and word_count <= 8:
        return 1
    if word_count <= 6:
        return 2
    return 3


def _segment_to_markdown(segment: dict) -> str:
    text = str(segment.get("text") or "").strip()
    if not text:
        return ""

    metadata = segment.get("metadata") or {}
    role = str(metadata.get("role") or "").strip().lower()
    source_type = str(segment.get("source_type") or "").strip().lower()

    if role == "heading":
        level = _detect_heading_level(text)
        return f"{'#' * level} {text}"
    if role == "list":
        return text
    if source_type == "table":
        return text
    return text


def _build_markdown_output(segments: list[dict]) -> str:
    if not segments:
        return ""
    markdown_chunks = [_segment_to_markdown(seg) for seg in segments]
    markdown_chunks = [chunk for chunk in markdown_chunks if chunk]
    return "\n\n".join(markdown_chunks).strip()


def build_parser_error(code, message, details=None):
    return {
        "code": code,
        "message": message,
        "details": details or {},
    }


def _base_result(document_id=None, file_type=None):
    return {
        "document_id": str(document_id) if document_id is not None else None,
        "file_type": file_type,
        "segments": [],
        "assets": [],
        "references": [],
        "metadata": {},
        "errors": [],
    }


def detect_file_type(file_path, mime_type=None, original_filename=None):
    file_path = Path(file_path)
    extension = file_path.suffix.lower()
    if extension in EXTENSION_TO_TYPE:
        return EXTENSION_TO_TYPE[extension]

    candidate_name = original_filename or file_path.name
    guessed_mime, _ = mimetypes.guess_type(candidate_name)
    normalized_mime = (mime_type or guessed_mime or "").split(";")[0].strip().lower()
    return MIME_TO_TYPE.get(normalized_mime)


def parse_document(file_path, document_id=None, mime_type=None, original_filename=None, progress_callback=None):
    result = _base_result(document_id=document_id)
    try:
        file_type = detect_file_type(
            file_path=file_path,
            mime_type=mime_type,
            original_filename=original_filename,
        )
        result["file_type"] = file_type

        if not file_type:
            result["errors"].append(
                build_parser_error(
                    code="unsupported_file_type",
                    message="Unsupported document format for parsing.",
                    details={
                        "file_path": str(file_path),
                        "mime_type": mime_type,
                        "original_filename": original_filename,
                    },
                )
            )
            return result

        extension = Path(original_filename or file_path).suffix.lower()
        if file_type in LOCAL_TEXT_TYPES:
            parser_result = parse_text_document(
                file_path,
                original_filename=original_filename,
            )
        elif file_type in LOCAL_IMAGE_TYPES:
            parser_result = parse_image_document(
                file_path,
                original_filename=original_filename,
            )
        else:
            if extension not in MINERU_SUPPORTED_EXTENSIONS:
                result["errors"].append(
                    build_parser_error(
                        code="unsupported_mineru_file_type",
                        message="This document type is not enabled for MinerU parsing.",
                        details={
                            "file_type": file_type,
                            "extension": extension,
                            "supported_extensions": sorted(MINERU_SUPPORTED_EXTENSIONS),
                        },
                    )
                )
                return result

            parser_result = parse_document_with_mineru(
                file_path,
                progress_callback=progress_callback,
                document_id=document_id,
                original_filename=original_filename,
            )
        result["segments"] = sorted(parser_result.get("segments", []), key=_segment_sort_key)
        result["assets"] = parser_result.get("assets", [])
        result["references"] = parser_result.get("references", [])
        result["metadata"] = parser_result.get("metadata", {})
        markdown_output = _build_markdown_output(result["segments"])
        result["markdown_output"] = markdown_output
        result["metadata"] = {
            **result["metadata"],
            "markdown_output": markdown_output,
        }
        errors = parser_result.get("errors", [])
        if errors:
            result["errors"].extend(errors)
        return result
    except Exception as exc:
        result["errors"].append(
            build_parser_error(
                code="parser_failure",
                message="Document parsing failed.",
                details={"exception": str(exc), "file_path": str(file_path)},
            )
        )
        return result
