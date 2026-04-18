from __future__ import annotations

from pathlib import Path
import re

from services.parsers.utils import clean_extracted_text


_TEXT_READ_ENCODINGS = (
    "utf-8-sig",
    "utf-8",
    "utf-16",
    "utf-16-le",
    "utf-16-be",
    "cp1252",
    "latin-1",
)


def _read_plain_text(file_path: Path) -> str:
    raw_bytes = file_path.read_bytes()
    for encoding in _TEXT_READ_ENCODINGS:
        try:
            return raw_bytes.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw_bytes.decode("utf-8", errors="replace")


def _split_paragraphs(text: str) -> list[str]:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    paragraphs = [
        clean_extracted_text(chunk)
        for chunk in re.split(r"\n\s*\n+", normalized)
    ]
    paragraphs = [chunk for chunk in paragraphs if chunk]
    if paragraphs:
        return paragraphs

    fallback_lines = [
        clean_extracted_text(line)
        for line in normalized.split("\n")
    ]
    return [line for line in fallback_lines if line]


def parse_text_document(file_path, *, original_filename: str | None = None) -> dict:
    source_path = Path(file_path)
    metadata = {
        "parser": "text_plain",
        "source_path": str(source_path),
        "original_filename": str(original_filename or source_path.name),
    }

    try:
        raw_text = _read_plain_text(source_path)
        paragraphs = _split_paragraphs(raw_text)
        segments = []
        for paragraph_index, paragraph in enumerate(paragraphs, start=1):
            segments.append(
                {
                    "segment_id": f"text-document-para-{paragraph_index}",
                    "source_type": "document",
                    "source_index": 1,
                    "block_index": 0,
                    "paragraph_index": paragraph_index,
                    "text": paragraph,
                    "metadata": {
                        "char_count": len(paragraph),
                        "parser_adapter": "text",
                        "source_anchor_key": f"document:1:paragraph:{paragraph_index}",
                    },
                }
            )

        metadata.update(
            {
                "character_count": len(raw_text),
                "paragraph_count": len(segments),
                "line_count": raw_text.count("\n") + (1 if raw_text else 0),
            }
        )
        return {
            "segments": segments,
            "assets": [],
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
                    "code": "text_parse_error",
                    "message": "Unable to parse TXT file.",
                    "details": {
                        "exception": str(exc),
                        "source_path": str(source_path),
                    },
                }
            ],
        }
