from pathlib import Path

from services.parsers.utils import clean_extracted_text


def _dependency_error(package_name):
    return {
        "code": "missing_dependency",
        "message": f"Required parser dependency '{package_name}' is not installed.",
        "details": {"package": package_name},
    }


def _segment_sort_key(segment):
    return (
        segment.get("source_index", 0),
        segment.get("block_index", 0),
        segment.get("paragraph_index", 0),
        segment.get("segment_id", ""),
    )


def parse_pdf(file_path):
    try:
        import fitz
    except Exception:
        return {
            "segments": [],
            "metadata": {},
            "errors": [_dependency_error("PyMuPDF")],
        }

    file_path = Path(file_path)
    segments = []
    metadata = {
        "source_path": str(file_path),
        "page_count": 0,
    }

    document = None
    try:
        document = fitz.open(file_path)
        metadata["page_count"] = document.page_count

        for page_index, page in enumerate(document, start=1):
            text = clean_extracted_text(page.get_text("text") or "")
            if not text:
                continue
            segments.append(
                {
                    "segment_id": f"pdf-page-{page_index}",
                    "source_type": "page",
                    "source_index": page_index,
                    "text": text,
                    "metadata": {
                        "char_count": len(text),
                        "parser_adapter": "pdf",
                    },
                }
            )

        segments.sort(key=_segment_sort_key)
        return {"segments": segments, "metadata": metadata, "errors": []}
    except Exception as exc:
        return {
            "segments": [],
            "metadata": metadata,
            "errors": [
                {
                    "code": "pdf_parse_error",
                    "message": "Unable to parse PDF file.",
                    "details": {"exception": str(exc), "source_path": str(file_path)},
                }
            ],
        }
    finally:
        if document is not None:
            document.close()
