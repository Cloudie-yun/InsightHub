from pathlib import Path


def _dependency_error(package_name):
    return {
        "code": "missing_dependency",
        "message": f"Required parser dependency '{package_name}' is not installed.",
        "details": {"package": package_name},
    }


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
            text = (page.get_text("text") or "").strip()
            if not text:
                continue
            segments.append(
                {
                    "segment_id": f"pdf-page-{page_index}",
                    "type": "page",
                    "page_number": page_index,
                    "text": text,
                    "metadata": {"char_count": len(text)},
                }
            )

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
