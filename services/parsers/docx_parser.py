from pathlib import Path


def _dependency_error(package_name):
    return {
        "code": "missing_dependency",
        "message": f"Required parser dependency '{package_name}' is not installed.",
        "details": {"package": package_name},
    }


def parse_docx(file_path):
    try:
        from docx import Document
    except Exception:
        return {
            "segments": [],
            "metadata": {},
            "errors": [_dependency_error("python-docx")],
        }

    file_path = Path(file_path)
    segments = []
    metadata = {
        "source_path": str(file_path),
        "paragraph_count": 0,
    }

    try:
        document = Document(file_path)

        for index, paragraph in enumerate(document.paragraphs, start=1):
            text = (paragraph.text or "").strip()
            if not text:
                continue
            segments.append(
                {
                    "segment_id": f"docx-paragraph-{index}",
                    "type": "paragraph",
                    "paragraph_number": index,
                    "text": text,
                    "metadata": {"char_count": len(text)},
                }
            )

        metadata["paragraph_count"] = len(document.paragraphs)
        return {"segments": segments, "metadata": metadata, "errors": []}
    except Exception as exc:
        return {
            "segments": [],
            "metadata": metadata,
            "errors": [
                {
                    "code": "docx_parse_error",
                    "message": "Unable to parse DOCX file.",
                    "details": {"exception": str(exc), "source_path": str(file_path)},
                }
            ],
        }
