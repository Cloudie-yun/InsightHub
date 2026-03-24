from pathlib import Path


def _dependency_error(package_name):
    return {
        "code": "missing_dependency",
        "message": f"Required parser dependency '{package_name}' is not installed.",
        "details": {"package": package_name},
    }


def parse_pptx(file_path):
    try:
        from pptx import Presentation
    except Exception:
        return {
            "segments": [],
            "metadata": {},
            "errors": [_dependency_error("python-pptx")],
        }

    file_path = Path(file_path)
    segments = []
    metadata = {
        "source_path": str(file_path),
        "slide_count": 0,
    }

    try:
        presentation = Presentation(file_path)
        metadata["slide_count"] = len(presentation.slides)

        for slide_index, slide in enumerate(presentation.slides, start=1):
            slide_text_parts = []
            for shape in slide.shapes:
                if not hasattr(shape, "text"):
                    continue
                text = (shape.text or "").strip()
                if text:
                    slide_text_parts.append(text)

            if not slide_text_parts:
                continue

            slide_text = "\n".join(slide_text_parts)
            segments.append(
                {
                    "segment_id": f"pptx-slide-{slide_index}",
                    "type": "slide",
                    "slide_number": slide_index,
                    "text": slide_text,
                    "metadata": {
                        "shape_text_count": len(slide_text_parts),
                        "char_count": len(slide_text),
                    },
                }
            )

        return {"segments": segments, "metadata": metadata, "errors": []}
    except Exception as exc:
        return {
            "segments": [],
            "metadata": metadata,
            "errors": [
                {
                    "code": "pptx_parse_error",
                    "message": "Unable to parse PPTX file.",
                    "details": {"exception": str(exc), "source_path": str(file_path)},
                }
            ],
        }
