from pathlib import Path
import mimetypes

from services.parsers.docx_parser import parse_docx
from services.parsers.pdf_parser import parse_pdf
from services.parsers.pptx_parser import parse_pptx

EXTENSION_TO_TYPE = {
    ".pdf": "pdf",
    ".docx": "docx",
    ".pptx": "pptx",
}

MIME_TO_TYPE = {
    "application/pdf": "pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": "pptx",
}

PARSER_BY_TYPE = {
    "pdf": parse_pdf,
    "docx": parse_docx,
    "pptx": parse_pptx,
}


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


def parse_document(file_path, document_id=None, mime_type=None, original_filename=None):
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

        parser = PARSER_BY_TYPE.get(file_type)
        if not parser:
            result["errors"].append(
                build_parser_error(
                    code="missing_parser_adapter",
                    message=f"No parser adapter is configured for '{file_type}'.",
                )
            )
            return result

        parser_result = parser(file_path)
        result["segments"] = parser_result.get("segments", [])
        result["metadata"] = parser_result.get("metadata", {})
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
