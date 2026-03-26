import base64
import re

_LIST_LINE_PATTERN = re.compile(
    r"^\s*(?:[\-\*\u2022\u2023\u25E6\u2043\u2219]|(?:\d+|[A-Za-z]|[ivxlcdm]+)[\.\)])\s+",
    re.IGNORECASE,
)


def _looks_like_list_line(line: str) -> bool:
    return bool(_LIST_LINE_PATTERN.match((line or "").strip()))


def normalize_extracted_line(text: str) -> str:
    """
    Lightweight per-line normalization only.
    Does NOT make any paragraph boundary decisions.
    Safe to call on individual lines or single blocks.
    """
    if not text:
        return ""

    cleaned = text.replace("\r\n", "\n").replace("\r", "\n")

    # Rejoin hyphenated line-breaks within a single block
    cleaned = re.sub(r"-\n(?=[a-z])", "", cleaned)

    # Collapse horizontal whitespace only
    cleaned = re.sub(r"[ \t]+", " ", cleaned)

    return cleaned.strip()


def clean_extracted_text(text: str) -> str:
    """
    Full normalization for already-segmented text.
    Call this ONLY on a finalized segment — after geometry-based paragraph
    assembly is complete. Never call this during block extraction or merging.
    """
    if not text:
        return ""

    cleaned = text.replace("\r\n", "\n").replace("\r", "\n")
    cleaned = re.sub(r"-\n(?=[a-z])", "", cleaned)

    # Collapse runs of blank lines to a single paragraph break
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)

    # Collapse horizontal whitespace
    cleaned = re.sub(r"[ \t]+", " ", cleaned)

    # Basic fallback behavior:
    # - join wrapped prose lines into a single line per paragraph
    # - keep list line breaks for list-only paragraphs
    paragraphs = re.split(r"\n\s*\n", cleaned)
    normalized_paragraphs: list[str] = []

    for paragraph in paragraphs:
        lines = [line.strip() for line in paragraph.split("\n") if line.strip()]
        if not lines:
            continue

        if all(_looks_like_list_line(line) for line in lines):
            normalized_paragraphs.append("\n".join(lines))
        else:
            normalized_paragraphs.append(" ".join(lines))

    return "\n\n".join(normalized_paragraphs).strip()


def encode_image_bytes(image_bytes: bytes, media_type: str = "image/png") -> str:
    """Base64-encode raw image bytes into a data URI string."""
    b64 = base64.b64encode(image_bytes).decode("utf-8")
    return f"data:{media_type};base64,{b64}"


def table_to_markdown(rows: list[list[str]], has_header: bool = True) -> str:
    """Convert a 2D list of strings into a Markdown table."""
    if not rows:
        return ""

    col_count = max(len(r) for r in rows)
    normalized = []
    for row in rows:
        padded = [str(cell).strip() for cell in row]
        while len(padded) < col_count:
            padded.append("")
        normalized.append(padded)

    col_widths = [
        max(len(normalized[r][c]) for r in range(len(normalized)))
        for c in range(col_count)
    ]
    col_widths = [max(w, 3) for w in col_widths]

    def fmt_row(row: list[str]) -> str:
        cells = [f" {cell.ljust(col_widths[c])} " for c, cell in enumerate(row)]
        return "|" + "|".join(cells) + "|"

    lines = []
    if has_header and normalized:
        lines.append(fmt_row(normalized[0]))
        divider = ["-" * (col_widths[c] + 2) for c in range(col_count)]
        lines.append("|" + "|".join(divider) + "|")
        for row in normalized[1:]:
            lines.append(fmt_row(row))
    else:
        for row in normalized:
            lines.append(fmt_row(row))

    return "\n".join(lines)


_FORMULA_PATTERNS = [
    re.compile(r"\\[a-zA-Z]+\{"),
    re.compile(r"\$.*?\$"),
    re.compile(r"\\\(.*?\\\)"),
    re.compile(r"\b\d+\s*/\s*\d+\b"),
    re.compile(r"[a-zA-Z]\s*[=<>]\s*[a-zA-Z0-9]"),
    re.compile(r"\^[\d{]"),
    re.compile(r"_[\d{]"),
]


def looks_like_formula(text: str) -> bool:
    """Return True when text appears to be a mathematical expression."""
    return any(p.search(text or "") for p in _FORMULA_PATTERNS)
