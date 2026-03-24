import re


def clean_extracted_text(text: str) -> str:
    """
    Normalize extracted document text:
    1) Replace single line breaks within paragraphs with spaces.
    2) Preserve paragraph separators (double+ line breaks).
    3) Collapse repeated spaces/tabs.
    4) Trim leading/trailing whitespace.
    """
    if text is None:
        return ""

    # Normalize line endings first
    cleaned = text.replace("\r\n", "\n").replace("\r", "\n")

    # Temporarily protect paragraph breaks (2+ newlines)
    cleaned = re.sub(r"\n{2,}", "<<<PARA_BREAK>>>", cleaned)

    # Single newlines inside paragraphs -> space
    cleaned = cleaned.replace("\n", " ")

    # Collapse repeated spaces/tabs
    cleaned = re.sub(r"[ \t]+", " ", cleaned)

    # Restore paragraph breaks as exactly two newlines
    cleaned = cleaned.replace("<<<PARA_BREAK>>>", "\n\n")

    # Remove extra spaces around paragraph separators
    cleaned = re.sub(r" *\n\n *", "\n\n", cleaned)

    return cleaned.strip()
