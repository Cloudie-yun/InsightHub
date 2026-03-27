from __future__ import annotations

import io
import json
import re
import zipfile
from pathlib import Path

from services.parsers.utils import clean_extracted_text, encode_image_bytes, table_to_markdown

from .client import request_with_retry
from .constants import IMAGE_MIME_BY_EXT


def safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def download_zip(zip_url: str) -> zipfile.ZipFile:
    response = request_with_retry("GET", zip_url, _timeout_seconds=300)
    return zipfile.ZipFile(io.BytesIO(response.content))


def parse_zip(zf: zipfile.ZipFile, source_path: str) -> list[dict]:
    names = zf.namelist()

    content_list_name = next((n for n in names if n.endswith("content_list.json")), None)
    if content_list_name:
        return parse_content_list(zf, content_list_name, names)

    markdown_name = next((n for n in names if n.lower().endswith(".md")), None)
    if markdown_name:
        markdown_text = zf.read(markdown_name).decode("utf-8", errors="replace")
        return parse_markdown_fallback(markdown_text, source_path)

    return []


def parse_content_list(zf: zipfile.ZipFile, content_list_name: str, all_names: list[str]) -> list[dict]:
    raw = zf.read(content_list_name).decode("utf-8", errors="replace")
    items = json.loads(raw)
    if not isinstance(items, list):
        return []

    image_lookup: dict[str, str] = {}
    for name in all_names:
        lower = name.lower()
        ext = lower.rsplit(".", 1)[-1] if "." in lower else ""
        if ext not in IMAGE_MIME_BY_EXT:
            continue
        try:
            img_bytes = zf.read(name)
            mime = IMAGE_MIME_BY_EXT[ext]
            data_uri = encode_image_bytes(img_bytes, mime)
            image_lookup[name] = data_uri
            image_lookup[Path(name).name] = data_uri
        except Exception:
            continue

    segments: list[dict] = []
    seg_counter: dict[int, int] = {}

    for item in items:
        if not isinstance(item, dict):
            continue

        page_index = safe_int(item.get("page_idx", 0), 0) + 1
        seg_counter[page_index] = seg_counter.get(page_index, 0) + 1
        seg_num = seg_counter[page_index]
        seg_id = f"mineru-page-{page_index}-{seg_num}"
        item_type = str(item.get("type") or "text").lower()
        bbox = item.get("bbox") or []

        if item_type in {"text", "title", "interleaved_title"}:
            text = clean_extracted_text(item.get("text") or "")
            if not text:
                continue
            role = "heading" if item_type in {"title", "interleaved_title"} else "paragraph"
            segments.append({
                "segment_id": seg_id,
                "source_type": "paragraph",
                "source_index": page_index,
                "block_index": seg_num,
                "paragraph_index": seg_num,
                "text": text,
                "metadata": {
                    "parser_adapter": "mineru",
                    "bbox": bbox,
                    "char_count": len(text),
                    "role": role,
                },
            })
            continue

        if item_type == "table":
            html = item.get("html") or ""
            markdown = html_table_to_markdown(html) if html else ""
            if not markdown:
                markdown = clean_extracted_text(item.get("text") or "")
            if not markdown:
                continue
            segments.append({
                "segment_id": seg_id,
                "source_type": "table",
                "source_index": page_index,
                "block_index": None,
                "paragraph_index": None,
                "text": markdown,
                "metadata": {
                    "parser_adapter": "mineru",
                    "bbox": bbox,
                    "char_count": len(markdown),
                },
            })
            continue

        if item_type in {"image", "figure"}:
            img_path = item.get("img_path") or ""
            data_uri = image_lookup.get(img_path) or image_lookup.get(Path(img_path).name)
            caption = clean_extracted_text(item.get("text") or "")
            segments.append({
                "segment_id": seg_id,
                "source_type": "image",
                "source_index": page_index,
                "block_index": None,
                "paragraph_index": None,
                "text": caption,
                "metadata": {
                    "parser_adapter": "mineru",
                    "bbox": bbox,
                    "char_count": len(caption),
                    "data_uri": data_uri,
                },
            })
            continue

        text = clean_extracted_text(item.get("text") or "")
        if text:
            segments.append({
                "segment_id": seg_id,
                "source_type": "paragraph",
                "source_index": page_index,
                "block_index": seg_num,
                "paragraph_index": seg_num,
                "text": text,
                "metadata": {
                    "parser_adapter": "mineru",
                    "bbox": bbox,
                    "char_count": len(text),
                    "role": "paragraph",
                    "mineru_type": item_type,
                },
            })

    return segments


def html_table_to_markdown(html: str) -> str:
    html = (html or "").strip()
    if not html:
        return ""
    return parse_table_bs4(html) or parse_table_regex(html)


def parse_table_bs4(html: str) -> str:
    try:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")
        rows = []
        for tr in soup.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            row = [clean_extracted_text(cell.get_text(" ", strip=True)) for cell in cells]
            if row:
                rows.append(row)
        if rows:
            return table_to_markdown(rows, has_header=True)
    except Exception:
        return ""
    return ""


def parse_table_regex(html: str) -> str:
    rows = []
    tr_matches = re.findall(r"<tr\b.*?>(.*?)</tr>", html, flags=re.I | re.S)
    for tr_html in tr_matches:
        cell_matches = re.findall(r"<t[hd]\b.*?>(.*?)</t[hd]>", tr_html, flags=re.I | re.S)
        row = []
        for cell_html in cell_matches:
            cell_text = re.sub(r"<[^>]+>", " ", cell_html)
            row.append(clean_extracted_text(cell_text))
        if row:
            rows.append(row)
    return table_to_markdown(rows, has_header=True) if rows else ""


def parse_markdown_fallback(markdown_text: str, source_path: str) -> list[dict]:
    segments: list[dict] = []
    counter = 0
    for block in re.split(r"\n{2,}", (markdown_text or "").strip()):
        text = clean_extracted_text(block.strip())
        if not text:
            continue
        counter += 1
        role = "heading" if re.match(r"^#{1,6}\s", text) else "paragraph"
        if role == "heading":
            text = re.sub(r"^#{1,6}\s+", "", text)
        segments.append({
            "segment_id": f"mineru-fallback-{counter}",
            "source_type": "paragraph",
            "source_index": 1,
            "block_index": counter,
            "paragraph_index": counter,
            "text": text,
            "metadata": {
                "parser_adapter": "mineru_md_fallback",
                "char_count": len(text),
                "role": role,
                "source_path": source_path,
            },
        })
    return segments
