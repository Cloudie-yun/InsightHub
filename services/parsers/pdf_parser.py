from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from difflib import SequenceMatcher
from math import ceil
from pathlib import Path
import io
import re
import statistics
import unicodedata

from services.parsers.utils import (
    clean_extracted_text,
    encode_image_bytes,
    normalize_extracted_line,
    table_to_markdown,
)


FITZ_EXT_TO_MIME = {
    "png": "image/png",
    "jpeg": "image/jpeg",
    "jpg": "image/jpeg",
    "jpx": "image/jp2",
    "j2k": "image/jp2",
    "bmp": "image/bmp",
    "gif": "image/gif",
    "tiff": "image/tiff",
}

_RUNNING_HEAD_PATTERNS = [
    re.compile(r"^\d{3,5}\s*$"),                          # bare page numbers
    re.compile(r"^ISSN\s+[\d\-]+", re.IGNORECASE),        # ISSN lines
    re.compile(r"Radioelectronic", re.IGNORECASE),         # journal name fragments
    re.compile(r"^(Information security|functional safety)", re.IGNORECASE),
    re.compile(r"^Page\s+\d+", re.IGNORECASE),
    re.compile(r"^[-–—]{3,}\s*$"),
]

_HEADER_BAND_RATIO = 0.08
_FOOTER_BAND_RATIO = 0.92
_MIN_DIGITAL_WORDS = 8
_MIN_DIGITAL_CHARS = 30
_OCR_DPI = 220


@dataclass
class PageClassification:
    mode: str
    has_text_layer: bool
    text_char_count: int
    word_count: int
    image_count: int
    image_area_ratio: float
    confidence: float


@dataclass
class TextBlock:
    text: str
    x0: float
    y0: float
    x1: float
    y1: float
    font_size: float
    is_bold: bool
    direction: tuple[float, float]
    source: str

    @property
    def width(self) -> float:
        return max(0.0, self.x1 - self.x0)

    @property
    def height(self) -> float:
        return max(0.0, self.y1 - self.y0)

    @property
    def center_x(self) -> float:
        return (self.x0 + self.x1) / 2.0


@dataclass
class RegionLayout:
    y0: float
    y1: float
    column_centers: list[float]


class PageClassifier:
    def classify(self, page) -> PageClassification:
        rect = page.rect
        page_area = max(1.0, float(rect.width) * float(rect.height))

        words = page.get_text("words") or []
        text = page.get_text("text") or ""
        text_chars = len(text.strip())
        has_text_layer = len(words) >= _MIN_DIGITAL_WORDS and text_chars >= _MIN_DIGITAL_CHARS

        image_area = 0.0
        image_count = 0
        for img in page.get_images(full=True):
            xref = img[0]
            rects = page.get_image_rects(xref)
            if not rects:
                continue
            image_count += 1
            image_area += sum(max(0.0, r.width * r.height) for r in rects)

        image_ratio = min(1.0, image_area / page_area)

        if has_text_layer and image_ratio < 0.45:
            mode = "digital"
            confidence = 0.9 if image_ratio < 0.2 else 0.75
        elif not has_text_layer and image_ratio > 0.2:
            mode = "scanned"
            confidence = 0.85
        else:
            mode = "hybrid"
            confidence = 0.65

        return PageClassification(
            mode=mode,
            has_text_layer=has_text_layer,
            text_char_count=text_chars,
            word_count=len(words),
            image_count=image_count,
            image_area_ratio=image_ratio,
            confidence=confidence,
        )


class LayoutDetector:
    def detect(self, blocks: list[TextBlock], page_width: float) -> list[RegionLayout]:
        if not blocks:
            return []

        ordered = sorted(blocks, key=lambda b: (b.y0, b.x0))
        median_h = statistics.median([max(8.0, b.height) for b in ordered])
        split_gap = max(24.0, median_h * 2.0)

        regions: list[list[TextBlock]] = [[ordered[0]]]
        for block in ordered[1:]:
            prev = regions[-1][-1]
            if block.y0 - prev.y1 > split_gap:
                regions.append([block])
            else:
                regions[-1].append(block)

        layouts: list[RegionLayout] = []
        for region_blocks in regions:
            y0 = min(b.y0 for b in region_blocks)
            y1 = max(b.y1 for b in region_blocks)
            centers = self._detect_columns(region_blocks, page_width)
            layouts.append(RegionLayout(y0=y0, y1=y1, column_centers=centers))

        return layouts

    def _detect_columns(self, region_blocks: list[TextBlock], page_width: float) -> list[float]:
        if len(region_blocks) < 3:
            return [statistics.mean([b.center_x for b in region_blocks])]

        narrow_blocks = [b for b in region_blocks if b.width < page_width * 0.72]
        if len(narrow_blocks) < 3:
            return [statistics.mean([b.center_x for b in region_blocks])]

        centers = sorted(b.center_x for b in narrow_blocks)
        split_threshold = max(28.0, page_width * 0.12)

        groups: list[list[float]] = [[centers[0]]]
        for c in centers[1:]:
            if c - groups[-1][-1] > split_threshold and len(groups) < 3:
                groups.append([c])
            else:
                groups[-1].append(c)

        if len(groups) == 1:
            return [statistics.mean(centers)]
        return [statistics.mean(group) for group in groups]


class TextExtractor:
    def extract(self, page, classification: PageClassification) -> tuple[list[TextBlock], list[str]]:
        warnings: list[str] = []
        blocks = self._extract_digital_blocks(page)

        if self._is_encoding_suspect(blocks):
            warnings.append("text_encoding_suspect")

        need_ocr = (
            classification.mode in {"scanned", "hybrid"}
            and (not classification.has_text_layer or len(blocks) < 3)
        )
        if need_ocr:
            ocr_blocks, ocr_warnings = self._extract_ocr_blocks(page)
            warnings.extend(ocr_warnings)
            if ocr_blocks:
                if classification.mode == "hybrid" and blocks:
                    blocks.extend(ocr_blocks)
                else:
                    blocks = ocr_blocks

        return blocks, warnings

    def _extract_digital_blocks(self, page) -> list[TextBlock]:
        page_dict = page.get_text("dict")
        extracted: list[TextBlock] = []

        for raw in page_dict.get("blocks", []):
            if raw.get("type") != 0:
                continue

            lines: list[str] = []
            font_sizes: list[float] = []
            is_bold = False
            direction = (1.0, 0.0)

            for line in raw.get("lines", []):
                spans = line.get("spans", [])
                text = "".join((s.get("text") or "") for s in spans)
                text = normalize_extracted_line(text)
                if text:
                    lines.append(text)

                for span in spans:
                    size = span.get("size")
                    if isinstance(size, (int, float)) and size > 0:
                        font_sizes.append(float(size))
                    flags = span.get("flags", 0)
                    if flags & 16:
                        is_bold = True

                d = line.get("dir")
                if isinstance(d, (list, tuple)) and len(d) == 2:
                    try:
                        direction = (float(d[0]), float(d[1]))
                    except Exception:
                        pass

            joined = "\n".join(lines).strip()
            if not joined:
                continue

            x0, y0, x1, y1 = raw.get("bbox", (0, 0, 0, 0))
            extracted.append(
                TextBlock(
                    text=joined,
                    x0=float(x0),
                    y0=float(y0),
                    x1=float(x1),
                    y1=float(y1),
                    font_size=statistics.median(font_sizes) if font_sizes else 0.0,
                    is_bold=is_bold,
                    direction=direction,
                    source="digital",
                )
            )

        return extracted

    def _extract_ocr_blocks(self, page) -> tuple[list[TextBlock], list[str]]:
        warnings: list[str] = []
        try:
            import pytesseract
            from PIL import Image
        except Exception:
            return [], ["ocr_dependency_missing"]

        try:
            matrix = self._dpi_matrix(page)
            pix = page.get_pixmap(matrix=matrix, alpha=False)
            image = Image.open(io.BytesIO(pix.tobytes("png")))
            data = pytesseract.image_to_data(image, output_type=pytesseract.Output.DICT)
        except Exception:
            return [], ["ocr_failed"]

        blocks: list[TextBlock] = []
        n = len(data.get("text", []))
        for i in range(n):
            text = normalize_extracted_line(data["text"][i] or "")
            conf_text = str(data.get("conf", ["-1"])[i])
            try:
                conf = float(conf_text)
            except Exception:
                conf = -1.0

            if not text or conf < 35:
                continue

            l = float(data["left"][i])
            t = float(data["top"][i])
            w = float(data["width"][i])
            h = float(data["height"][i])
            x0, y0, x1, y1 = self._pixel_to_pdf_coords(page, l, t, w, h)

            blocks.append(
                TextBlock(
                    text=text,
                    x0=x0,
                    y0=y0,
                    x1=x1,
                    y1=y1,
                    font_size=0.0,
                    is_bold=False,
                    direction=(1.0, 0.0),
                    source="ocr",
                )
            )

        if not blocks:
            warnings.append("ocr_empty")

        return blocks, warnings

    def _dpi_matrix(self, page):
        import fitz

        zoom = _OCR_DPI / 72.0
        return fitz.Matrix(zoom, zoom)

    def _pixel_to_pdf_coords(self, page, left: float, top: float, width: float, height: float) -> tuple[float, float, float, float]:
        scale = 72.0 / _OCR_DPI
        x0 = left * scale
        y0 = top * scale
        x1 = (left + width) * scale
        y1 = (top + height) * scale
        x1 = min(float(page.rect.width), x1)
        y1 = min(float(page.rect.height), y1)
        return x0, y0, x1, y1

    def _is_encoding_suspect(self, blocks: list[TextBlock]) -> bool:
        joined = "\n".join(b.text for b in blocks)
        if not joined:
            return False

        private_use_chars = 0
        for ch in joined:
            code = ord(ch)
            if 0xE000 <= code <= 0xF8FF:
                private_use_chars += 1

        ratio = private_use_chars / max(1, len(joined))
        return ratio > 0.03


class ReadingOrderReconstructor:
    def order(self, blocks: list[TextBlock], layouts: list[RegionLayout]) -> list[TextBlock]:
        if not blocks:
            return []
        if not layouts:
            return sorted(blocks, key=lambda b: (b.y0, b.x0))

        ordered: list[TextBlock] = []
        remaining = set(range(len(blocks)))

        for layout in layouts:
            member_ids = [
                i for i, b in enumerate(blocks)
                if not (b.y1 < layout.y0 or b.y0 > layout.y1)
            ]
            if not member_ids:
                continue

            columns = self._assign_columns([blocks[i] for i in member_ids], layout.column_centers)
            rtl = self._looks_rtl([blocks[i] for i in member_ids])

            column_order = sorted(columns.keys(), reverse=rtl)
            for col in column_order:
                col_blocks = sorted(columns[col], key=lambda b: (b.y0, b.x0 if not rtl else -b.x1))
                for block in col_blocks:
                    idx = blocks.index(block)
                    if idx in remaining:
                        ordered.append(block)
                        remaining.remove(idx)

        for idx in sorted(remaining, key=lambda i: (blocks[i].y0, blocks[i].x0)):
            ordered.append(blocks[idx])

        return ordered

    def _assign_columns(self, blocks: list[TextBlock], centers: list[float]) -> dict[int, list[TextBlock]]:
        if len(centers) <= 1:
            return {0: blocks}

        result: dict[int, list[TextBlock]] = {i: [] for i in range(len(centers))}
        for block in blocks:
            idx = min(range(len(centers)), key=lambda i: abs(block.center_x - centers[i]))
            result[idx].append(block)
        return result

    def _looks_rtl(self, blocks: list[TextBlock]) -> bool:
        sample = " ".join(b.text for b in blocks[:12])
        if not sample:
            return False
        rtl_count = sum(1 for ch in sample if unicodedata.bidirectional(ch) in {"R", "AL", "AN"})
        return rtl_count > max(5, len(sample) * 0.2)


class StructureTagger:
    def build_segments(self, ordered_blocks: list[TextBlock], page_index: int, table_bboxes: list[list[float]]) -> list[dict]:
        content_blocks = [b for b in ordered_blocks if not self._inside_any_table(b, table_bboxes)]
        if not content_blocks:
            return []

        median_size = statistics.median([b.font_size for b in content_blocks if b.font_size > 0] or [11.0])
        paragraphs: list[list[TextBlock]] = [[content_blocks[0]]]

        for block in content_blocks[1:]:
            prev = paragraphs[-1][-1]
            if self._should_split(prev, block, median_size):
                paragraphs.append([block])
            else:
                paragraphs[-1].append(block)

        segments: list[dict] = []
        for i, para in enumerate(paragraphs, start=1):
            role = self._paragraph_role(para, median_size)
            text = self._merge_paragraph_text(para)
            text = clean_extracted_text(text)
            if not text:
                continue

            bbox = [
                min(b.x0 for b in para),
                min(b.y0 for b in para),
                max(b.x1 for b in para),
                max(b.y1 for b in para),
            ]

            segments.append({
                "segment_id": f"pdf-page-{page_index}-para-{i}",
                "source_type": "paragraph",
                "source_index": page_index,
                "block_index": i,
                "paragraph_index": i,
                "text": text,
                "metadata": {
                    "parser_adapter": "pdf_pipeline",
                    "bbox": bbox,
                    "block_count": len(para),
                    "char_count": len(text),
                    "role": role,           # "heading" | "paragraph" | "list"
                    "origin_sources": sorted({b.source for b in para}),
                },
            })

        # FIX 3: attach orphan headings to the next paragraph as metadata,
        # but keep them as SEPARATE segments so downstream can use role="heading"
        # to build a document tree. Do NOT merge heading text into the next segment.
        # (The old _attach_orphan_headings_to_next_segment pattern is removed.)

        return segments

    def _should_split(self, prev: TextBlock, current: TextBlock, median_size: float) -> bool:
        gap = current.y0 - prev.y1

        # FIX 1: use line-height-aware threshold, not just font size.
        # A typical line advance is ~1.2× font size. A paragraph gap is larger.
        # We split when gap > 1.0× the larger of the two blocks' font sizes,
        # which means: same-paragraph wrapped lines (gap ≈ 0) never split,
        # but separate paragraph blocks (gap > one line height) always split.
        effective_size = max(prev.font_size, current.font_size, 8.0)
        split_gap = effective_size * 1.0  # anything > one line height = new paragraph
        if gap > split_gap:
            return True

        # Different column = always split (x0 differs by more than half page)
        if abs(current.x0 - prev.x0) > 80.0:
            return True

        # List item starting after non-list = split
        if self._is_list_item(current.text) and not self._is_list_item(prev.text):
            return True

        # Heading always starts a new segment
        if self._is_heading_like(current.text, current, median_size):
            return True

        # Sentence end + capital start + meaningful indent = new paragraph
        prev_text = prev.text.strip()
        curr_text = current.text.strip()
        indent_change = abs(current.x0 - prev.x0)
        if (
            re.search(r"[.!?]\s*$", prev_text)
            and re.match(r"^[A-Z]", curr_text)
            and indent_change > 12.0
        ):
            return True

        return False
    def _paragraph_role(self, blocks: list[TextBlock], median_size: float) -> str:
        text = " ".join(b.text.strip() for b in blocks if b.text.strip())
        if not text:
            return "paragraph"

        if self._is_list_item(text):
            return "list"

        if self._is_heading_like(text, blocks[0], median_size):
            return "heading"

        return "paragraph"

    def _is_heading_like(self, text: str, block: TextBlock, median_size: float) -> bool:
        compact = " ".join(text.split())
        if not compact or len(compact) > 120:
            return False
        if re.match(r"^(chapter|section|\d+(?:\.\d+)+)", compact, re.IGNORECASE):
            return True
        if compact.isupper() and len(compact.split()) <= 12:
            return True
        if block.is_bold and block.font_size >= median_size * 1.08 and not re.search(r"[.!?]$", compact):
            return True
        return False

    def _is_list_item(self, text: str) -> bool:
        return bool(re.match(r"^\s*(?:[-*\u2022]|\d+[\.)]|[A-Za-z][\.)])\s+", text))

    def _merge_paragraph_text(self, blocks: list[TextBlock]) -> str:
        lines: list[str] = []
        for block in blocks:
            for line in block.text.splitlines():
                stripped = line.strip()
                if stripped:
                    lines.append(stripped)

        merged = ""
        for line in lines:
            if not merged:
                merged = line
                continue
            if merged.endswith("-") and line[:1].islower():
                merged = merged[:-1] + line
            elif self._is_list_item(line):
                merged += "\n" + line
            else:
                merged += " " + line
        return merged

    def _inside_any_table(self, block: TextBlock, bboxes: list[list[float]]) -> bool:
        cx = block.center_x
        cy = (block.y0 + block.y1) / 2.0
        for bbox in bboxes:
            if len(bbox) != 4:
                continue
            x0, y0, x1, y1 = bbox
            if x0 <= cx <= x1 and y0 <= cy <= y1:
                return True
        return False


def _extract_tables(page, page_index: int) -> tuple[list[dict], list[list[float]], list[str]]:
    warnings: list[str] = []
    try:
        table_result = page.find_tables()
    except Exception:
        return [], [], ["table_detector_unavailable"]

    segments: list[dict] = []
    bboxes: list[list[float]] = []

    for t_index, table in enumerate(table_result.tables, start=1):
        try:
            rows = table.extract()
        except Exception:
            warnings.append("table_extract_failed")
            continue

        if not rows:
            continue

        normalized_rows = [[cell if cell is not None else "" for cell in row] for row in rows]
        markdown = table_to_markdown(normalized_rows, has_header=True)
        if not markdown:
            continue

        bbox = list(table.bbox) if hasattr(table, "bbox") else []
        if len(bbox) == 4:
            bboxes.append([float(v) for v in bbox])

        has_sparse_rows = any(sum(1 for c in row if str(c).strip()) <= 1 for row in normalized_rows)
        confidence = 0.55 if has_sparse_rows else 0.8

        segments.append({
            "segment_id": f"pdf-page-{page_index}-table-{t_index}",
            "source_type": "table",
            "source_index": page_index,
            "block_index": None,
            "paragraph_index": None,
            "text": markdown,
            "metadata": {
                "parser_adapter": "pdf_pipeline",
                "bbox": bbox,
                "row_count": len(normalized_rows),
                "col_count": max(len(r) for r in normalized_rows),
                "confidence": confidence,
                "layout_hint": "borderless_possible" if has_sparse_rows else "grid_like",
            },
        })

    return segments, bboxes, warnings


def _extract_images(page, document, page_index: int) -> list[dict]:
    segments: list[dict] = []
    seen: set[int] = set()

    for i, img in enumerate(page.get_images(full=True), start=1):
        xref = img[0]
        if xref in seen:
            continue
        seen.add(xref)

        try:
            payload = document.extract_image(xref)
        except Exception:
            continue

        image_bytes = payload.get("image")
        if not image_bytes:
            continue

        ext = str(payload.get("ext", "png")).lower()
        media_type = FITZ_EXT_TO_MIME.get(ext, "image/png")
        rects = page.get_image_rects(xref)
        bbox = list(rects[0]) if rects else []

        segments.append({
            "segment_id": f"pdf-page-{page_index}-img-{i}",
            "source_type": "image",
            "source_index": page_index,
            "block_index": None,
            "paragraph_index": None,
            "text": "",
            "metadata": {
                "parser_adapter": "pdf_pipeline",
                "media_type": media_type,
                "width": payload.get("width"),
                "height": payload.get("height"),
                "bbox": bbox,
                "data_uri": encode_image_bytes(image_bytes, media_type),
            },
        })

    return segments


def _collect_header_footer_candidates(page_paragraphs: list[dict], page_height: float) -> tuple[set[str], set[str]]:
    headers: set[str] = set()
    footers: set[str] = set()
    head_limit = page_height * _HEADER_BAND_RATIO
    foot_start = page_height * _FOOTER_BAND_RATIO

    for seg in page_paragraphs:
        bbox = seg.get("metadata", {}).get("bbox") or []
        text = seg.get("text", "").strip()
        if not text:
            continue

        # Zone-based detection
        if len(bbox) == 4:
            y0, y1 = float(bbox[1]), float(bbox[3])
            if y1 <= head_limit:
                headers.add(text)
            if y0 >= foot_start:
                footers.add(text)

        # Pattern-based detection regardless of zone
        for line in text.splitlines():
            stripped = line.strip()
            if stripped and any(p.search(stripped) for p in _RUNNING_HEAD_PATTERNS):
                headers.add(stripped)  # add to headers pool for repeat counting

    return headers, footers


def _zone_tag_from_bbox(bbox: list[float] | tuple[float, ...] | None, page_height: float) -> str:
    if not bbox or len(bbox) != 4 or page_height <= 0:
        return "middle"
    y0 = float(bbox[1])
    y1 = float(bbox[3])
    head_limit = page_height * _HEADER_BAND_RATIO
    foot_start = page_height * _FOOTER_BAND_RATIO
    if y1 <= head_limit:
        return "header_band"
    if y0 >= foot_start:
        return "footer_band"
    return "middle"


def _canonicalize_running_line(line: str) -> str:
    normalized = (line or "").strip().lower()
    if not normalized:
        return ""

    base = normalized
    marker_match = (
        bool(re.search(r"\b(?:issn|isbn|volume|vol\.|number|no\.|journal|conference|proceedings|procedia)\b", base))
        or any(p.search(base) for p in _RUNNING_HEAD_PATTERNS)
    )
    starts_or_ends_numeric = bool(re.match(r"^\d{1,5}\b", base) or re.search(r"\b\d{1,5}$", base))
    likely_title_line = bool("/" in base or "et al" in base)
    looks_running = marker_match or starts_or_ends_numeric or likely_title_line
    if looks_running and re.fullmatch(r"\d{1,5}", normalized):
        return "<n>"

    normalized = normalized.replace("–", "-").replace("—", "-")
    if looks_running:
        normalized = re.sub(r"\bissn\s*[:#]?\s*[\d\-xX]+\b", "issn <id>", normalized, flags=re.IGNORECASE)
        normalized = re.sub(r"\bisbn\s*[:#]?\s*[\d\-xX]+\b", "isbn <id>", normalized, flags=re.IGNORECASE)
        normalized = re.sub(r"\bpage\s+\d+\b", "page <n>", normalized, flags=re.IGNORECASE)
        normalized = re.sub(r"\bpp?\.?\s*\d+(?:\s*-\s*\d+)?\b", "pp <range>", normalized, flags=re.IGNORECASE)
        normalized = re.sub(r"\bvolume\s+\d+\b", "volume <n>", normalized, flags=re.IGNORECASE)
        normalized = re.sub(r"\bvol\.?\s*\d+\b", "vol <n>", normalized, flags=re.IGNORECASE)
        normalized = re.sub(r"\bnumber\s+\d+\b", "number <n>", normalized, flags=re.IGNORECASE)
        normalized = re.sub(r"\bno\.?\s*\d+\b", "no <n>", normalized, flags=re.IGNORECASE)
        normalized = re.sub(r"\(\d+\)", "(<n>)", normalized)
        normalized = re.sub(r"\b(?:19|20)\d{2}\b", "<year>", normalized)
        normalized = re.sub(
            r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\w*\s+\d{4}\b",
            "<month-year>",
            normalized,
            flags=re.IGNORECASE,
        )
        normalized = re.sub(r"\b\d{1,5}\s*-\s*\d{1,5}\b", "<range>", normalized)
        normalized = re.sub(r"^\s*\d{1,5}\s+", "<n> ", normalized)
        normalized = re.sub(r"\s+\d{1,5}\s*$", " <n>", normalized)

    normalized = re.sub(r"[.,;:|/\\\[\]{}]+", " ", normalized)
    if normalized.strip() not in {"<n>", "(<n>)"}:
        normalized = re.sub(r"^<n>\s+", "", normalized)
        normalized = re.sub(r"\s+<n>$", "", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    if not normalized and looks_running and re.search(r"\d", base):
        return "<n>"
    return normalized


def _token_set(text: str) -> set[str]:
    return {tok for tok in re.findall(r"[a-z0-9<>]+", text.lower()) if tok}


def _token_jaccard(left: str, right: str) -> float:
    left_tokens = _token_set(left)
    right_tokens = _token_set(right)
    if not left_tokens and not right_tokens:
        return 1.0
    if not left_tokens or not right_tokens:
        return 0.0
    union = left_tokens | right_tokens
    if not union:
        return 0.0
    return len(left_tokens & right_tokens) / len(union)


def _looks_sentence_like(lines: list[str]) -> bool:
    if not lines:
        return False
    helper_words = {
        "is", "are", "was", "were", "have", "has", "had", "be", "been", "being",
        "this", "that", "these", "those", "we", "they", "it", "our", "their",
        "can", "will", "should", "may", "might", "using", "used", "propose",
        "present", "show", "shows", "results", "method",
    }
    long_line_count = 0
    helper_hit_count = 0
    punctuated_count = 0
    for line in lines:
        words = re.findall(r"[a-zA-Z']+", line.lower())
        if len(words) >= 6:
            long_line_count += 1
        if any(w in helper_words for w in words):
            helper_hit_count += 1
        if re.search(r"[.!?,:;]", line):
            punctuated_count += 1
    total = max(1, len(lines))
    return (
        long_line_count / total >= 0.5
        and helper_hit_count / total >= 0.5
        and punctuated_count / total >= 0.2
    )


def _cluster_running_text_candidates(candidates: list[dict]) -> list[dict]:
    primary: dict[str, list[dict]] = {}
    for cand in candidates:
        key = cand["canonical"]
        primary.setdefault(key, []).append(cand)

    clusters = [{"canonical": key, "candidates": group} for key, group in primary.items() if key]
    clusters.sort(key=lambda c: len(c["candidates"]), reverse=True)

    merged: list[dict] = []
    for cluster in clusters:
        merged_target = None
        for existing in merged:
            ratio = SequenceMatcher(None, cluster["canonical"], existing["canonical"]).ratio()
            jaccard = _token_jaccard(cluster["canonical"], existing["canonical"])
            if ratio >= 0.86 and jaccard >= 0.7:
                merged_target = existing
                break
        if merged_target is None:
            merged.append(cluster)
            continue
        merged_target["candidates"].extend(cluster["candidates"])
        if len(cluster["canonical"]) > len(merged_target["canonical"]):
            merged_target["canonical"] = cluster["canonical"]

    return merged


def _score_running_cluster(cluster: dict, page_count: int) -> dict:
    members = cluster.get("candidates", [])
    page_hits = {m["page_index"] for m in members}
    repeat_threshold = max(2, ceil(page_count * 0.30))
    zone_counts: Counter = Counter(m.get("zone", "middle") for m in members)
    zone_ratio = (zone_counts["header_band"] + zone_counts["footer_band"]) / max(1, len(members))
    sample_lines = [m["raw_line"] for m in members]
    mean_len = statistics.mean([len(line) for line in sample_lines] or [0.0])

    tokens = [tok for line in sample_lines for tok in re.findall(r"[a-z0-9<>]+", line.lower()) if tok not in {"<n>", "<year>", "<range>", "<id>"}]
    lexical_diversity = len(set(tokens)) / max(1, len(tokens))

    joined = " ".join(sample_lines).lower()
    has_marker = (
        bool(re.search(r"\b(?:issn|isbn|volume|vol\.|number|no\.|proceedings|procedia|journal|conference)\b", joined))
        or any(any(p.search(line) for p in _RUNNING_HEAD_PATTERNS) for line in sample_lines)
    )
    page_counter_like = all(
        bool(re.match(r"^\d{1,5}$", line.strip()))
        or bool(re.match(r"^(?:page\s+)?<n>$", _canonicalize_running_line(line)))
        for line in sample_lines
    )
    sentence_like = _looks_sentence_like(sample_lines)

    score = 0.0
    if len(page_hits) >= repeat_threshold:
        score += 0.40
    if zone_ratio >= 0.70:
        score += 0.25
    if has_marker:
        score += 0.20
    if page_counter_like and zone_ratio >= 0.90:
        score += 0.20
    if 8 <= mean_len <= 140 and lexical_diversity <= 0.72:
        score += 0.15

    return {
        "score": round(min(1.0, score), 3),
        "page_hits": len(page_hits),
        "zone_ratio": round(zone_ratio, 3),
        "has_marker": has_marker,
        "page_counter_like": page_counter_like,
        "mean_len": round(mean_len, 1),
        "lexical_diversity": round(lexical_diversity, 3),
        "sentence_like": sentence_like,
    }


def _remove_repeated_running_text(
    segments: list[dict],
    page_count: int,
    return_debug: bool = False,
) -> list[dict] | tuple[list[dict], dict]:
    empty_debug = {
        "clusters_detected": 0,
        "clusters_removed": 0,
        "lines_removed_total": 0,
        "removed_clusters": [],
    }
    if page_count < 3:
        return (segments, empty_debug) if return_debug else segments

    candidates: list[dict] = []
    for seg in segments:
        text = seg.get("text", "")
        if not text:
            continue
        page_index = int(seg.get("source_index", 0) or 0)
        metadata = seg.get("metadata", {})
        bbox = metadata.get("bbox") or []
        page_height = float(metadata.get("page_height", 0.0) or 0.0)
        zone = _zone_tag_from_bbox(bbox, page_height) if page_height > 0 else "middle"

        for line_index, raw in enumerate(text.splitlines()):
            raw_line = raw.strip()
            if not raw_line:
                continue
            candidates.append({
                "segment_id": seg.get("segment_id"),
                "line_index": line_index,
                "page_index": page_index,
                "raw_line": raw_line,
                "canonical": _canonicalize_running_line(raw_line),
                "bbox": bbox,
                "zone": zone,
            })

    clusters = _cluster_running_text_candidates(candidates)
    removed_line_keys: set[tuple[str, int]] = set()
    removed_clusters: list[dict] = []

    for cluster in clusters:
        metrics = _score_running_cluster(cluster, page_count)
        if metrics["score"] < 0.70 or metrics["page_hits"] < 2:
            continue
        if metrics["sentence_like"] and metrics["zone_ratio"] < 0.70:
            continue

        allowed_members = []
        for member in cluster["candidates"]:
            if member["zone"] == "middle" and metrics["score"] < 0.90:
                continue
            allowed_members.append(member)

        if not allowed_members:
            continue

        for member in allowed_members:
            removed_line_keys.add((str(member["segment_id"]), int(member["line_index"])))

        sample = sorted({m["raw_line"] for m in allowed_members}, key=len)[0]
        removed_clusters.append({
            "sample": sample[:160],
            "page_coverage": metrics["page_hits"],
            "confidence": metrics["score"],
            "zone_ratio": metrics["zone_ratio"],
            "removed_members": len(allowed_members),
        })

    if not removed_line_keys:
        return (segments, empty_debug) if return_debug else segments

    cleaned: list[dict] = []
    lines_removed_total = 0
    for seg in segments:
        kept_lines: list[str] = []
        seg_id = str(seg.get("segment_id"))
        for idx, line in enumerate(seg.get("text", "").splitlines()):
            if (seg_id, idx) in removed_line_keys:
                lines_removed_total += 1
                continue
            kept_lines.append(line)
        text = clean_extracted_text("\n".join(kept_lines))
        if not text:
            continue
        new_seg = {**seg, "text": text, "metadata": {**seg.get("metadata", {}), "char_count": len(text)}}
        cleaned.append(new_seg)

    debug = {
        "clusters_detected": len(clusters),
        "clusters_removed": len(removed_clusters),
        "lines_removed_total": lines_removed_total,
        "removed_clusters": sorted(removed_clusters, key=lambda c: c["confidence"], reverse=True)[:5],
    }
    return (cleaned, debug) if return_debug else cleaned


def _merge_cross_page_paragraphs(segments: list[dict]) -> list[dict]:
    merged: list[dict] = []
    i = 0

    while i < len(segments):
        current = segments[i]
        if i + 1 >= len(segments):
            merged.append(current)
            break

        nxt = segments[i + 1]
        same_flow = nxt["source_index"] == current["source_index"] + 1 and nxt.get("paragraph_index") == 1
        if not same_flow:
            merged.append(current)
            i += 1
            continue

        current_text = current.get("text", "").strip()
        next_text = nxt.get("text", "").strip()

        trailing_incomplete = bool(current_text) and not re.search(r'[.!?;:)\]"\']\s*$', current_text)
        next_starts_continuation = bool(re.match(r'^[a-z(\["\']', next_text))

        if trailing_incomplete and next_starts_continuation:
            joined = current_text[:-1] + next_text if current_text.endswith("-") else current_text + " " + next_text
            merged_seg = {
                **current,
                "text": joined,
                "metadata": {
                    **current.get("metadata", {}),
                    "char_count": len(joined),
                    "merged_with": nxt.get("segment_id"),
                },
            }
            merged.append(merged_seg)
            i += 2
            continue

        merged.append(current)
        i += 1

    return merged


def _dependency_error(package_name: str) -> dict:
    return {
        "code": "missing_dependency",
        "message": f"Required parser dependency '{package_name}' is not installed.",
        "details": {"package": package_name},
    }


def parse_pdf(file_path) -> dict:
    try:
        import fitz
    except Exception:
        return {"segments": [], "metadata": {}, "errors": [_dependency_error("PyMuPDF")]}

    file_path = Path(file_path)
    metadata = {
        "source_path": str(file_path),
        "page_count": 0,
        "parser": "pdf_pipeline_v1",
        "pipeline": [
            "page_classifier",
            "layout_detector",
            "text_extractor",
            "order_reconstructor",
            "structure_tagger",
        ],
        "warnings": [],
    }

    classifier = PageClassifier()
    layout_detector = LayoutDetector()
    extractor = TextExtractor()
    orderer = ReadingOrderReconstructor()
    tagger = StructureTagger()

    document = None
    all_segments: list[dict] = []
    all_headers: Counter = Counter()
    all_footers: Counter = Counter()

    try:
        document = fitz.open(file_path)
        metadata["page_count"] = document.page_count

        for page_index, page in enumerate(document, start=1):
            classification = classifier.classify(page)
            blocks, warnings = extractor.extract(page, classification)
            metadata["warnings"].extend([f"page_{page_index}:{w}" for w in warnings])

            page_width = float(page.rect.width)
            page_height = float(page.rect.height)

            layouts = layout_detector.detect(blocks, page_width)
            ordered_blocks = orderer.order(blocks, layouts)

            table_segments, table_bboxes, table_warnings = _extract_tables(page, page_index)
            metadata["warnings"].extend([f"page_{page_index}:{w}" for w in table_warnings])

            paragraph_segments = tagger.build_segments(ordered_blocks, page_index, table_bboxes)

            headers, footers = _collect_header_footer_candidates(paragraph_segments, page_height)
            for line in headers:
                all_headers[line] += 1
            for line in footers:
                all_footers[line] += 1

            image_segments = _extract_images(page, document, page_index)

            for seg in paragraph_segments:
                seg["metadata"]["page_mode"] = classification.mode
                seg["metadata"]["classification_confidence"] = classification.confidence
                seg["metadata"]["page_height"] = page_height

            all_segments.extend(paragraph_segments)
            all_segments.extend(table_segments)
            all_segments.extend(image_segments)

        paragraphs = [s for s in all_segments if s["source_type"] == "paragraph"]
        others = [s for s in all_segments if s["source_type"] != "paragraph"]

        repeated_headers = {
            line for line, cnt in all_headers.items()
            if cnt >= max(2, int(metadata["page_count"] * 0.35))
        }
        repeated_footers = {
            line for line, cnt in all_footers.items()
            if cnt >= max(2, int(metadata["page_count"] * 0.35))
        }
        repeated_running = repeated_headers | repeated_footers

        if repeated_running:
            cleaned: list[dict] = []
            for seg in paragraphs:
                if seg["text"].strip() in repeated_running:
                    continue
                cleaned.append(seg)
            paragraphs = cleaned

        paragraphs, running_text_debug = _remove_repeated_running_text(
            paragraphs,
            metadata["page_count"],
            return_debug=True,
        )
        metadata["running_text_debug"] = running_text_debug
        paragraphs.sort(key=lambda s: (s["source_index"], s.get("paragraph_index") or 0))
        paragraphs = _merge_cross_page_paragraphs(paragraphs)

        final_segments = paragraphs + others
        final_segments.sort(
            key=lambda s: (
                s.get("source_index", 0),
                0 if s.get("source_type") == "paragraph" else 1,
                s.get("paragraph_index") or 0,
                s.get("segment_id", ""),
            )
        )

        metadata["warnings"] = sorted(set(metadata["warnings"]))
        return {"segments": final_segments, "metadata": metadata, "errors": []}

    except Exception as exc:
        return {
            "segments": [],
            "metadata": metadata,
            "errors": [{
                "code": "pdf_parse_error",
                "message": "Unable to parse PDF file.",
                "details": {
                    "exception": str(exc),
                    "source_path": str(file_path),
                },
            }],
        }
    finally:
        if document is not None:
            document.close()
