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

_DOI_PATTERN = re.compile(r"\b10\.\d{4,9}/[-._;()/:A-Za-z0-9]+\b", re.IGNORECASE)
_ISBN_PATTERN = re.compile(
    r"\bISBN(?:-1[03])?:?\s*((?:97[89][-\s]?)?\d(?:[-\s]?\d){8,16}[\dXx])\b",
    re.IGNORECASE,
)
_ISSN_PATTERN = re.compile(
    r"\bISSN(?:\s*\((?:print|online)\))?\s*[:#]?\s*([0-9]{4}-?[0-9]{3}[0-9Xx])\b",
    re.IGNORECASE,
)

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
    is_italic: bool
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
            is_italic = False
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
                    if flags & 2:
                        is_italic = True

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
                    is_italic=is_italic,
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
                    is_italic=False,
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

        # Academic numbering patterns:
        # 1 Introduction, 1. Introduction, 1.1 Motivation, 1.1.1 Details
        if re.match(r"^\d+\.\d+\.\d+\b", compact):
            return True
        if re.match(r"^\d+\.\d+\b", compact):
            return True
        if re.match(r"^\d+\.\s*[A-Z]", compact):
            return True
        if re.match(r"^\d+\s+[A-Z]", compact):
            return True

        if re.match(r"^(chapter|section)", compact, re.IGNORECASE):
            return True
        if compact.isupper() and len(compact.split()) <= 12:
            return True
        if (
            (block.is_bold or block.is_italic)
            and block.font_size >= median_size * 1.04
            and not re.search(r"[.!?]$", compact)
        ):
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


def _build_running_text_fingerprints(
    segments: list[dict],
    page_count: int,
) -> set[str]:
    """
    Positional running-text detector.

    A canonical line fingerprint is considered running text when it repeats
    across enough pages and stays vertically consistent near header/footer zones.
    """
    if page_count < 3:
        return set()

    min_pages = max(2, ceil(page_count * 0.25))
    occurrences: dict[str, list[tuple[int, float, str]]] = {}

    for seg in segments:
        text = seg.get("text", "")
        if not text:
            continue
        page_index = int(seg.get("source_index", 0) or 0)
        metadata = seg.get("metadata", {})
        bbox = metadata.get("bbox") or []
        page_height = float(metadata.get("page_height", 0.0) or 0.0)

        stripped_lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        if not stripped_lines:
            continue

        for line_num, line in enumerate(stripped_lines):
            if len(line) > 200:
                continue
            canonical = _canonicalize_running_line(line)
            if not canonical:
                continue

            y_fraction = -1.0
            if len(bbox) == 4 and page_height > 0:
                seg_y0 = float(bbox[1])
                seg_y1 = float(bbox[3])
                seg_h = max(1.0, seg_y1 - seg_y0)
                line_y = seg_y0 + (line_num / max(1, len(stripped_lines))) * seg_h
                y_fraction = line_y / page_height

            occurrences.setdefault(canonical, []).append((page_index, y_fraction, line))

    fingerprints: set[str] = set()
    for canonical, occ in occurrences.items():
        distinct_pages = {p for p, _, _ in occ}
        if len(distinct_pages) < min_pages:
            continue

        y_values = [y for _, y, _ in occ if y >= 0]
        if len(y_values) >= 2:
            y_std = statistics.stdev(y_values)
            if y_std > 0.06:
                continue
            y_mean = statistics.mean(y_values)
            if not (y_mean < 0.15 or y_mean > 0.85):
                continue
        elif len(y_values) == 0:
            # Unknown position everywhere is too risky in conservative mode.
            continue

        sample_lines = [line for _, _, line in occ]
        if _looks_sentence_like(sample_lines):
            continue

        fingerprints.add(canonical)

    return fingerprints


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
    protected_lines: set[str] | None = None,
    return_debug: bool = False,
) -> list[dict] | tuple[list[dict], dict]:
    empty_debug = {
        "fingerprints_found": 0,
        "lines_removed_total": 0,
        "samples": [],
    }
    if page_count < 3:
        return (segments, empty_debug) if return_debug else segments

    fingerprints = _build_running_text_fingerprints(segments, page_count)
    if not fingerprints:
        return (segments, empty_debug) if return_debug else segments

    protected_canonicals = {
        _canonicalize_running_line(line)
        for line in (protected_lines or set())
        if (line or "").strip()
    }

    # Build a quick line lookup with positional zone for conservative stripping.
    candidates: list[dict] = []
    for seg in segments:
        text = seg.get("text", "")
        if not text:
            continue
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
                "raw_line": raw_line,
                "canonical": _canonicalize_running_line(raw_line),
                "zone": zone,
                "page_index": int(seg.get("source_index", 0) or 0),
            })

    protected_first_occurrence_keys: set[tuple[str, int]] = set()
    if protected_canonicals:
        for canonical in protected_canonicals:
            matching = [c for c in candidates if c["canonical"] == canonical]
            if not matching:
                continue
            matching.sort(key=lambda c: (c["page_index"], c["line_index"]))
            first = matching[0]
            protected_first_occurrence_keys.add((str(first["segment_id"]), int(first["line_index"])))

    removed_line_keys: set[tuple[str, int]] = set()
    for cand in candidates:
        if cand["canonical"] not in fingerprints:
            continue
        if cand["canonical"] in protected_canonicals and cand["page_index"] <= 1:
            continue
        if cand["zone"] == "middle":
            continue
        removed_line_keys.add((str(cand["segment_id"]), int(cand["line_index"])))

    removed_line_keys -= protected_first_occurrence_keys

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
        "fingerprints_found": len(fingerprints),
        "lines_removed_total": lines_removed_total,
        "samples": sorted(fingerprints, key=len)[:8],
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


def _extract_first_page_profile(paragraphs: list[dict]) -> dict:
    page_one = [
        seg for seg in paragraphs
        if int(seg.get("source_index", 0) or 0) == 1
    ]
    page_one.sort(key=lambda seg: int(seg.get("paragraph_index", 0) or 0))
    if not page_one:
        return {"title": "", "authors": "", "doi": "", "isbn": "", "issn": ""}

    text_chunks = [str(seg.get("text", "")).strip() for seg in page_one if str(seg.get("text", "")).strip()]
    whole_text = "\n".join(text_chunks)

    doi_match = _DOI_PATTERN.search(whole_text)
    isbn_match = _ISBN_PATTERN.search(whole_text)
    issn_match = _ISSN_PATTERN.search(whole_text)
    doi = doi_match.group(0).strip() if doi_match else ""
    isbn = isbn_match.group(1).strip() if isbn_match else ""
    issn = issn_match.group(1).strip() if issn_match else ""

    # Score early-page candidates to avoid misreading running headers / abstract lines as title.
    title = ""
    title_candidates: list[tuple[float, str]] = []
    for seg in page_one[:14]:
        text = str(seg.get("text", "")).strip()
        low = text.lower()
        if not text or len(text) < 10 or len(text) > 240:
            continue

        metadata = seg.get("metadata", {}) or {}
        role = str(metadata.get("role", "")).lower()
        bbox = metadata.get("bbox") or []
        page_height = float(metadata.get("page_height", 0.0) or 0.0)
        y0 = float(bbox[1]) if len(bbox) == 4 else 0.0
        y_ratio = (y0 / page_height) if page_height > 0 else 0.0

        score = 0.0
        if role == "heading":
            score += 2.2
        if y_ratio <= 0.35:
            score += 1.8
        if 18 <= len(text) <= 180:
            score += 1.0
        if "\n" not in text:
            score += 0.6

        # Penalize obvious non-title lines.
        if low.startswith(("abstract", "keywords", "introduction", "doi", "isbn", "issn")):
            score -= 4.0
        if _DOI_PATTERN.search(text) or _ISBN_PATTERN.search(text) or _ISSN_PATTERN.search(text):
            score -= 4.0
        if "http://" in low or "https://" in low:
            score -= 2.0
        if len(re.findall(r"\d", text)) >= 8:
            score -= 2.0
        if re.search(r"\b(university|department|faculty|journal|proceedings|conference)\b", low):
            score -= 1.5

        title_candidates.append((score, text))

    if title_candidates:
        title_candidates.sort(key=lambda item: item[0], reverse=True)
        if title_candidates[0][0] >= 1.0:
            title = title_candidates[0][1]

    authors = ""
    if title:
        title_index = next(
            (idx for idx, seg in enumerate(page_one) if str(seg.get("text", "")).strip() == title),
            -1,
        )
        candidate_window = page_one[title_index + 1:title_index + 6] if title_index >= 0 else page_one[:6]
    else:
        candidate_window = page_one[:6]

    for seg in candidate_window:
        text = str(seg.get("text", "")).strip()
        if not text:
            continue
        low = text.lower()
        if low.startswith(("abstract", "keywords", "doi", "isbn")):
            continue
        if len(text) > 180:
            continue
        if "@" in text or "university" in low or "department" in low:
            continue
        tokens = re.findall(r"[A-Za-z][A-Za-z\.\-']+", text)
        if len(tokens) < 2:
            continue
        if "," in text or " and " in low or re.search(r"\b[A-Z]\.\s*[A-Z][a-z]+", text):
            authors = text
            break

    return {
        "title": title,
        "authors": authors,
        "doi": doi,
        "isbn": isbn,
        "issn": issn,
    }


def _looks_like_publication_header(text: str) -> bool:
    low = text.lower()
    return bool(
        "sciencedirect" in low
        or "procedia" in low
        or "computer science" in low
        or "international conference" in low
        or re.search(r"\bvol(?:ume)?\.?\s*\d+\b", low)
        or re.search(r"\(\d{4}\)\s*\d{2,5}\s*[–-]\s*\d{2,5}", text)
    )


def _extract_first_page_profile_from_blocks(
    ordered_blocks: list[TextBlock],
    page_height: float,
) -> dict:
    if not ordered_blocks:
        return {"title": "", "authors": "", "doi": "", "isbn": "", "issn": ""}

    text_all = "\n".join((b.text or "").strip() for b in ordered_blocks if (b.text or "").strip())
    doi_match = _DOI_PATTERN.search(text_all)
    isbn_match = _ISBN_PATTERN.search(text_all)
    issn_match = _ISSN_PATTERN.search(text_all)
    doi = doi_match.group(0).strip() if doi_match else ""
    isbn = isbn_match.group(1).strip() if isbn_match else ""
    issn = issn_match.group(1).strip() if issn_match else ""

    body_font_sizes = [b.font_size for b in ordered_blocks if b.font_size > 0]
    body_median = statistics.median(body_font_sizes) if body_font_sizes else 11.0

    title_candidates: list[tuple[float, int, TextBlock]] = []
    for idx, block in enumerate(ordered_blocks):
        text = (block.text or "").strip()
        if not text:
            continue
        low = text.lower()
        y_ratio = (block.y0 / page_height) if page_height > 0 else 0.0

        if y_ratio > 0.60:
            continue
        if len(text) < 12 or len(text) > 260:
            continue
        if low.startswith(("abstract", "keywords", "introduction", "doi", "isbn", "issn")):
            continue
        if _DOI_PATTERN.search(text) or _ISBN_PATTERN.search(text) or _ISSN_PATTERN.search(text):
            continue
        if "http://" in low or "https://" in low:
            continue
        if _looks_like_publication_header(text):
            continue

        score = 0.0
        score += min(5.0, max(0.0, block.font_size - body_median) * 0.9)
        if block.is_bold:
            score += 1.2
        if y_ratio < 0.35:
            score += 1.0
        if 18 <= len(text) <= 180:
            score += 0.8
        if "\n" not in text:
            score += 0.3

        title_candidates.append((score, idx, block))

    title = ""
    title_end_y = 0.0
    title_end_index = -1
    if title_candidates:
        title_candidates.sort(key=lambda item: item[0], reverse=True)
        _, seed_idx, seed_block = title_candidates[0]

        title_parts = [seed_block.text.strip()]
        title_end_y = seed_block.y1
        title_end_index = seed_idx
        seed_size = max(1.0, seed_block.font_size or body_median)

        # Merge adjacent title line blocks when they look like continuation.
        for j in range(seed_idx + 1, min(seed_idx + 5, len(ordered_blocks))):
            nxt = ordered_blocks[j]
            nxt_text = (nxt.text or "").strip()
            if not nxt_text:
                continue
            if _looks_like_publication_header(nxt_text):
                continue
            if re.match(r"^(abstract|keywords)\b", nxt_text, re.IGNORECASE):
                break

            gap = nxt.y0 - title_end_y
            size_close = abs((nxt.font_size or seed_size) - seed_size) <= max(1.8, seed_size * 0.22)
            if gap <= max(12.0, seed_size * 1.3) and size_close and len(nxt_text) <= 180:
                title_parts.append(nxt_text)
                title_end_y = nxt.y1
                title_end_index = j
            else:
                break

        title = " ".join(part.strip() for part in title_parts if part.strip())
        title = re.sub(r"\s+", " ", title).strip()

    authors = ""
    if title_end_index >= 0:
        author_parts: list[str] = []
        for j in range(title_end_index + 1, min(title_end_index + 8, len(ordered_blocks))):
            block = ordered_blocks[j]
            text = (block.text or "").strip()
            if not text:
                continue
            low = text.lower()
            y_ratio = (block.y0 / page_height) if page_height > 0 else 0.0
            if y_ratio > 0.82:
                break
            if re.match(r"^(abstract|keywords)\b", low):
                break
            if _DOI_PATTERN.search(text) or _ISBN_PATTERN.search(text) or _ISSN_PATTERN.search(text):
                continue
            if _looks_like_publication_header(text):
                continue

            # Keep author/affiliation lines close to title and before abstract.
            if len(text) <= 360:
                author_parts.append(text)

            if len(author_parts) >= 3:
                break

        authors = " ".join(author_parts).strip()
        authors = re.sub(r"\s+", " ", authors)

    return {
        "title": title,
        "authors": authors,
        "doi": doi,
        "isbn": isbn,
        "issn": issn,
    }


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
    first_page_ordered_blocks: list[TextBlock] = []
    first_page_height = 0.0

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
            if page_index == 1:
                first_page_ordered_blocks = ordered_blocks[:]
                first_page_height = page_height

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

        first_page_profile = _extract_first_page_profile_from_blocks(
            first_page_ordered_blocks,
            first_page_height,
        )
        # Fallback to paragraph-based extraction if block-based profile is incomplete.
        if not first_page_profile.get("title"):
            fallback_profile = _extract_first_page_profile(paragraphs)
            for key, value in fallback_profile.items():
                if not first_page_profile.get(key) and value:
                    first_page_profile[key] = value
        metadata["paper_profile"] = first_page_profile
        protected_lines = {
            value.strip()
            for value in first_page_profile.values()
            if isinstance(value, str) and value.strip()
        }

        if repeated_running:
            cleaned: list[dict] = []
            for seg in paragraphs:
                seg_text = seg["text"].strip()
                if (
                    seg_text in repeated_running
                    and int(seg.get("source_index", 0) or 0) > 1
                    and seg_text not in protected_lines
                ):
                    continue
                cleaned.append(seg)
            paragraphs = cleaned

        paragraphs, running_text_debug = _remove_repeated_running_text(
            paragraphs,
            metadata["page_count"],
            protected_lines=protected_lines,
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
