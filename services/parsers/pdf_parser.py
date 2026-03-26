from pathlib import Path
from collections import Counter
import math
import re
import unicodedata

from services.parsers.utils import (
    normalize_extracted_line,
    clean_extracted_text,
    encode_image_bytes,
    table_to_markdown,
    looks_like_formula,
)


# ===========================================================================
# 1. CONSTANTS & PATTERNS
# ===========================================================================

_HEADER_ZONE_RATIO = 0.08
_FOOTER_ZONE_RATIO = 0.92

_MIN_IMAGE_AREA = 2_000

_MAX_COLUMN_COUNT = 3
_COLUMN_GAP_RATIO = 0.08
_BAND_MERGE_TOLERANCE = 14.0
_Y_SLICE_MIN_HEIGHT = 8.0
_VERTICAL_OVERLAP_RATIO = 0.15

_PARAGRAPH_VERTICAL_GAP_MULTIPLIER = 1.3
_PARAGRAPH_INDENT_TOLERANCE = 18.0
_PARAGRAPH_FONT_RATIO_TOLERANCE = 0.25
_MAX_PARAGRAPH_SEGMENT_CHARS = 1_500

_BULLET_CHARS = {"-", "*", "•", "‣", "◦", "⁃", "∙", "·", "▪", "◾", "◼", "●", "○", "♦"}

_FITZ_EXT_TO_MIME = {
    "png": "image/png",
    "jpeg": "image/jpeg",
    "jpg": "image/jpeg",
    "jpx": "image/jp2",
    "j2k": "image/jp2",
    "jxr": "image/jxr",
    "bmp": "image/bmp",
    "gif": "image/gif",
    "tiff": "image/tiff",
}

_JUNK_LINE_PATTERNS = [
    re.compile(r"^Creative\s+Commons", re.IGNORECASE),
    re.compile(r"NonCommercial", re.IGNORECASE),
    re.compile(r"^\d+\.\d+\s+International\s*$", re.IGNORECASE),
    re.compile(r"^(CC\s+BY|CC\s+BY-NC|CC\s+BY-SA)", re.IGNORECASE),
    re.compile(r"^\d+\s*$"),
    re.compile(r"^Page\s+\d+\s+of\s+\d+$", re.IGNORECASE),
    re.compile(r"^https?://\S+$"),
    re.compile(r"^doi:\s*\S+$", re.IGNORECASE),
    re.compile(r"^\d+\s+(International|Journal|Conference|Proceedings)", re.IGNORECASE),
]

_HEADER_FOOTER_PATTERNS = [
    re.compile(r"^[A-Z]{2,6}\s*\d{3,5}", re.IGNORECASE),
    re.compile(r"^Page\s+\d+", re.IGNORECASE),
    re.compile(r"\bCONFIDENTIAL\b", re.IGNORECASE),
    re.compile(r"^(CHAPTER|SECTION)\s+\d+", re.IGNORECASE),
    re.compile(r"^\d+\s*$"),
    re.compile(r"^[-–—]{3,}\s*$"),
]


# ===========================================================================
# 2. SMALL HELPERS
# ===========================================================================

def _dependency_error(package_name: str) -> dict:
    return {
        "code": "missing_dependency",
        "message": f"Required parser dependency '{package_name}' is not installed.",
        "details": {"package": package_name},
    }


def _segment_sort_key(segment: dict) -> tuple:
    return (
        segment.get("source_index", 0),
        segment.get("block_index", 0) if segment.get("block_index") is not None else 0,
        segment.get("paragraph_index", 0) if segment.get("paragraph_index") is not None else 0,
        segment.get("segment_id", ""),
    )


def _is_junk_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    return any(p.match(stripped) for p in _JUNK_LINE_PATTERNS)


def _looks_like_header_footer(text: str) -> bool:
    stripped = text.strip()
    if not stripped:
        return False
    return any(p.search(stripped) for p in _HEADER_FOOTER_PATTERNS)


def _median(values: list[float], default: float = 0.0) -> float:
    if not values:
        return default
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return float(ordered[mid])
    return float((ordered[mid - 1] + ordered[mid]) / 2.0)


def _merge_close_numbers(values: list[float], tolerance: float) -> list[float]:
    if not values:
        return []

    values = sorted(float(v) for v in values)
    groups: list[list[float]] = [[values[0]]]

    for value in values[1:]:
        if abs(value - groups[-1][-1]) <= tolerance:
            groups[-1].append(value)
        else:
            groups.append([value])

    return [sum(group) / len(group) for group in groups]


def _overlap_length(a0: float, a1: float, b0: float, b1: float) -> float:
    return max(0.0, min(a1, b1) - max(a0, b0))


def _vertical_overlap_ratio(block_a: dict, block_b: dict) -> float:
    overlap = _overlap_length(block_a["y0"], block_a["y1"], block_b["y0"], block_b["y1"])
    min_height = min(block_a["height"], block_b["height"])
    if min_height <= 0:
        return 0.0
    return overlap / min_height


def _same_column_hint(a: dict, b: dict, page_width: float) -> bool:
    center_tol = max(12.0, page_width * 0.03)
    x0_tol = max(10.0, page_width * 0.025)

    return (
        abs(a["center_x"] - b["center_x"]) <= center_tol
        or abs(a["x0"] - b["x0"]) <= x0_tol
    )


def _is_heading_like(text: str, block: dict | None = None) -> bool:
    stripped = " ".join(text.split())
    if not stripped:
        return False
    if len(stripped) <= 120 and stripped.isupper():
        return True
    if len(stripped) <= 120 and re.match(
        r"^(chapter|section|\d+(\.\d+)*\s+)", stripped, re.IGNORECASE
    ):
        return True
    # NEW: single short line that is bold and not sentence-ending punctuation
    if block and block.get("is_bold") and block.get("line_count", 1) == 1:
        if len(stripped) <= 120 and not re.search(r"[.!?;,]\s*$", stripped):
            return True
    return False


def _ends_paragraph(text: str) -> bool:
    return bool(re.search(r"[.!?;:)\]\"\']\s*$", text.strip()))


def _is_private_use_char(ch: str) -> bool:
    if not ch:
        return False
    code = ord(ch)
    return (
        0xE000 <= code <= 0xF8FF
        or 0xF0000 <= code <= 0xFFFFD
        or 0x100000 <= code <= 0x10FFFD
    )


def _strip_private_use(text: str) -> str:
    return "".join(
        " " if _is_private_use_char(ch) else ch
        for ch in text
    )


def _looks_like_bullet_char(ch: str) -> bool:
    if not ch:
        return False
    if ch in _BULLET_CHARS:
        return True
    if _is_private_use_char(ch):
        return True
    try:
        return "BULLET" in unicodedata.name(ch, "")
    except Exception:
        return False


def _split_inline_bullet_runs(text: str) -> str:
    if not text:
        return ""

    def _repl_inline(match: re.Match) -> str:
        marker = match.group(1)
        if marker in {"-", "*", "?"} or _looks_like_bullet_char(marker):
            return "\n- "
        return match.group(0)

    text = re.sub(r"(?m)^\s*([^\w\s])\s+(?=[A-Za-z0-9(])", _repl_inline, text)
    text = re.sub(r"(?<=[;:])\s*([^\w\s])\s+(?=[A-Za-z0-9(])", _repl_inline, text)
    text = re.sub(r"(?<=[;:])\s*-\s+(?=[A-Za-z0-9(])", "\n- ", text)
    return text


def _starts_list_item(text: str) -> bool:
    stripped = text.strip()
    if not stripped:
        return False

    if re.match(r"^(?:(?:\d+|[A-Za-z]|[ivxlcdm]+)[\.\)])\s+", stripped, re.IGNORECASE):
        return True

    if len(stripped) >= 2 and stripped[1].isspace():
        first = stripped[0]
        if first in {"-", "*"} or _looks_like_bullet_char(first):
            return True

    return False


def _normalize_bullet_prefix(line: str) -> str:
    if not line:
        return ""
    stripped = line.lstrip()
    if len(stripped) >= 2 and stripped[1].isspace():
        first = stripped[0]
        if first in {"-", "*"} or _looks_like_bullet_char(first):
            return "- " + stripped[2:].lstrip()
    return line


def _normalize_join_text(parts: list[str]) -> str:
    if not parts:
        return ""

    joined = parts[0].strip()
    for part in parts[1:]:
        part = part.strip()
        if not part:
            continue
        if joined.endswith("-") and part[:1].islower():
            # dehyphenate
            joined = joined[:-1] + part
        elif joined.endswith("/"):
            joined += part
        else:
            # If previous part ends a sentence and this starts capitalised,
            # preserve as separate sentence rather than joining with a space
            # that loses the boundary. Use a single space — the segment is
            # already one logical paragraph at this point.
            joined += " " + part

    return normalize_extracted_line(joined)


# ===========================================================================
# 3. BLOCK EXTRACTION
# ===========================================================================

def _build_text_blocks(page) -> list[dict]:
    """
    Extract text blocks with geometry and light typography metadata.
    One PDF block becomes one logical raw block candidate.
    """
    page_dict = page.get_text("dict")
    text_blocks: list[dict] = []

    for raw_block in page_dict.get("blocks", []):
        if raw_block.get("type") != 0:
            continue

        line_texts: list[str] = []
        font_sizes: list[float] = []
        line_dirs: list[tuple[float, float]] = []
        is_bold = False
        is_italic = False

        for line in raw_block.get("lines", []):
            spans = line.get("spans", [])
            span_text = "".join((span.get("text") or "") for span in spans).strip()
            if not span_text:
                continue

            line_texts.append(span_text)

            for span in spans:
                size = span.get("size")
                if isinstance(size, (int, float)) and size > 0:
                    font_sizes.append(float(size))
                flags = span.get("flags", 0)
                if flags & 2**4:   # bold bit
                    is_bold = True
                if flags & 2**1:   # italic bit
                    is_italic = True

            line_dir = line.get("dir")
            if (
                isinstance(line_dir, (list, tuple))
                and len(line_dir) == 2
                and all(isinstance(v, (int, float)) for v in line_dir)
            ):
                line_dirs.append((float(line_dir[0]), float(line_dir[1])))

        block_text = "\n".join(line_texts).strip()
        if not block_text:
            continue

        x0, y0, x1, y1 = raw_block.get("bbox", (0, 0, 0, 0))
        width = float(x1) - float(x0)
        height = float(y1) - float(y0)

        dir_x = _median([d[0] for d in line_dirs], default=1.0)
        dir_y = _median([d[1] for d in line_dirs], default=0.0)

        text_blocks.append({
            "x0": float(x0),
            "y0": float(y0),
            "x1": float(x1),
            "y1": float(y1),
            "width": width,
            "height": height,
            "center_x": (float(x0) + float(x1)) / 2.0,
            "center_y": (float(y0) + float(y1)) / 2.0,
            "text": block_text,
            "line_count": len(line_texts),
            "font_size": _median(font_sizes, default=0.0),
            "dir_x": dir_x,
            "dir_y": dir_y,
            "is_bold": is_bold,
            "is_italic": is_italic,
        })

    text_blocks.sort(key=lambda b: (b["y0"], b["x0"]))
    return text_blocks


# ===========================================================================
# 4. LAYOUT ANALYSIS
# ===========================================================================

def _cluster_active_columns(active_blocks: list[dict], page_width: float) -> list[dict]:
    """
    Cluster overlapping active blocks into 1..3 columns using x center positions.
    Wider gaps imply separate columns.
    """
    if not active_blocks:
        return []

    blocks = sorted(active_blocks, key=lambda b: b["x0"])
    if len(blocks) == 1:
        only = blocks[0]
        return [{"x0": only["x0"], "x1": only["x1"],
                 "center_x": only["center_x"], "members": [only]}]

    gap_threshold = max(24.0, page_width * 0.055)

    columns: list[list[dict]] = [[blocks[0]]]
    for block in blocks[1:]:
        # Gap = this block's left edge minus the rightmost x1 in current column
        current_col_x1 = max(b["x1"] for b in columns[-1])
        gap = block["x0"] - current_col_x1
        if gap >= gap_threshold and len(columns) < _MAX_COLUMN_COUNT:
            columns.append([block])
        else:
            columns[-1].append(block)

    result: list[dict] = []
    for members in columns:
        result.append({
            "x0": min(b["x0"] for b in members),
            "x1": max(b["x1"] for b in members),
            "center_x": sum(b["center_x"] for b in members) / len(members),
            "members": members,
        })

    result.sort(key=lambda c: c["center_x"])
    return result

def _column_signature(columns: list[dict]) -> tuple:
    centers = tuple(round(col["center_x"] / 10.0) * 10 for col in columns)
    return (len(columns), centers)


def _build_vertical_bands(body_blocks: list[dict], page_height: float, page_width: float) -> list[dict]:
    """
    Build bands by sweeping vertically through the page.
    Each band can be single-column, two-column or three-column.
    This allows layout changes midway down the page.
    """
    if not body_blocks:
        return []

    y_points: list[float] = []
    for block in body_blocks:
        y_points.append(block["y0"])
        y_points.append(block["y1"])

    merged_points = _merge_close_numbers(y_points, tolerance=_BAND_MERGE_TOLERANCE)
    if len(merged_points) < 2:
        return [{
            "y0": min(b["y0"] for b in body_blocks),
            "y1": max(b["y1"] for b in body_blocks),
            "columns": _cluster_active_columns(body_blocks, page_width),
        }]

    slices: list[dict] = []
    for y0, y1 in zip(merged_points, merged_points[1:]):
        if (y1 - y0) < _Y_SLICE_MIN_HEIGHT:
            continue

        y_mid = (y0 + y1) / 2.0
        active = [b for b in body_blocks if b["y0"] <= y_mid <= b["y1"]]
        if not active:
            continue

        columns = _cluster_active_columns(active, page_width)
        slices.append({
            "y0": y0,
            "y1": y1,
            "columns": columns,
            "signature": _column_signature(columns),
        })

    if not slices:
        return [{
            "y0": min(b["y0"] for b in body_blocks),
            "y1": max(b["y1"] for b in body_blocks),
            "columns": _cluster_active_columns(body_blocks, page_width),
        }]

    bands: list[dict] = [{
        "y0": slices[0]["y0"],
        "y1": slices[0]["y1"],
        "columns": slices[0]["columns"],
        "signature": slices[0]["signature"],
    }]

    for current in slices[1:]:
        prev = bands[-1]
        compatible = (
            current["signature"] == prev["signature"]
            and abs(current["y0"] - prev["y1"]) <= _BAND_MERGE_TOLERANCE
        )

        if compatible:
            prev["y1"] = current["y1"]
        else:
            bands.append({
                "y0": current["y0"],
                "y1": current["y1"],
                "columns": current["columns"],
                "signature": current["signature"],
            })

    return bands


def _band_blocks(body_blocks: list[dict], band: dict) -> list[dict]:
    band_y0, band_y1 = band["y0"], band["y1"]
    members = []
    for block in body_blocks:
        overlap = _overlap_length(block["y0"], block["y1"], band_y0, band_y1)
        if overlap <= 0:
            continue
        if overlap / max(block["height"], 1.0) >= _VERTICAL_OVERLAP_RATIO or band_y0 <= block["center_y"] <= band_y1:
            members.append(block)
    return members


def _assign_block_to_column(
    block: dict,
    columns: list[dict],
    page_width: float,
    region_y0: float = 0.0,
    region_y1: float = float("inf"),
) -> int | None:
    if not columns:
        return None

    wide_threshold = page_width * 0.72
    if block["width"] >= wide_threshold and len(columns) > 1:
        return None

    # Block must meaningfully overlap this region's vertical span.
    block_region_overlap = _overlap_length(block["y0"], block["y1"], region_y0, region_y1)
    if block_region_overlap / max(block["height"], 1.0) < 0.4:
        return None

    best_index = None
    best_distance = float("inf")

    for idx, col in enumerate(columns):
        distance = abs(block["center_x"] - col["center_x"])
        if distance < best_distance:
            best_distance = distance
            best_index = idx

    if best_index is None:
        return None

    col = columns[best_index]
    expanded_x0 = col["x0"] - max(18.0, page_width * 0.03)
    expanded_x1 = col["x1"] + max(18.0, page_width * 0.03)

    if block["x1"] < expanded_x0 or block["x0"] > expanded_x1:
        return None

    return best_index


def _bands_have_similar_columns(
    band_a: dict,
    band_b: dict,
    page_width: float,
) -> bool:
    cols_a = band_a.get("columns", [])
    cols_b = band_b.get("columns", [])

    if len(cols_a) != len(cols_b):
        return False

    if len(cols_a) <= 1:
        return True

    center_tol = max(24.0, page_width * 0.05)
    edge_tol = max(28.0, page_width * 0.06)

    for col_a, col_b in zip(cols_a, cols_b):
        if abs(col_a["center_x"] - col_b["center_x"]) > center_tol:
            return False
        if abs(col_a["x0"] - col_b["x0"]) > edge_tol:
            return False
        if abs(col_a["x1"] - col_b["x1"]) > edge_tol:
            return False

    return True


def _group_bands_into_regions(
    bands: list[dict],
    page_width: float,
) -> list[list[dict]]:
    """
    Group adjacent bands into larger reading regions.
    Important: adjacent two-column bands with similar column geometry are grouped
    together, so we can read the whole left column before the right column.
    """
    if not bands:
        return []

    regions: list[list[dict]] = [[bands[0]]]

    for band in bands[1:]:
        prev_band = regions[-1][-1]

        same_column_count = len(prev_band.get("columns", [])) == len(band.get("columns", []))
        close_vertically = abs(band["y0"] - prev_band["y1"]) <= max(_BAND_MERGE_TOLERANCE, 20.0)
        compatible = (
            same_column_count
            and close_vertically
            and _bands_have_similar_columns(prev_band, band, page_width)
        )

        if compatible:
            regions[-1].append(band)
        else:
            regions.append([band])

    return regions


def _region_blocks(body_blocks: list[dict], region_bands: list[dict]) -> list[dict]:
    """
    Collect blocks belonging to a whole grouped region.
    """
    if not region_bands:
        return []

    region_y0 = min(b["y0"] for b in region_bands)
    region_y1 = max(b["y1"] for b in region_bands)

    members = []
    for block in body_blocks:
        overlap = _overlap_length(block["y0"], block["y1"], region_y0, region_y1)
        if overlap <= 0:
            continue
        if overlap / max(block["height"], 1.0) >= _VERTICAL_OVERLAP_RATIO or region_y0 <= block["center_y"] <= region_y1:
            members.append(block)

    return members


def _detect_page_columns(body_blocks: list[dict], page_width: float) -> list[dict] | None:
    """
    Detect whether the page body has a stable two-column layout by looking
    at all blocks globally, not slice by slice.
    Returns column definitions if multi-column is detected, else None.
    """
    if not body_blocks:
        return None

    # Separate full-width blocks from column candidates
    full_width_threshold = page_width * 0.72
    candidates = [b for b in body_blocks if b["width"] < full_width_threshold]

    if not candidates:
        return None

    # Split candidates into left vs right by x0 using a gap search
    candidates_sorted = sorted(candidates, key=lambda b: b["x0"])
    
    # Find the largest x0 gap — that's the column gutter
    best_gap = 0.0
    best_split = 0
    for i in range(1, len(candidates_sorted)):
        # Gap between this block's x0 and the previous block's x1
        gap = candidates_sorted[i]["x0"] - max(b["x1"] for b in candidates_sorted[:i])
        if gap > best_gap:
            best_gap = gap
            best_split = i

    min_gap = max(20.0, page_width * 0.04)
    if best_gap < min_gap or best_split == 0:
        return None  # No clear column split

    left_blocks = candidates_sorted[:best_split]
    right_blocks = candidates_sorted[best_split:]

    # Sanity check: both groups must have meaningful content
    # and the right group must start clearly to the right of the left group
    left_x1 = max(b["x1"] for b in left_blocks)
    right_x0 = min(b["x0"] for b in right_blocks)
    if right_x0 <= left_x1:
        return None

    return [
        {
            "x0": min(b["x0"] for b in left_blocks),
            "x1": left_x1,
            "center_x": sum(b["center_x"] for b in left_blocks) / len(left_blocks),
            "members": left_blocks,
        },
        {
            "x0": right_x0,
            "x1": max(b["x1"] for b in right_blocks),
            "center_x": sum(b["center_x"] for b in right_blocks) / len(right_blocks),
            "members": right_blocks,
        },
    ]


def _order_body_blocks(body_blocks: list[dict], page_height: float, page_width: float) -> list[dict]:
    if not body_blocks:
        return []

    full_width_threshold = page_width * 0.72
    full_width_blocks = [b for b in body_blocks if b["width"] >= full_width_threshold]
    column_candidates = [b for b in body_blocks if b["width"] < full_width_threshold]

    columns = _detect_page_columns(body_blocks, page_width)

    # Single-column page: plain top-to-bottom
    if columns is None or len(columns) <= 1:
        return sorted(body_blocks, key=lambda b: (b["y0"], b["x0"]))

    # Use gutter midpoint as the column split
    gutter_mid = (columns[0]["x1"] + columns[1]["x0"]) / 2.0

    per_column: dict[int, list[dict]] = {0: [], 1: []}

    for block in column_candidates:
        if block["center_x"] < gutter_mid:
            per_column[0].append(block)
        else:
            per_column[1].append(block)

    left_blocks = sorted(per_column[0], key=lambda b: (b["y0"], b["x0"]))
    right_blocks = sorted(per_column[1], key=lambda b: (b["y0"], b["x0"]))

    # Full-width blocks: separate leading (above columns) from interstitial/trailing
    all_col_blocks = left_blocks + right_blocks
    if all_col_blocks:
        col_content_y0 = min(b["y0"] for b in all_col_blocks)
        col_content_y1 = max(b["y1"] for b in all_col_blocks)
    else:
        col_content_y0 = col_content_y1 = 0.0

    top_full = [b for b in full_width_blocks if b["y1"] <= col_content_y0 + 4.0]
    bottom_full = [b for b in full_width_blocks if b["y0"] >= col_content_y1 - 4.0]
    mid_full = [b for b in full_width_blocks if b not in top_full and b not in bottom_full]

    top_full.sort(key=lambda b: (b["y0"], b["x0"]))
    mid_full.sort(key=lambda b: (b["y0"], b["x0"]))
    bottom_full.sort(key=lambda b: (b["y0"], b["x0"]))

    ordered: list[dict] = []

    # 1. Leading full-width blocks first
    ordered.extend(top_full)

    # 2. Header-aware two-column ordering
    #
    # Normal rule:
    #   read left column fully, then right column
    #
    # Extra safeguard:
    #   if a left heading/subheading appears, also emit the immediate left
    #   continuation directly below it before switching to right column.
    #
    # This fixes cases like:
    #   1.2. State of the art
    #   [left paragraph starts below]
    #   [right paragraph starts at same vertical level]
    #
    left_index = 0
    right_hold_y = None

    while left_index < len(left_blocks):
        block = left_blocks[left_index]
        ordered.append(block)

        # If this left block looks like a heading, temporarily block right-column
        # output until we have emitted the first real left continuation beneath it.
        if _is_heading_like(block["text"]):
            heading_bottom = block["y1"]

            # Find the first non-heading left block below this heading
            probe = left_index + 1
            while probe < len(left_blocks):
                next_left = left_blocks[probe]
                if not _is_heading_like(next_left["text"]):
                    # Only treat it as the continuation if it is reasonably close
                    gap = next_left["y0"] - heading_bottom
                    if gap <= max(36.0, block["height"] * 2.0):
                        ordered.append(next_left)
                        left_index = probe
                        right_hold_y = next_left["y1"]
                    break
                else:
                    ordered.append(next_left)
                    left_index = probe
                    heading_bottom = next_left["y1"]
                probe += 1

        left_index += 1

    # 3. Right column afterwards
    #
    # If we previously held the right side because of a heading, this still keeps
    # the whole right column after the left flow, which matches academic PDF reading
    # order much better than letting the right paragraph attach to the heading.
    ordered.extend(right_blocks)

    # 4. Interstitial/trailing full-width blocks
    ordered.extend(mid_full)
    ordered.extend(bottom_full)

    # Safety: stable de-dup in case of accidental double insert
    deduped: list[dict] = []
    seen_ids: set[int] = set()
    for block in ordered:
        if id(block) in seen_ids:
            continue
        seen_ids.add(id(block))
        deduped.append(block)

    return deduped
# ===========================================================================
# 5. TEXT CLEANING + PARAGRAPH ASSEMBLY
# ===========================================================================

def _clean_block_text(raw_text: str) -> str:
    raw_text = raw_text.replace("\r\n", "\n").replace("\r", "\n")
    raw_text = re.sub(r"-\n(?=[a-z])", "", raw_text)
    raw_text = _strip_private_use(raw_text)
    raw_text = re.sub(r" {2,}", " ", raw_text)
    raw_text = _split_inline_bullet_runs(raw_text)

    cleaned_lines: list[str] = []
    for line in raw_text.split("\n"):
        stripped = _normalize_bullet_prefix(line).strip()
        if not stripped or _is_junk_line(stripped):
            continue
        # Use lightweight normalization — no paragraph inference here
        normalized = normalize_extracted_line(stripped)
        if normalized:
            cleaned_lines.append(normalized)

    return "\n".join(cleaned_lines).strip()


def _clean_individual_block(block: dict) -> dict | None:
    cleaned = _clean_block_text(block["text"])
    if not cleaned:
        return None
    return {**block, "text": cleaned}


def _should_merge_blocks(prev_block: dict, curr_block: dict, page_width: float) -> bool:
    if not prev_block or not curr_block:
        return False

    # Heading: attach only the immediately following block if it's close
    if _is_heading_like(prev_block["text"], prev_block):
        same_column = _same_column_hint(prev_block, curr_block, page_width)
        gap = curr_block["y0"] - prev_block["y1"]
        return same_column and gap <= max(28.0, prev_block["height"] * 1.6)

    prev_starts_list = _starts_list_item(prev_block["text"])
    curr_starts_list = _starts_list_item(curr_block["text"])

    # Never merge a non-list block into a list starter
    if curr_starts_list and not prev_starts_list:
        return False

    if not _same_column_hint(prev_block, curr_block, page_width):
        return False

    gap = curr_block["y0"] - prev_block["y1"]
    typical_height = max(min(prev_block["height"], curr_block["height"]), 1.0)
    max_gap = typical_height * _PARAGRAPH_VERTICAL_GAP_MULTIPLIER

    if gap > max_gap:
        return False

    # NEW: if the gap is notably larger than a normal line advance, treat as
    # paragraph break regardless of other signals
    line_advance = prev_block["font_size"] if prev_block.get("font_size", 0) > 0 else typical_height
    if gap > line_advance * 1.4:
        return False

    font_a = prev_block.get("font_size", 0.0)
    font_b = curr_block.get("font_size", 0.0)
    if font_a > 0 and font_b > 0:
        ratio = abs(font_a - font_b) / max(font_a, font_b)
        if ratio > _PARAGRAPH_FONT_RATIO_TOLERANCE:
            return False

    # NEW: sentence-ending line followed by a capitalised new line = new paragraph
    prev_text = prev_block["text"].strip()
    curr_text = curr_block["text"].strip()
    indent_change = abs(curr_block["x0"] - prev_block["x0"])
    
    if (
        _ends_paragraph(prev_text)
        and re.match(r"^[A-Z(\"'\[]", curr_text)
        and indent_change > _PARAGRAPH_INDENT_TOLERANCE
    ):
        return False

    if prev_text.endswith(":") and not curr_text[:1].islower():
        return False

    return True

def _attach_orphan_headings_to_next_segment(segments: list[dict]) -> list[dict]:
    if not segments:
        return segments

    merged: list[dict] = []
    skip_next = False

    for i, seg in enumerate(segments):
        if skip_next:
            skip_next = False
            continue

        if i + 1 >= len(segments):
            merged.append(seg)
            continue

        next_seg = segments[i + 1]

        same_page = seg["source_index"] == next_seg["source_index"]
        short_heading = _is_heading_like(seg["text"]) and len(seg["text"].strip()) <= 120
        next_is_normal_paragraph = (
            next_seg["source_type"] == "paragraph"
            and not _starts_list_item(next_seg["text"])
        )

        seg_bbox = seg.get("metadata", {}).get("bbox", [])
        next_bbox = next_seg.get("metadata", {}).get("bbox", [])

        same_column = False
        vertically_close = False

        if len(seg_bbox) == 4 and len(next_bbox) == 4:
            seg_x0, seg_y0, seg_x1, seg_y1 = seg_bbox
            nxt_x0, nxt_y0, nxt_x1, nxt_y1 = next_bbox

            same_column = abs(seg_x0 - nxt_x0) <= 40.0
            vertically_close = 0.0 <= (nxt_y0 - seg_y1) <= 48.0

        if same_page and short_heading and next_is_normal_paragraph and same_column and vertically_close:
            joined = f'{seg["text"].strip()}\n\n{next_seg["text"].strip()}'
            merged.append({
                **next_seg,
                "text": joined,
                "metadata": {
                    **next_seg["metadata"],
                    "char_count": len(joined),
                    "attached_heading": seg["text"].strip(),
                },
            })
            skip_next = True
        else:
            merged.append(seg)

    return merged

def _build_paragraph_segments(
    ordered_blocks: list[dict],
    page_index: int,
    page_width: float,
) -> list[dict]:
    """
    Merge neighboring ordered blocks into paragraph segments using geometry,
    not only blank lines.
    """
    cleaned_blocks = []
    for block in ordered_blocks:
        cleaned = _clean_individual_block(block)
        if cleaned:
            cleaned_blocks.append(cleaned)

    if not cleaned_blocks:
        return []

    paragraphs: list[list[dict]] = [[cleaned_blocks[0]]]

    for block in cleaned_blocks[1:]:
        prev_block = paragraphs[-1][-1]
        if _should_merge_blocks(prev_block, block, page_width):
            paragraphs[-1].append(block)
        else:
            paragraphs.append([block])

    segments: list[dict] = []

    def _build_paragraph_parts(para_blocks: list[dict]) -> list[tuple[str, str]]:
        lines: list[str] = []
        for block in para_blocks:
            for line in block["text"].split("\n"):
                stripped = line.strip()
                if stripped:
                    lines.append(stripped)

        if not lines:
            return []

        parts: list[tuple[str, str]] = []
        index = 0
        while index < len(lines):
            if _starts_list_item(lines[index]):
                list_lines: list[str] = []
                while index < len(lines) and _starts_list_item(lines[index]):
                    list_lines.append(lines[index])
                    index += 1

                chunk: list[str] = []
                chunk_len = 0
                for item in list_lines:
                    item_len = len(item)
                    add_len = item_len + (1 if chunk else 0)
                    if chunk and (chunk_len + add_len) > _MAX_PARAGRAPH_SEGMENT_CHARS:
                        parts.append(("list", "\n".join(chunk)))
                        chunk = [item]
                        chunk_len = item_len
                    else:
                        chunk.append(item)
                        chunk_len += add_len
                if chunk:
                    parts.append(("list", "\n".join(chunk)))
            else:
                prose_lines: list[str] = []
                while index < len(lines) and not _starts_list_item(lines[index]):
                    prose_lines.append(lines[index])
                    index += 1
                prose_text = _normalize_join_text(prose_lines).strip()
                if prose_text:
                    parts.append(("prose", prose_text))

        return parts

    def _pack_parts_into_segments(parts: list[tuple[str, str]]) -> list[str]:
        if not parts:
            return []

        packed: list[str] = []
        current_parts: list[str] = []
        current_len = 0
        current_kind = ""

        for kind, part in parts:
            if current_parts and kind != current_kind:
                packed.append("\n".join(current_parts).strip())
                current_parts = []
                current_len = 0

            add_len = len(part) + (1 if current_parts else 0)
            if current_parts and (current_len + add_len) > _MAX_PARAGRAPH_SEGMENT_CHARS:
                packed.append("\n".join(current_parts).strip())
                current_parts = [part]
                current_len = len(part)
            else:
                current_parts.append(part)
                current_len += add_len
            current_kind = kind

        if current_parts:
            packed.append("\n".join(current_parts).strip())

        return [p for p in packed if p]

    for para_index, para_blocks in enumerate(paragraphs, start=1):
        paragraph_parts = _build_paragraph_parts(para_blocks)
        segment_texts = _pack_parts_into_segments(paragraph_parts)
        if not segment_texts:
            continue

        bbox = [
            min(b["x0"] for b in para_blocks),
            min(b["y0"] for b in para_blocks),
            max(b["x1"] for b in para_blocks),
            max(b["y1"] for b in para_blocks),
        ]

        for split_index, para_text in enumerate(segment_texts, start=1):
            suffix = f"-part-{split_index}" if len(segment_texts) > 1 else ""
            segments.append({
                "segment_id": f"pdf-page-{page_index}-para-{para_index}{suffix}",
                "source_type": "paragraph",
                "source_index": page_index,
                "block_index": para_index,
                "paragraph_index": para_index,
                "text": para_text,
                "metadata": {
                    "char_count": len(para_text),
                    "parser_adapter": "pdf",
                    "bbox": bbox,
                    "block_count": len(para_blocks),
                    "split_index": split_index if len(segment_texts) > 1 else 1,
                    "split_count": len(segment_texts),
                },
            })

    segments = _attach_orphan_headings_to_next_segment(segments)
    return segments


# ===========================================================================
# 6. ZONE SEPARATION
# ===========================================================================

def _extract_page_body_blocks(page) -> tuple[list[dict], set[str], set[str]]:
    """
    Split page into header, body and footer zones first.
    Returns body blocks and raw header/footer line sets.
    """
    blocks = _build_text_blocks(page)
    page_height = page.rect.height

    header_threshold = page_height * _HEADER_ZONE_RATIO
    footer_threshold = page_height * _FOOTER_ZONE_RATIO

    header_blocks = [b for b in blocks if b["y1"] <= header_threshold]
    footer_blocks = [b for b in blocks if b["y0"] >= footer_threshold]
    body_blocks = [
        b for b in blocks
        if b["y0"] >= header_threshold and b["y1"] <= footer_threshold
    ]

    def _lines_from_blocks(blks: list[dict]) -> set[str]:
        lines: set[str] = set()
        for block in blks:
            for line in block["text"].split("\n"):
                stripped = line.strip()
                if stripped:
                    lines.add(stripped)
        return lines

    return body_blocks, _lines_from_blocks(header_blocks), _lines_from_blocks(footer_blocks)


# ===========================================================================
# 7. PER-PAGE CONTENT EXTRACTORS
# ===========================================================================

def _extract_page_images(page, document, page_index: int) -> list[dict]:
    segments: list[dict] = []
    seen_xrefs: set[int] = set()

    for img_index, img_info in enumerate(page.get_images(full=True), start=1):
        xref = img_info[0]
        if xref in seen_xrefs:
            continue
        seen_xrefs.add(xref)

        width_hint = img_info[2]
        height_hint = img_info[3]
        if width_hint * height_hint < _MIN_IMAGE_AREA:
            continue

        try:
            base_image = document.extract_image(xref)
        except Exception:
            continue

        image_bytes = base_image.get("image")
        if not image_bytes:
            continue

        width = base_image.get("width", width_hint)
        height = base_image.get("height", height_hint)
        ext = base_image.get("ext", "png").lower()
        media_type = _FITZ_EXT_TO_MIME.get(ext, "image/png")
        data_uri = encode_image_bytes(image_bytes, media_type)

        rects = page.get_image_rects(xref)
        bbox = list(rects[0]) if rects else []

        segments.append({
            "segment_id": f"pdf-page-{page_index}-img-{img_index}",
            "source_type": "image",
            "source_index": page_index,
            "block_index": None,
            "paragraph_index": None,
            "text": "",
            "metadata": {
                "parser_adapter": "pdf",
                "width": width,
                "height": height,
                "media_type": media_type,
                "data_uri": data_uri,
                "bbox": bbox,
            },
        })

    return segments


def _extract_page_tables(page, page_index: int) -> list[dict]:
    segments: list[dict] = []

    try:
        tabs = page.find_tables()
    except AttributeError:
        return []

    for tbl_index, table in enumerate(tabs.tables, start=1):
        try:
            raw_rows = table.extract()
        except Exception:
            continue

        if not raw_rows:
            continue

        rows = [[cell if cell is not None else "" for cell in row] for row in raw_rows]
        markdown = table_to_markdown(rows, has_header=True)
        if not markdown:
            continue

        bbox = list(table.bbox) if hasattr(table, "bbox") else []

        segments.append({
            "segment_id": f"pdf-page-{page_index}-table-{tbl_index}",
            "source_type": "table",
            "source_index": page_index,
            "block_index": None,
            "paragraph_index": None,
            "text": markdown,
            "metadata": {
                "parser_adapter": "pdf",
                "row_count": len(rows),
                "col_count": max(len(r) for r in rows) if rows else 0,
                "bbox": bbox,
            },
        })

    return segments


def _extract_page_formulas(page, page_index: int) -> list[dict]:
    segments: list[dict] = []
    formula_index = 0

    for block in page.get_text("dict").get("blocks", []):
        if block.get("type") != 0:
            continue

        for line in block.get("lines", []):
            for span in line.get("spans", []):
                span_text = (span.get("text") or "").strip()
                if not span_text or not looks_like_formula(span_text):
                    continue

                formula_index += 1
                x0, y0, x1, y1 = span.get("bbox", (0, 0, 0, 0))

                segments.append({
                    "segment_id": f"pdf-page-{page_index}-formula-{formula_index}",
                    "source_type": "formula",
                    "source_index": page_index,
                    "block_index": None,
                    "paragraph_index": None,
                    "text": span_text,
                    "metadata": {
                        "parser_adapter": "pdf",
                        "bbox": [float(x0), float(y0), float(x1), float(y1)],
                    },
                })

    return segments


# ===========================================================================
# 8. POST-PROCESSING
# ===========================================================================

def _block_inside_table(block: dict, table_bboxes: list[list[float]]) -> bool:
    bx0, by0, bx1, by1 = block["x0"], block["y0"], block["x1"], block["y1"]
    cx = (bx0 + bx1) / 2.0
    cy = (by0 + by1) / 2.0

    for bbox in table_bboxes:
        if not isinstance(bbox, (list, tuple)) or len(bbox) != 4:
            continue
        tx0, ty0, tx1, ty1 = [float(v) for v in bbox]
        # block center must be inside table bbox with small margin
        if tx0 - 4 <= cx <= tx1 + 4 and ty0 - 4 <= cy <= ty1 + 4:
            return True
    return False


def _is_normal_orientation(block: dict) -> bool:
    # dir_x ≈ 1.0 and dir_y ≈ 0.0 means standard LTR horizontal text
    return block.get("dir_x", 1.0) > 0.5 and abs(block.get("dir_y", 0.0)) < 0.5


def _strip_header_footer_lines(segments: list[dict], banned_lines: set[str]) -> list[dict]:
    result: list[dict] = []

    for seg in segments:
        filtered = [
            line for line in seg["text"].split("\n")
            if line.strip() not in banned_lines
            and not _looks_like_header_footer(line)
        ]
        text = re.sub(r"\n{3,}", "\n\n", "\n".join(filtered)).strip()
        text = clean_extracted_text(text).strip()
        if text:
            result.append({**seg, "text": text, "metadata": {**seg["metadata"], "char_count": len(text)}})

    return result


def _collect_repeated_lines(segments: list[dict], page_count: int) -> set[str]:
    # Don't strip anything on very short docs — too much collateral damage
    if page_count <= 3:
        return set()

    min_repeat = max(3, int(page_count * 0.35))
    line_page_count: Counter = Counter()

    for seg in segments:
        seen_in_page: set[str] = set()
        for line in seg["text"].split("\n"):
            stripped = line.strip()
            if stripped and stripped not in seen_in_page:
                line_page_count[stripped] += 1
                seen_in_page.add(stripped)

    return {line for line, count in line_page_count.items() if count >= min_repeat}


def _reindex_paragraphs(segments: list[dict]) -> list[dict]:
    result: list[dict] = []
    grouped: dict[int, list[dict]] = {}

    for seg in segments:
        grouped.setdefault(seg["source_index"], []).append(seg)

    for page_index in sorted(grouped):
        page_segments = sorted(grouped[page_index], key=_segment_sort_key)
        for new_index, seg in enumerate(page_segments, start=1):
            result.append({
                **seg,
                "block_index": new_index,
                "paragraph_index": new_index,
                "segment_id": f"pdf-page-{page_index}-para-{new_index}",
            })

    return result


def _find_next_paragraph(segments: list[dict], from_index: int) -> int | None:
    """Return index of next paragraph segment on the immediately following page."""
    current_page = segments[from_index]["source_index"]
    for j in range(from_index + 1, min(from_index + 4, len(segments))):
        seg = segments[j]
        if seg["source_index"] > current_page + 1:
            break
        if seg["source_type"] == "paragraph":
            return j
    return None


def _merge_cross_page_paragraphs(segments: list[dict]) -> list[dict]:
    if not segments:
        return segments

    merged: list[dict] = []
    skip_indices: set[int] = set()

    for i, seg in enumerate(segments):
        if i in skip_indices:
            continue

        next_index = _find_next_paragraph(segments, i)
        if next_index is None:
            merged.append(seg)
            continue

        next_seg = segments[next_index]
        current_text = seg["text"].strip()
        next_text = next_seg["text"].strip()

        is_consecutive_page = next_seg["source_index"] == seg["source_index"] + 1
        is_same_page = next_seg["source_index"] == seg["source_index"]
        is_first_of_next = is_consecutive_page and next_seg.get("paragraph_index") == 1

        ends_incomplete = bool(re.search(r"[^.!?:)\]\"\']\s*$", current_text))
        next_starts_lower = bool(re.match(r"^[a-z\(\[\"\']", next_text))
        next_starts_list = _starts_list_item(next_text)

        should_merge = (
            (is_first_of_next or is_same_page)
            and ends_incomplete
            and next_starts_lower
            and not next_starts_list
        )

        if should_merge:
            joined = (
                current_text[:-1] + next_text
                if current_text.endswith("-")
                else current_text + " " + next_text
            )

            merged.append({
                **seg,
                "text": joined,
                "metadata": {
                    **seg["metadata"],
                    "char_count": len(joined),
                    "merged_with": next_seg["segment_id"],
                },
            })
            skip_indices.add(next_index)
        else:
            merged.append(seg)

    return merged

# ===========================================================================
# 9. PUBLIC ENTRY POINT
# ===========================================================================

def parse_pdf(file_path) -> dict:
    try:
        import fitz
    except Exception:
        return {"segments": [], "metadata": {}, "errors": [_dependency_error("PyMuPDF")]}

    file_path = Path(file_path)
    segments: list[dict] = []
    metadata = {"source_path": str(file_path), "page_count": 0}
    document = None

    try:
        document = fitz.open(file_path)
        metadata["page_count"] = document.page_count

        all_header_lines: set[str] = set()
        all_footer_lines: set[str] = set()

        for page_index, page in enumerate(document, start=1):
            page_width = float(page.rect.width)
            page_height = float(page.rect.height)

            body_blocks, header_lines, footer_lines = _extract_page_body_blocks(page)
            all_header_lines |= header_lines
            all_footer_lines |= footer_lines

            table_segments = _extract_page_tables(page, page_index)
            table_bboxes = [
                s["metadata"]["bbox"]
                for s in table_segments
                if isinstance(s.get("metadata", {}).get("bbox"), (list, tuple))
                and len(s["metadata"]["bbox"]) == 4
            ]
            if table_bboxes:
                body_blocks = [b for b in body_blocks if not _block_inside_table(b, table_bboxes)]
            body_blocks = [b for b in body_blocks if _is_normal_orientation(b)]

            if body_blocks:
                ordered_blocks = _order_body_blocks(body_blocks, page_height, page_width)
                import sys
                for i, b in enumerate(ordered_blocks[:20]):
                    print(
                        f"  [{i}] y={b['y0']:.0f}-{b['y1']:.0f} x={b['x0']:.0f}-{b['x1']:.0f} "
                        f"text={b['text'][:50]!r}",
                        file=sys.stderr,
                    )
                paragraph_segments = _build_paragraph_segments(
                    ordered_blocks=ordered_blocks,
                    page_index=page_index,
                    page_width=page_width,
                )
                segments.extend(paragraph_segments)

            segments.extend(_extract_page_images(page, document, page_index))
            segments.extend(table_segments)
            segments.extend(_extract_page_formulas(page, page_index))

        paragraph_segs = [s for s in segments if s["source_type"] == "paragraph"]
        other_segs = [s for s in segments if s["source_type"] != "paragraph"]

        paragraph_segs.sort(key=_segment_sort_key)

        banned_lines = all_header_lines | all_footer_lines
        paragraph_segs = _strip_header_footer_lines(paragraph_segs, banned_lines)

        repeated_lines = _collect_repeated_lines(paragraph_segs, metadata["page_count"])
        paragraph_segs = _strip_header_footer_lines(paragraph_segs, repeated_lines)

        paragraph_segs = _reindex_paragraphs(paragraph_segs)
        paragraph_segs = _merge_cross_page_paragraphs(paragraph_segs)

        all_segments = paragraph_segs + other_segs
        all_segments.sort(key=lambda s: (
            s["source_index"],
            s.get("block_index") if s.get("block_index") is not None else 0,
            s.get("paragraph_index") if s.get("paragraph_index") is not None else 0,
            s.get("segment_id", ""),
        ))

        return {
            "segments": all_segments,
            "metadata": metadata,
            "errors": [],
        }

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
