from __future__ import annotations

import hashlib
from importlib import metadata
import io
import json
import logging
import re
import zipfile
from pathlib import Path

from services.parsers.utils import clean_extracted_text, table_to_markdown

from .client import request_with_retry
from .constants import IMAGE_MIME_BY_EXT

_FIGURE_REF_PATTERN = re.compile(r"\b(?:fig(?:ure)?)[\.\s]+(\d+)\b", re.IGNORECASE)
_TABLE_REF_PATTERN = re.compile(r"\btable\s+(\d+)\b", re.IGNORECASE)
_SECTION_REF_PATTERN = re.compile(r"\b(?:sect(?:ion)?)[\.\s]+((?:\d+\.)*\d+)\b", re.IGNORECASE)
_SECTION_NUMBER_PATTERN = re.compile(r"^\s*((?:\d+\.)*\d+)\b")

PARAGRAPH_BOUNDARY = "\n\n"
WORD_CONTINUATION = " "

logger = logging.getLogger(__name__)


def safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def download_zip(zip_url: str) -> zipfile.ZipFile:
    response = request_with_retry("GET", zip_url, _timeout_seconds=300)
    return zipfile.ZipFile(io.BytesIO(response.content))


def parse_zip(
    zf: zipfile.ZipFile,
    source_path: str,
    *,
    document_id: str | None = None,
    asset_output_dir: str | Path | None = None,
) -> dict:
    manifest = discover_zip_artifacts(zf)
    raw_artifacts = load_raw_artifacts(zf, manifest)
    logger.warning(
        "MinerU raw artifacts received for %s: %s",
        source_path,
        json.dumps(_build_raw_artifact_debug_preview(raw_artifacts), ensure_ascii=True, default=str),
    )
    normalized_artifacts = normalize_coordinates(raw_artifacts)
    intermediate_blocks = build_intermediate_blocks(normalized_artifacts)
    segments, segment_context = build_segments_from_blocks(intermediate_blocks)
    segments, postprocess_stats = post_process_segments(segments)
    segment_context["post_processing"] = postprocess_stats
    segments, heading_stats = attach_section_paths(segments)
    assets, asset_warnings = extract_assets(
        intermediate_blocks,
        normalized_artifacts,
        source_path=source_path,
        document_id=document_id,
        asset_output_dir=asset_output_dir,
    )
    attach_asset_links_to_segments(segments, assets)
    references = extract_and_resolve_references(segments, assets)

    metadata = build_metadata(
        source_path=source_path,
        manifest=manifest,
        raw_artifacts=raw_artifacts,
        normalized_artifacts=normalized_artifacts,
        segment_context=segment_context,
        assets=assets,
        references=references,
        heading_stats=heading_stats,
        warnings=asset_warnings,
    )

    return {
        "segments": segments,
        "assets": assets,
        "references": references,
        "metadata": metadata,
        "errors": [],
    }


def discover_zip_artifacts(zf: zipfile.ZipFile) -> dict:
    names = zf.namelist()
    content_list_v2 = next((name for name in names if name.endswith("content_list_v2.json")), None)
    flat_content_list = next(
        (name for name in names if name.endswith("content_list.json") and not name.endswith("content_list_v2.json")),
        None,
    )
    model_json = next((name for name in names if name.endswith("_model.json")), None)
    markdown = next((name for name in names if Path(name).name.lower() == "full.md"), None)
    if not markdown:
        markdown = next((name for name in names if name.lower().endswith(".md")), None)

    image_files = []
    for name in names:
        suffix = Path(name).suffix.lower().lstrip(".")
        if suffix in IMAGE_MIME_BY_EXT:
            image_files.append(name)

    return {
        "content_list_v2": content_list_v2,
        "content_list": flat_content_list,
        "model_json": model_json,
        "markdown": markdown,
        "image_files": sorted(image_files),
        "all_names": names,
    }


def load_raw_artifacts(zf: zipfile.ZipFile, manifest: dict) -> dict:
    raw = {
        "manifest": manifest,
        "content_list_v2": None,
        "content_list": None,
        "model_json": None,
        "markdown": "",
        "images": {},
    }

    if manifest.get("content_list_v2"):
        raw["content_list_v2"] = json.loads(zf.read(manifest["content_list_v2"]).decode("utf-8", errors="replace"))
    if manifest.get("content_list"):
        raw["content_list"] = json.loads(zf.read(manifest["content_list"]).decode("utf-8", errors="replace"))
    if manifest.get("model_json"):
        raw["model_json"] = json.loads(zf.read(manifest["model_json"]).decode("utf-8", errors="replace"))
    if manifest.get("markdown"):
        raw["markdown"] = zf.read(manifest["markdown"]).decode("utf-8", errors="replace")

    for image_name in manifest.get("image_files", []):
        try:
            raw["images"][image_name] = zf.read(image_name)
        except Exception:
            continue

    return raw


def _build_raw_artifact_debug_preview(raw_artifacts: dict) -> dict:
    content_list_v2 = raw_artifacts.get("content_list_v2")
    flat_content_list = raw_artifacts.get("content_list")
    model_json = raw_artifacts.get("model_json")
    markdown = raw_artifacts.get("markdown") or ""
    manifest = raw_artifacts.get("manifest") or {}

    first_v2_block = None
    if isinstance(content_list_v2, list):
        for page_blocks in content_list_v2:
            if isinstance(page_blocks, list) and page_blocks:
                first_v2_block = page_blocks[0]
                break

    first_flat_item = flat_content_list[0] if isinstance(flat_content_list, list) and flat_content_list else None
    first_model_block = None
    if isinstance(model_json, list):
        for page_blocks in model_json:
            if isinstance(page_blocks, list) and page_blocks:
                first_model_block = page_blocks[0]
                break

    return {
        "manifest": {
            "content_list_v2": manifest.get("content_list_v2"),
            "content_list": manifest.get("content_list"),
            "model_json": manifest.get("model_json"),
            "markdown": manifest.get("markdown"),
            "image_count": len(manifest.get("image_files") or []),
        },
        "counts": {
            "content_list_v2_pages": len(content_list_v2) if isinstance(content_list_v2, list) else 0,
            "content_list_items": len(flat_content_list) if isinstance(flat_content_list, list) else 0,
            "model_json_pages": len(model_json) if isinstance(model_json, list) else 0,
            "markdown_chars": len(markdown),
        },
        "first_v2_block": _compact_debug_value(first_v2_block),
        "first_flat_item": _compact_debug_value(first_flat_item),
        "first_model_block": _compact_debug_value(first_model_block),
        "markdown_preview": markdown[:500],
    }


def _compact_debug_value(value, *, max_items: int = 6, max_string: int = 200):
    if isinstance(value, dict):
        compact: dict[str, object] = {}
        for index, (key, item) in enumerate(value.items()):
            if index >= max_items:
                compact["..."] = f"{len(value) - max_items} more keys"
                break
            compact[str(key)] = _compact_debug_value(item, max_items=max_items, max_string=max_string)
        return compact

    if isinstance(value, list):
        compact_list = [
            _compact_debug_value(item, max_items=max_items, max_string=max_string)
            for item in value[:max_items]
        ]
        if len(value) > max_items:
            compact_list.append(f"... {len(value) - max_items} more items")
        return compact_list

    if isinstance(value, str):
        return value[:max_string]

    return value


def _build_raw_artifact_payload(raw_artifacts: dict) -> dict:
    manifest = raw_artifacts.get("manifest") or {}
    return {
        "manifest": {
            "content_list_v2": manifest.get("content_list_v2"),
            "content_list": manifest.get("content_list"),
            "model_json": manifest.get("model_json"),
            "markdown": manifest.get("markdown"),
            "image_files": manifest.get("image_files") or [],
            "all_names": manifest.get("all_names") or [],
        },
        "content_list_v2": raw_artifacts.get("content_list_v2"),
        "content_list": raw_artifacts.get("content_list"),
        "model_json": raw_artifacts.get("model_json"),
        "markdown": raw_artifacts.get("markdown") or "",
    }


def normalize_coordinates(raw_artifacts: dict) -> dict:
    content_list_v2 = raw_artifacts.get("content_list_v2")
    flat_content_list = raw_artifacts.get("content_list")
    model_json = raw_artifacts.get("model_json")
    page_dimensions: dict[int, tuple[float, float]] = {}

    if isinstance(model_json, list):
        for page_index, page_blocks in enumerate(model_json, start=1):
            max_x = 0.0
            max_y = 0.0
            if isinstance(page_blocks, list):
                for block in page_blocks:
                    if not isinstance(block, dict):
                        continue
                    poly = block.get("poly") or []
                    if isinstance(poly, list) and poly:
                        for idx, value in enumerate(poly):
                            numeric = float(value)
                            if idx % 2 == 0:
                                max_x = max(max_x, numeric)
                            else:
                                max_y = max(max_y, numeric)
                    bbox = block.get("bbox") or []
                    if len(bbox) == 4:
                        x0, y0, x1, y1 = [float(value) for value in bbox]
                        if x1 > 1.5 or y1 > 1.5:
                            max_x = max(max_x, x1)
                            max_y = max(max_y, y1)
            if max_x > 0 and max_y > 0:
                page_dimensions[page_index] = (max_x, max_y)

    if isinstance(content_list_v2, list):
        for page_index, page_blocks in enumerate(content_list_v2, start=1):
            if page_index in page_dimensions or not isinstance(page_blocks, list):
                continue
            max_x = 0.0
            max_y = 0.0
            for block in page_blocks:
                if not isinstance(block, dict):
                    continue
                bbox = block.get("bbox") or []
                if len(bbox) == 4:
                    _, _, x1, y1 = [float(value) for value in bbox]
                    max_x = max(max_x, x1)
                    max_y = max(max_y, y1)
            if max_x > 0 and max_y > 0:
                page_dimensions[page_index] = (max_x, max_y)

    if isinstance(flat_content_list, list):
        for item in flat_content_list:
            if not isinstance(item, dict):
                continue
            page_index = safe_int(item.get("page_idx"), 0) + 1
            if page_index in page_dimensions:
                continue
            bbox = item.get("bbox") or []
            if len(bbox) == 4:
                _, _, x1, y1 = [float(value) for value in bbox]
                if x1 > 0 and y1 > 0:
                    page_dimensions[page_index] = (x1, y1)

    if isinstance(content_list_v2, list):
        for page_index, page_blocks in enumerate(content_list_v2, start=1):
            if not isinstance(page_blocks, list):
                continue
            for block in page_blocks:
                if not isinstance(block, dict):
                    continue
                block["normalized_bbox"] = _normalize_bbox(block.get("bbox"), page_dimensions.get(page_index))
                block["page_index"] = page_index

    if isinstance(flat_content_list, list):
        for item in flat_content_list:
            if not isinstance(item, dict):
                continue
            page_index = safe_int(item.get("page_idx"), 0) + 1
            item["normalized_bbox"] = _normalize_bbox(item.get("bbox"), page_dimensions.get(page_index))
            item["page_index"] = page_index

    if isinstance(model_json, list):
        for page_index, page_blocks in enumerate(model_json, start=1):
            if not isinstance(page_blocks, list):
                continue
            for block in page_blocks:
                if not isinstance(block, dict):
                    continue
                block["normalized_bbox"] = _normalize_model_bbox(block.get("bbox"))
                block["page_index"] = page_index

    return {
        **raw_artifacts,
        "page_dimensions": page_dimensions,
    }


def build_intermediate_blocks(normalized_artifacts: dict) -> list[dict]:
    content_list_v2 = normalized_artifacts.get("content_list_v2")
    flat_content_list = normalized_artifacts.get("content_list")
    blocks: list[dict] = []

    if isinstance(content_list_v2, list) and content_list_v2:
        flat_lookup = build_flat_lookup(flat_content_list)
        for page_index, page_blocks in enumerate(content_list_v2, start=1):
            if not isinstance(page_blocks, list):
                continue
            for order_index, item in enumerate(page_blocks, start=1):
                block = build_v2_intermediate_block(item, page_index, order_index, flat_lookup)
                if block:
                    blocks.append(block)
    elif isinstance(flat_content_list, list):
        for order_index, item in enumerate(flat_content_list, start=1):
            block = build_flat_intermediate_block(item, order_index)
            if block:
                blocks.append(block)
    else:
        blocks.extend(build_markdown_fallback_blocks(normalized_artifacts.get("markdown") or ""))

    blocks.sort(key=lambda block: (block.get("page_index", 0), block.get("order_index", 0), block.get("block_type", "")))
    return blocks


def build_segments_from_blocks(intermediate_blocks: list[dict]) -> tuple[list[dict], dict]:
    segments: list[dict] = []
    suppressed_counts = {
        "page_header": 0,
        "page_footer": 0,
        "page_number": 0,
    }

    for block in intermediate_blocks:
        page_index = block.get("page_index")
        order_index = block.get("order_index")
        bbox = block.get("bbox") or []
        block_type = block.get("block_type")
        source_file = block.get("source_file")

        if block_type in suppressed_counts:
            suppressed_counts[block_type] += 1
            continue

        if block_type == "heading":
            text = clean_extracted_text(block.get("text") or "")
            if not text:
                continue
            segments.append(build_segment(
                segment_id=f"mineru-page-{page_index}-block-{order_index}-heading",
                text=text,
                source_type="paragraph",
                source_index=page_index,
                block_index=order_index,
                paragraph_index=1,
                metadata={
                    "role": "heading",
                    "heading_level": block.get("heading_level") or 1,
                    "bbox": bbox,
                    "reading_order": order_index,
                    "source_locator": block.get("source_locator") or "",
                    "source_file": source_file,
                    "mineru_type": "title",
                    "layout_role": "heading",
                    "can_merge_text": False,
                },
            ))
            continue

        if block_type == "paragraph":
            text = clean_extracted_text(block.get("text") or "")
            if not text:
                continue
            segments.append(build_segment(
                segment_id=f"mineru-page-{page_index}-block-{order_index}-paragraph",
                text=text,
                source_type="paragraph",
                source_index=page_index,
                block_index=order_index,
                paragraph_index=1,
                metadata={
                    "role": "paragraph",
                    "bbox": bbox,
                    "reading_order": order_index,
                    "source_locator": block.get("source_locator") or "",
                    "source_file": source_file,
                    "mineru_type": block.get("original_type") or "paragraph",
                    "layout_role": "body_text",
                    "can_merge_text": True,
                },
            ))
            continue

        if block_type == "list":
            for item_index, item_text in enumerate(block.get("list_items") or [], start=1):
                text = clean_extracted_text(item_text)
                if not text:
                    continue
                segments.append(build_segment(
                    segment_id=f"mineru-page-{page_index}-block-{order_index}-list-{item_index}",
                    text=text,
                    source_type="paragraph",
                    source_index=page_index,
                    block_index=order_index,
                    paragraph_index=item_index,
                    metadata={
                        "role": "list_item",
                        "bbox": bbox,
                        "reading_order": order_index,
                        "source_locator": block.get("source_locator") or "",
                        "source_file": source_file,
                        "mineru_type": "list",
                        "list_type": block.get("list_type") or "list",
                        "layout_role": "body_text",
                        "can_merge_text": True,
                    },
                ))
            continue

        if block_type == "reference_list":
            for item_index, item_text in enumerate(block.get("list_items") or [], start=1):
                text = clean_extracted_text(item_text)
                if not text:
                    continue
                segments.append(build_segment(
                    segment_id=f"mineru-page-{page_index}-block-{order_index}-ref-{item_index}",
                    text=text,
                    source_type="paragraph",
                    source_index=page_index,
                    block_index=order_index,
                    paragraph_index=item_index,
                    metadata={
                        "role": "reference_entry",
                        "bbox": bbox,
                        "reading_order": order_index,
                        "source_locator": block.get("source_locator") or "",
                        "source_file": source_file,
                        "mineru_type": "reference_list",
                        "layout_role": "body_text",
                        "can_merge_text": True,
                    },
                ))
            continue

        if block_type == "table":
            table_text = clean_extracted_text(block.get("table_text") or "")
            if not table_text:
                continue
            metadata = {
                "role": "table",
                "bbox": bbox,
                "reading_order": order_index,
                "source_locator": block.get("source_locator") or "",
                "source_file": source_file,
                "mineru_type": "table",
                "layout_role": "table",
                "can_merge_text": False,
                "is_merge_interruptor": True,
            }
            if block.get("table_html"):
                metadata["table_html"] = block["table_html"]
            table_caption_text = clean_extracted_text(block.get("table_caption_text") or "")
            if table_caption_text:
                metadata["table_caption"] = table_caption_text
            table_footnote_texts = [
                clean_extracted_text(value)
                for value in (block.get("table_footnote_texts") or [])
                if clean_extracted_text(value)
            ]
            if table_footnote_texts:
                metadata["table_footnote"] = table_footnote_texts
            label = extract_table_label(table_text)
            if label:
                metadata["reference_label"] = label
                metadata["reference_key"] = normalize_reference_key("table", label)
            segments.append(build_segment(
                segment_id=f"mineru-page-{page_index}-block-{order_index}-table",
                text=table_text,
                source_type="table",
                source_index=page_index,
                block_index=order_index,
                paragraph_index=None,
                metadata=metadata,
            ))
            continue

        if block_type == "image":
            caption_text = clean_extracted_text(block.get("caption_text") or "")
            if not caption_text:
                continue
            label = extract_figure_label(caption_text)
            metadata = {
                "role": "figure_caption",
                "bbox": bbox,
                "reading_order": order_index,
                "source_locator": block.get("source_locator") or "",
                "source_file": source_file,
                "mineru_type": "image",
                "layout_role": "figure_caption",
                "can_merge_text": False,
                "is_merge_interruptor": True,
            }
            if label:
                metadata["reference_label"] = label
                metadata["reference_key"] = normalize_reference_key("figure", label)
            if block.get("image_path"):
                metadata["image_path"] = block["image_path"]
            if block.get("footnote_text"):
                metadata["footnote_text"] = block["footnote_text"]
            segments.append(build_segment(
                segment_id=f"mineru-page-{page_index}-block-{order_index}-figure-caption",
                text=caption_text,
                source_type="paragraph",
                source_index=page_index,
                block_index=order_index,
                paragraph_index=1,
                metadata=metadata,
            ))

    return segments, {"suppressed_counts": suppressed_counts}


def _segment_bbox(segment: dict) -> list[float]:
    bbox = segment.get("metadata", {}).get("bbox") or []
    return bbox if isinstance(bbox, list) and len(bbox) == 4 else []


def _segment_reading_order(segment: dict) -> int:
    return safe_int(segment.get("metadata", {}).get("reading_order"), safe_int(segment.get("block_index"), 0))


def _segment_page_index(segment: dict) -> int:
    return safe_int(segment.get("source_index"), 0)


def _segment_role(segment: dict) -> str:
    return str(segment.get("metadata", {}).get("role") or "").strip().lower()


def _can_merge_text_segment(segment: dict) -> bool:
    metadata = segment.get("metadata") or {}
    if metadata.get("can_merge_text") is True:
        return True
    return _segment_role(segment) in {"paragraph", "list_item", "reference_entry"}


def _is_interruptor_segment(segment: dict) -> bool:
    metadata = segment.get("metadata") or {}
    if metadata.get("is_merge_interruptor") is True:
        return True
    return _segment_role(segment) in {"figure_caption", "table"}


def _is_hard_boundary_segment(segment: dict) -> bool:
    return _segment_role(segment) == "heading"


def _infer_column_bucket(segment: dict) -> str:
    bbox = _segment_bbox(segment)
    if len(bbox) != 4:
        return "unknown"
    x0, _, x1, _ = [float(value) for value in bbox]
    width = max(0.0, x1 - x0)
    center = (x0 + x1) / 2.0
    if width >= 0.72:
        return "full"
    if center <= 0.42:
        return "left"
    if center >= 0.58:
        return "right"
    return "center"


def _bbox_vertical_gap(previous: dict, current: dict) -> float | None:
    prev_bbox = _segment_bbox(previous)
    curr_bbox = _segment_bbox(current)
    if len(prev_bbox) != 4 or len(curr_bbox) != 4:
        return None
    return float(curr_bbox[1]) - float(prev_bbox[3])


def _text_ends_open(text: str) -> bool:
    compact = str(text or "").strip()
    if not compact:
        return False
    if compact.endswith("-"):
        return True
    return not bool(re.search(r'[.!?;:)\]"\']\s*$', compact))


def _text_starts_like_continuation(text: str) -> bool:
    compact = str(text or "").strip()
    if not compact:
        return False
    if re.match(r'^[a-z(\["\']', compact):
        return True
    if re.match(r"^\d", compact):
        return True
    if re.match(r"^\[[0-9,\-\s]+\]", compact):
        return True
    if re.match(r"^(and|or|but|for|nor|so|yet|to|of|in|on|with|by|from|as|that|which|who|whose|where|when)\b", compact, re.IGNORECASE):
        return True
    return False


def _sections_compatible(previous: dict, current: dict) -> bool:
    prev_sections = previous.get("metadata", {}).get("section_path") or []
    curr_sections = current.get("metadata", {}).get("section_path") or []
    if not prev_sections or not curr_sections:
        return True
    return prev_sections == curr_sections

def _is_new_paragraph_boundary(prev_text: str, curr_text: str) -> bool:
    """True when evidence suggests curr_text starts a new paragraph, not a continuation."""
    ends_closed = prev_text and prev_text[-1] in ".?!:"
    starts_fresh = curr_text and curr_text[0].isupper()
    starts_like_continuation = _text_starts_like_continuation(curr_text)
    return ends_closed and starts_fresh and not starts_like_continuation

def _continuation_score(previous: dict, current: dict, interruptors: list[dict]) -> int:
    prev_text = str(previous.get("text") or "").strip()
    curr_text = str(current.get("text") or "").strip()
    if not prev_text or not curr_text:
        return -999

    prev_role = _segment_role(previous)
    curr_role = _segment_role(current)
    if prev_role != curr_role and {prev_role, curr_role} != {"paragraph", "list_item"}:
        return -999

    score = 0
    if _text_ends_open(prev_text):
        score += 3
    if _text_starts_like_continuation(curr_text):
        score += 3

    prev_page = _segment_page_index(previous)
    curr_page = _segment_page_index(current)
    page_gap = curr_page - prev_page
    if page_gap == 0:
        score += 2
    elif page_gap == 1:
        score += 1
    else:
        return -999

    prev_col = _infer_column_bucket(previous)
    curr_col = _infer_column_bucket(current)
    if page_gap == 0 and prev_col == "left" and curr_col == "right":
        score += 2
    elif page_gap == 0 and prev_col == curr_col:
        vertical_gap = _bbox_vertical_gap(previous, current)
        if vertical_gap is not None and vertical_gap <= 0.12:
            score += 2
    elif page_gap == 1 and curr_col in {"left", "full", "unknown"}:
        score += 2

    order_gap = _segment_reading_order(current) - _segment_reading_order(previous)
    if page_gap == 0 and order_gap in {1, 2, 3}:
        score += 1
    if interruptors:
        score -= len(interruptors)
        if any(_segment_role(item) == "heading" for item in interruptors):
            return -999
        if len(interruptors) <= 2:
            score += 1

    if _sections_compatible(previous, current):
        score += 1

    if prev_text.endswith("-"):
        score += 1

    # After all existing score computation, before return:
    if _is_new_paragraph_boundary(prev_text, curr_text):
        score -= 4  # strong penalty — pushes most same-page cases below threshold 6

    return score


def _merge_segment_pair(base_segment: dict, continuation_segment: dict, interruptors: list[dict]) -> dict:
    base_text = str(base_segment.get("text") or "").strip()
    continuation_text = str(continuation_segment.get("text") or "").strip() 
    
    if base_text.endswith("-"):
        merged_text = f"{base_text[:-1]}{continuation_text}"
    else:
        if _is_new_paragraph_boundary(base_text, continuation_text):
            merged_text = f"{base_text}\n\n{continuation_text}"
        else:
            merged_text = f"{base_text} {continuation_text}".strip()

    metadata = {
        **(base_segment.get("metadata") or {}),
    }
    merged_from = list(metadata.get("merged_from_segment_ids") or [])
    merged_from.extend([base_segment.get("segment_id"), continuation_segment.get("segment_id")])
    seen_ids: list[str] = []
    for segment_id in merged_from:
        safe_id = str(segment_id or "").strip()
        if safe_id and safe_id not in seen_ids:
            seen_ids.append(safe_id)
    metadata["merged_from_segment_ids"] = seen_ids

    interruption_ids = list(metadata.get("interruption_segment_ids") or [])
    for interruptor in interruptors:
        interrupt_id = str(interruptor.get("segment_id") or "").strip()
        if interrupt_id and interrupt_id not in interruption_ids:
            interruption_ids.append(interrupt_id)
    if interruption_ids:
        metadata["interruption_segment_ids"] = interruption_ids

    metadata["char_count"] = len(merged_text)
    metadata["merged_continuation_count"] = safe_int(metadata.get("merged_continuation_count"), 0) + 1
    metadata["paragraph_boundary_count"] = safe_int(metadata.get("paragraph_boundary_count"), 0) + (
        1 if _is_new_paragraph_boundary(base_text, continuation_text) else 0
    )
    metadata["continued_into_page"] = _segment_page_index(continuation_segment)
    metadata["continued_into_reading_order"] = _segment_reading_order(continuation_segment)

    return {
        **base_segment,
        "text": merged_text,
        "metadata": metadata,
    }


def post_process_segments(segments: list[dict]) -> tuple[list[dict], dict]:
    ordered_segments = sorted(
        segments,
        key=lambda segment: (
            _segment_page_index(segment),
            _segment_reading_order(segment),
            safe_int(segment.get("paragraph_index"), 0),
            str(segment.get("segment_id") or ""),
        ),
    )

    consumed_ids: set[str] = set()
    merged_count = 0
    interruption_merge_count = 0

    for index, segment in enumerate(ordered_segments):
        segment_id = str(segment.get("segment_id") or "").strip()
        if not segment_id or segment_id in consumed_ids or not _can_merge_text_segment(segment):
            continue

        current = segment
        cursor = index + 1
        while cursor < len(ordered_segments):
            interruptors: list[dict] = []
            candidate = None
            scan = cursor

            while scan < len(ordered_segments):
                nxt = ordered_segments[scan]
                nxt_id = str(nxt.get("segment_id") or "").strip()
                if not nxt_id or nxt_id in consumed_ids:
                    scan += 1
                    continue

                page_gap = _segment_page_index(nxt) - _segment_page_index(current)
                if page_gap > 1:
                    break

                if _is_hard_boundary_segment(nxt):
                    candidate = None
                    break
                if _is_interruptor_segment(nxt):
                    interruptors.append(nxt)
                    if len(interruptors) > 2:
                        candidate = None
                        break
                    scan += 1
                    continue
                if not _can_merge_text_segment(nxt):
                    candidate = None
                    break

                candidate = nxt
                break

            if not candidate:
                break

            score = _continuation_score(current, candidate, interruptors)
            if score < 6:
                break

            consumed_ids.add(str(candidate.get("segment_id") or "").strip())
            current = _merge_segment_pair(current, candidate, interruptors)
            ordered_segments[index] = current
            merged_count += 1
            if interruptors:
                interruption_merge_count += 1
            cursor = scan + 1

    final_segments = []
    for segment in ordered_segments:
        segment_id = str(segment.get("segment_id") or "").strip()
        if segment_id and segment_id in consumed_ids:
            continue
        final_segments.append(segment)

    stats = {
        "merged_segment_count": merged_count,
        "interruption_merge_count": interruption_merge_count,
        "output_segment_count": len(final_segments),
    }
    return final_segments, stats


def attach_section_paths(segments: list[dict]) -> tuple[list[dict], dict]:
    heading_stack: list[dict] = []
    applied_count = 0

    for segment in segments:
        metadata = segment.get("metadata") or {}
        if metadata.get("role") == "heading":
            level = safe_int(metadata.get("heading_level"), 1)
            label = segment.get("text") or ""
            number = extract_section_number(label)
            entry = {
                "level": level,
                "label": label,
                "number": number,
            }
            while heading_stack and heading_stack[-1]["level"] >= level:
                heading_stack.pop()
            heading_stack.append(entry)

        if heading_stack:
            metadata["section_path"] = [entry["label"] for entry in heading_stack]
            section_numbers = [entry["number"] for entry in heading_stack if entry.get("number")]
            if section_numbers:
                metadata["section_numbers"] = section_numbers
            segment["metadata"] = metadata
            applied_count += 1

    return segments, {"segments_with_section_path": applied_count}


def extract_assets(
    intermediate_blocks: list[dict],
    normalized_artifacts: dict,
    *,
    source_path: str,
    document_id: str | None,
    asset_output_dir: str | Path | None,
) -> tuple[list[dict], list[str]]:
    asset_dir = Path(asset_output_dir) if asset_output_dir else None
    if asset_dir:
        asset_dir.mkdir(parents=True, exist_ok=True)

    source_file = Path(source_path)
    document_token = str(document_id or source_file.stem)
    source_parent = source_file.parent
    relative_root = source_parent.parent if source_parent.parent != source_parent else source_parent
    image_bytes_lookup = normalized_artifacts.get("images") or {}
    warnings: list[str] = []
    assets: list[dict] = []
    seen_hashes: dict[str, dict] = {}

    for block in intermediate_blocks:
        if block.get("block_type") != "image":
            continue

        image_path = str(block.get("image_path") or "").strip()
        image_bytes = image_bytes_lookup.get(image_path) or image_bytes_lookup.get(Path(image_path).name)
        if not image_bytes:
            warnings.append(f"missing_image:{image_path or 'unknown'}")
            continue

        content_hash = hashlib.sha256(image_bytes).hexdigest()
        existing = seen_hashes.get(content_hash)
        if existing:
            existing.setdefault("linked_block_orders", []).append(block.get("order_index"))
            continue
        ext = Path(image_path).suffix.lower().lstrip(".") or "png"
        mime_type = IMAGE_MIME_BY_EXT.get(ext, "image/png")
        file_name = f"{content_hash}.{ext}"
        storage_path = ""
        upload_path = ""
        if asset_dir:
            destination = asset_dir / file_name
            if not destination.exists():
                destination.write_bytes(image_bytes)
            storage_path = str(destination)
            try:
                upload_path = str(destination.relative_to(relative_root)).replace("\\", "/")
            except Exception:
                upload_path = destination.name

        asset_index = len(assets)
        asset = {
            "asset_id": f"{document_token}-asset-{asset_index + 1}",
            "asset_type": "image",
            "storage_path": storage_path,
            "upload_path": upload_path,
            "original_zip_path": image_path,
            "mime_type": mime_type,
            "byte_size": len(image_bytes),
            "content_hash": content_hash,
            "source_index": block.get("page_index"),
            "bbox": block.get("bbox") or [],
            "caption_segment_id": None,
            "metadata": {
                "source_file": block.get("source_file"),
                "reading_order": block.get("order_index"),
                "caption_text": block.get("caption_text") or "",
                "footnote_text": block.get("footnote_text") or "",
            },
        }
        assets.append(asset)
        seen_hashes[content_hash] = asset

    return assets, warnings


def attach_asset_links_to_segments(segments: list[dict], assets: list[dict]) -> None:
    assets_by_path = {}
    for asset in assets:
        original_path = str(asset.get("original_zip_path") or "").strip()
        if original_path:
            assets_by_path[original_path] = asset
            assets_by_path[Path(original_path).name] = asset

    for segment in segments:
        metadata = segment.get("metadata") or {}
        image_path = str(metadata.get("image_path") or "").strip()
        if not image_path:
            continue
        asset = assets_by_path.get(image_path) or assets_by_path.get(Path(image_path).name)
        if not asset:
            continue
        metadata["asset_id"] = asset["asset_id"]
        metadata["asset_upload_path"] = asset.get("upload_path") or ""
        segment["metadata"] = metadata
        asset["caption_segment_id"] = segment["segment_id"]


def extract_and_resolve_references(segments: list[dict], assets: list[dict]) -> list[dict]:
    figure_targets = {}
    table_targets = {}
    section_targets = {}
    asset_by_id = {asset["asset_id"]: asset for asset in assets}

    for segment in segments:
        metadata = segment.get("metadata") or {}
        role = metadata.get("role")
        if role == "figure_caption":
            reference_key = str(metadata.get("reference_key") or "").strip()
            if not reference_key:
                continue
            figure_targets[reference_key] = {
                "segment_id": segment["segment_id"],
                "asset_id": metadata.get("asset_id"),
            }
        elif role == "table":
            reference_key = str(metadata.get("reference_key") or "").strip()
            if not reference_key:
                continue
            table_targets[reference_key] = {"segment_id": segment["segment_id"]}
        elif role == "heading":
            number = extract_section_number(segment.get("text") or "")
            if number:
                section_targets[normalize_reference_key("section", number)] = {"segment_id": segment["segment_id"]}

    references: list[dict] = []
    for segment in segments:
        text = str(segment.get("text") or "")
        if not text:
            continue

        references.extend(build_segment_references(
            segment,
            kind="figure",
            matches=_FIGURE_REF_PATTERN.findall(text),
            targets=figure_targets,
        ))
        references.extend(build_segment_references(
            segment,
            kind="table",
            matches=_TABLE_REF_PATTERN.findall(text),
            targets=table_targets,
        ))
        references.extend(build_segment_references(
            segment,
            kind="section",
            matches=_SECTION_REF_PATTERN.findall(text),
            targets=section_targets,
        ))

    for reference in references:
        asset_id = reference.get("target_asset_id")
        if asset_id and asset_id not in asset_by_id:
            reference["target_asset_id"] = None
            reference["resolution_status"] = "unresolved"
            reference["confidence"] = 0.0

    return references


def build_metadata(
    *,
    source_path: str,
    manifest: dict,
    raw_artifacts: dict,
    normalized_artifacts: dict,
    segment_context: dict,
    assets: list[dict],
    references: list[dict],
    heading_stats: dict,
    warnings: list[str],
) -> dict:
    page_count = 0
    for key in ("content_list_v2", "model_json"):
        value = normalized_artifacts.get(key)
        if isinstance(value, list):
            page_count = max(page_count, len(value))

    unresolved_reference_count = sum(1 for reference in references if reference.get("resolution_status") != "resolved")

    return {
        "source_path": source_path,
        "parser": "mineru_api",
        "mode": "upload_batch",
        "model_version": "vlm",
        "page_count": page_count,
        "artifact_manifest": {
            "content_list_v2": manifest.get("content_list_v2"),
            "content_list": manifest.get("content_list"),
            "model_json": manifest.get("model_json"),
            "markdown": manifest.get("markdown"),
            "image_count": len(manifest.get("image_files") or []),
        },
        "mineru_raw_artifacts": _build_raw_artifact_payload(raw_artifacts),
        "normalization_warnings": warnings,
        "asset_count": len(assets),
        "reference_count": len(references),
        "unresolved_reference_count": unresolved_reference_count,
        "header_footer_suppression_stats": segment_context.get("suppressed_counts") or {},
        "post_processing": segment_context.get("post_processing") or {},
        "heading_stats": heading_stats,
    }


def build_flat_lookup(flat_content_list) -> dict[int, list[dict]]:
    lookup: dict[int, list[dict]] = {}
    if not isinstance(flat_content_list, list):
        return lookup

    for item in flat_content_list:
        if not isinstance(item, dict):
            continue
        page_index = item.get("page_index") or (safe_int(item.get("page_idx"), 0) + 1)
        lookup.setdefault(page_index, []).append(item)
    return lookup


def build_v2_intermediate_block(item: dict, page_index: int, order_index: int, flat_lookup: dict[int, list[dict]]) -> dict | None:
    if not isinstance(item, dict):
        return None

    item_type = str(item.get("type") or "").strip().lower()
    content = item.get("content") or {}
    bbox = item.get("normalized_bbox") or []
    base_block = {
        "page_index": page_index,
        "order_index": order_index,
        "bbox": bbox,
        "source_file": "content_list_v2.json",
        "source_locator": f"page:{page_index}:block:{order_index}",
        "original_type": item_type,
    }

    if item_type == "title":
        text = flatten_mineru_content(content.get("title_content"))
        return {
            **base_block,
            "block_type": "heading",
            "text": text,
            "heading_level": safe_int(content.get("level"), 1),
        }

    if item_type == "paragraph":
        text = flatten_mineru_content(content.get("paragraph_content"))
        if not text:
            text = fallback_flat_text(flat_lookup.get(page_index), bbox, preferred_type={"text"})
        return {
            **base_block,
            "block_type": "paragraph",
            "text": text,
        }

    if item_type == "list":
        list_type = str(content.get("list_type") or "").strip().lower()
        list_items = []
        for list_item in content.get("list_items") or []:
            if not isinstance(list_item, dict):
                continue
            list_items.append(flatten_mineru_content(list_item.get("item_content")))
        return {
            **base_block,
            "block_type": "reference_list" if list_type == "reference_list" else "list",
            "list_type": list_type,
            "list_items": [item for item in list_items if item],
        }

    if item_type == "image":
        image_source = content.get("image_source") or {}
        return {
            **base_block,
            "block_type": "image",
            "image_path": image_source.get("path") or "",
            "caption_text": flatten_mineru_content(content.get("image_caption")),
            "footnote_text": flatten_mineru_content(content.get("image_footnote")),
        }

    if item_type == "table":
        table_payload = extract_table_payload(content)
        return {
            **base_block,
            "block_type": "table",
            "table_html": table_payload["table_html"],
            "table_caption_text": table_payload["table_caption_text"],
            "table_footnote_texts": table_payload["table_footnote_texts"],
            "table_text": table_payload["table_text"],
        }

    if item_type in {"page_header", "page_footer", "page_number"}:
        return {
            **base_block,
            "block_type": item_type,
            "text": flatten_mineru_content(content.values() if isinstance(content, dict) else content),
        }

    fallback_text = flatten_mineru_content(content.values() if isinstance(content, dict) else content)
    if fallback_text:
        return {
            **base_block,
            "block_type": "paragraph",
            "text": fallback_text,
        }

    return None


def build_flat_intermediate_block(item: dict, order_index: int) -> dict | None:
    if not isinstance(item, dict):
        return None

    item_type = str(item.get("type") or "").strip().lower()
    page_index = item.get("page_index") or (safe_int(item.get("page_idx"), 0) + 1)
    text = clean_extracted_text(item.get("text") or "")
    bbox = item.get("normalized_bbox") or []
    base_block = {
        "page_index": page_index,
        "order_index": order_index,
        "bbox": bbox,
        "source_file": "content_list.json",
        "source_locator": f"page:{page_index}:block:{order_index}",
        "original_type": item_type,
    }

    if item_type == "title":
        return {
            **base_block,
            "block_type": "heading",
            "text": text,
            "heading_level": safe_int(item.get("text_level"), 1) or 1,
        }
    if item_type in {"text", "paragraph", "ref_text"}:
        return {
            **base_block,
            "block_type": "paragraph",
            "text": text,
        }
    if item_type in {"image", "figure"}:
        return {
            **base_block,
            "block_type": "image",
            "image_path": item.get("img_path") or "",
            "caption_text": text,
        }
    if item_type == "table":
        table_payload = extract_table_payload(item)
        return {
            **base_block,
            "block_type": "table",
            "table_html": table_payload["table_html"],
            "table_caption_text": table_payload["table_caption_text"],
            "table_footnote_texts": table_payload["table_footnote_texts"],
            "table_text": table_payload["table_text"] or text,
        }
    if item_type in {"header", "footer", "page_number"}:
        type_mapping = {
            "header": "page_header",
            "footer": "page_footer",
            "page_number": "page_number",
        }
        return {
            **base_block,
            "block_type": type_mapping[item_type],
            "text": text,
        }

    if text:
        return {
            **base_block,
            "block_type": "paragraph",
            "text": text,
        }
    return None


def build_markdown_fallback_blocks(markdown_text: str) -> list[dict]:
    blocks: list[dict] = []
    counter = 0
    for block_text in re.split(r"\n{2,}", (markdown_text or "").strip()):
        text = clean_extracted_text(block_text.strip())
        if not text:
            continue
        counter += 1
        is_heading = bool(re.match(r"^#{1,6}\s", block_text.strip()))
        heading_level = len(block_text.strip().split(" ")[0]) if is_heading else None
        if is_heading:
            text = re.sub(r"^#{1,6}\s+", "", text)
        blocks.append({
            "page_index": 1,
            "order_index": counter,
            "bbox": [],
            "source_file": "full.md",
            "source_locator": f"markdown:{counter}",
            "original_type": "markdown_heading" if is_heading else "markdown_paragraph",
            "block_type": "heading" if is_heading else "paragraph",
            "text": text,
            "heading_level": heading_level,
        })
    return blocks


def build_segment(*, segment_id: str, text: str, source_type: str, source_index: int, block_index: int | None,
                  paragraph_index: int | None, metadata: dict) -> dict:
    return {
        "segment_id": segment_id,
        "text": text,
        "source_type": source_type,
        "source_index": source_index,
        "block_index": block_index,
        "paragraph_index": paragraph_index,
        "metadata": metadata,
    }


def build_segment_references(segment: dict, *, kind: str, matches: list[str], targets: dict) -> list[dict]:
    references: list[dict] = []
    seen = set()
    for match in matches:
        label = match.strip()
        reference_key = normalize_reference_key(kind, label)
        if reference_key in seen:
            continue
        seen.add(reference_key)
        target = targets.get(reference_key) or {}
        references.append({
            "reference_id": f"{segment['segment_id']}:{kind}:{label}",
            "source_segment_id": segment["segment_id"],
            "reference_kind": kind,
            "reference_label": label,
            "target_segment_id": target.get("segment_id"),
            "target_asset_id": target.get("asset_id"),
            "normalized_target_key": reference_key,
            "confidence": 1.0 if target else 0.0,
            "resolution_status": "resolved" if target else "unresolved",
            "metadata": {
                "source_index": segment.get("source_index"),
            },
        })
    return references


def flatten_mineru_content(content) -> str:
    parts: list[str] = []

    def visit(value) -> None:
        if value is None:
            return
        if isinstance(value, str):
            stripped = value.strip()
            if stripped:
                parts.append(stripped)
            return
        if isinstance(value, dict):
            content_value = value.get("content")
            if isinstance(content_value, str):
                visit(content_value)
            for nested_key in (
                "text",
                "title_content",
                "paragraph_content",
                "page_header_content",
                "page_footer_content",
                "page_number_content",
                "image_caption",
                "image_footnote",
                "item_content",
            ):
                if nested_key in value:
                    visit(value.get(nested_key))
            return
        if isinstance(value, (list, tuple)):
            for item in value:
                visit(item)

    visit(content)
    return clean_extracted_text(" ".join(parts))


def find_nested_html(content) -> str:
    if isinstance(content, dict):
        for key, value in content.items():
            if key.lower().endswith("html") and isinstance(value, str) and value.strip():
                return value
            nested = find_nested_html(value)
            if nested:
                return nested
    elif isinstance(content, list):
        for item in content:
            nested = find_nested_html(item)
            if nested:
                return nested
    return ""


def extract_table_payload(content) -> dict[str, object]:
    table_html = extract_table_html(content)
    table_caption_text = extract_table_caption_text(content)
    table_footnote_texts = extract_table_footnote_texts(content)
    table_text = build_table_text_from_content(
        content,
        table_html=table_html,
        table_caption_text=table_caption_text,
        table_footnote_texts=table_footnote_texts,
    )
    return {
        "table_html": table_html,
        "table_caption_text": table_caption_text,
        "table_footnote_texts": table_footnote_texts,
        "table_text": table_text,
    }


def extract_table_html(content) -> str:
    if isinstance(content, dict):
        table_body = content.get("table_body")
        if isinstance(table_body, str):
            stripped = table_body.strip()
            if "<table" in stripped.lower():
                return stripped
        html_value = content.get("html")
        if isinstance(html_value, str) and html_value.strip():
            return html_value.strip()
    return find_nested_html(content)


def extract_table_caption_text(content) -> str:
    if not isinstance(content, dict):
        return ""
    return flatten_mineru_content(content.get("table_caption"))


def extract_table_footnote_texts(content) -> list[str]:
    if not isinstance(content, dict):
        return []
    footnote_value = content.get("table_footnote")
    if footnote_value is None:
        return []
    if isinstance(footnote_value, (list, tuple)):
        footnotes = [flatten_mineru_content(item) for item in footnote_value]
        return [footnote for footnote in footnotes if footnote]
    footnote_text = flatten_mineru_content(footnote_value)
    return [footnote_text] if footnote_text else []


def build_table_text_from_content(
    content,
    *,
    table_html: str,
    table_caption_text: str = "",
    table_footnote_texts: list[str] | None = None,
) -> str:
    if table_html:
        markdown = html_table_to_markdown(table_html)
        if markdown:
            parts = []
            if table_caption_text:
                parts.append(table_caption_text)
            parts.append(markdown)
            parts.extend([footnote for footnote in (table_footnote_texts or []) if footnote])
            return "\n".join(parts)

    if isinstance(content, dict):
        for key in ("table_caption", "table_content", "table_text"):
            if key in content:
                flattened = flatten_mineru_content(content[key])
                if flattened:
                    return flattened

    return flatten_mineru_content(content)


def fallback_flat_text(flat_items: list[dict] | None, bbox: list[float], preferred_type: set[str]) -> str:
    if not flat_items or not bbox:
        return ""

    best_text = ""
    best_score = None
    for item in flat_items:
        item_type = str(item.get("type") or "").strip().lower()
        if item_type not in preferred_type:
            continue
        candidate_bbox = item.get("normalized_bbox") or []
        candidate_text = clean_extracted_text(item.get("text") or "")
        if not candidate_text or len(candidate_bbox) != 4:
            continue
        score = bbox_distance(bbox, candidate_bbox)
        if best_score is None or score < best_score:
            best_score = score
            best_text = candidate_text
    return best_text


def bbox_distance(left: list[float], right: list[float]) -> float:
    if len(left) != 4 or len(right) != 4:
        return 9999.0
    left_center = ((left[0] + left[2]) / 2, (left[1] + left[3]) / 2)
    right_center = ((right[0] + right[2]) / 2, (right[1] + right[3]) / 2)
    return abs(left_center[0] - right_center[0]) + abs(left_center[1] - right_center[1])


def _normalize_bbox(bbox, page_dimensions: tuple[float, float] | None) -> list[float]:
    if not bbox or len(bbox) != 4:
        return []
    x0, y0, x1, y1 = [float(value) for value in bbox]
    if x1 <= 1.5 and y1 <= 1.5:
        return [x0, y0, x1, y1]
    if not page_dimensions:
        return [x0, y0, x1, y1]
    page_width, page_height = page_dimensions
    if page_width <= 0 or page_height <= 0:
        return [x0, y0, x1, y1]
    return [
        round(x0 / page_width, 6),
        round(y0 / page_height, 6),
        round(x1 / page_width, 6),
        round(y1 / page_height, 6),
    ]


def _normalize_model_bbox(bbox) -> list[float]:
    if not bbox or len(bbox) != 4:
        return []
    x0, y0, x1, y1 = [float(value) for value in bbox]
    if x1 > 1.5 or y1 > 1.5:
        max_x = max(x0, x1) or 1.0
        max_y = max(y0, y1) or 1.0
        return [
            round(x0 / max_x, 6),
            round(y0 / max_y, 6),
            round(x1 / max_x, 6),
            round(y1 / max_y, 6),
        ]
    return [x0, y0, x1, y1]


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


def normalize_reference_key(kind: str, label: str) -> str:
    raw = str(label or "").strip().lower()
    if kind in {"figure", "table", "section"}:
        number_match = re.search(r"((?:\d+\.)*\d+)", raw)
        if number_match:
            return f"{kind}:{number_match.group(1)}"
    compact = re.sub(r"[^a-z0-9]+", "", raw)
    return f"{kind}:{compact}"


def extract_figure_label(text: str) -> str:
    match = re.search(r"\b(?:fig(?:ure)?\.?\s*\d+)\b", text or "", flags=re.IGNORECASE)
    return match.group(0) if match else ""


def extract_table_label(text: str) -> str:
    match = re.search(r"\btable\s+\d+\b", text or "", flags=re.IGNORECASE)
    return match.group(0) if match else ""


def extract_section_number(text: str) -> str:
    match = _SECTION_NUMBER_PATTERN.match(text or "")
    return match.group(1) if match else ""
