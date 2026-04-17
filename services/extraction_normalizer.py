from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from typing import Any
from uuid import uuid4
from dataclasses import dataclass

from services.extracted_content import (
    BoundingBox,
    DiagramBlock,
    DocumentBlockAsset,
    EMBEDDING_STATUS_READY,
    EXTRACTION_STATUS_PARTIAL,
    EXTRACTION_STATUS_SUCCESS,
    LinkedContext,
    PROCESSING_STATUS_CONTEXT_LINKED,
    PROCESSING_STATUS_NORMALIZED,
    PROCESSING_STATUS_RETRIEVAL_PREPARED,
    SourceLocation,
    TableBlock,
    TextBlock,
)


_ORDERED_LIST_PATTERN = re.compile(r"^\s*(?:\d+[\.\)]|[a-zA-Z][\.\)])\s+")
_UNORDERED_LIST_PATTERN = re.compile(r"^\s*(?:[-*]|\u2022)\s+")
_NUMBERED_HEADING_PATTERN = re.compile(r"^\s*(\d+(?:\.\d+)*)\b")
_FIGURE_HINT_PATTERN = re.compile(r"\b(chart|graph|plot|axis|legend)\b", re.IGNORECASE)
_CAPTION_PREFIX_PATTERN = re.compile(r"^\s*(figure|fig\.|table)\s+\d+", re.IGNORECASE)


@dataclass
class TableParseResult:
    matrix: list[list[str]]
    cells: list[dict[str, Any]]
    merged_cells: list[dict[str, Any]]

def normalize_extraction_result(
    *,
    document_id: str,
    parser_result: dict[str, Any],
    conversation_id: str | None = None,
    parser_version: str | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    pipeline = CanonicalExtractionPipeline(parser_version=parser_version)
    blocks, block_assets, metadata = pipeline.run(
        document_id=str(document_id),
        parser_result=parser_result or {},
        conversation_id=str(conversation_id) if conversation_id else None,
    )
    return (
        [block.to_record() for block in blocks],
        [asset.to_record() for asset in block_assets],
        metadata,
    )


class CanonicalExtractionPipeline:
    def __init__(self, *, parser_version: str | None = None) -> None:
        self.block_normalization_service = BlockNormalizationService(parser_version=parser_version)
        self.context_linking_service = ContextLinkingService()
        self.embedding_preparation_service = EmbeddingPreparationService()

    def run(
        self,
        *,
        document_id: str,
        parser_result: dict[str, Any],
        conversation_id: str | None = None,
    ) -> tuple[list[Any], list[DocumentBlockAsset], dict[str, Any]]:
        blocks, block_assets, metadata = self.block_normalization_service.normalize(
            document_id=document_id,
            parser_result=parser_result,
            conversation_id=conversation_id,
        )
        self.context_linking_service.link(
            blocks=blocks,
            raw_references=parser_result.get("references") or [],
            segment_to_block_id=metadata.get("segment_to_block_id") or {},
            asset_to_block_id=metadata.get("asset_to_block_id") or {},
        )
        self.embedding_preparation_service.prepare(blocks)
        metadata["block_count"] = len(blocks)
        metadata["block_asset_count"] = len(block_assets)
        return blocks, block_assets, metadata


class BlockNormalizationService:
    def __init__(self, *, parser_version: str | None = None) -> None:
        self.parser_version = parser_version
        self.table_normalization_service = TableNormalizationService(parser_version=parser_version)
        self.diagram_preparation_service = DiagramPreparationService(parser_version=parser_version)

    def normalize(
        self,
        *,
        document_id: str,
        parser_result: dict[str, Any],
        conversation_id: str | None = None,
    ) -> tuple[list[Any], list[DocumentBlockAsset], dict[str, Any]]:
        file_type = str(parser_result.get("file_type") or "").strip().lower()
        parser_metadata = parser_result.get("metadata") or {}
        parser_name = _infer_parser_name(parser_metadata)

        segment_to_block_id: dict[str, str] = {}
        asset_to_block_id: dict[str, str] = {}
        duplicate_keys: list[str] = []
        seen_dedupe_keys: set[str] = set()

        table_blocks, consumed_segment_ids = self.table_normalization_service.normalize(
            document_id=document_id,
            conversation_id=conversation_id,
            segments=parser_result.get("segments") or [],
            file_type=file_type,
            parser_name=parser_name,
            parser_metadata=parser_metadata,
        )

        blocks: list[Any] = []
        for table_block in table_blocks:
            dedupe_key = _build_dedupe_key(table_block)
            if dedupe_key in seen_dedupe_keys:
                duplicate_keys.append(dedupe_key)
                continue
            seen_dedupe_keys.add(dedupe_key)
            table_block.dedupe_key = dedupe_key
            blocks.append(table_block)
            raw_segment = table_block.raw_content.get("primary_segment") or {}
            segment_id = str(raw_segment.get("segment_id") or "").strip()
            if segment_id:
                segment_to_block_id[segment_id] = table_block.id
            for grouped_segment in table_block.raw_content.get("segments") or []:
                grouped_segment_id = str(grouped_segment.get("segment_id") or "").strip()
                if grouped_segment_id:
                    segment_to_block_id[grouped_segment_id] = table_block.id

        for segment in parser_result.get("segments") or []:
            segment_id = str(segment.get("segment_id") or "").strip()
            if segment_id and segment_id in consumed_segment_ids:
                continue
            if _is_table_like_segment(segment):
                continue
            text_block = self._normalize_text_block(
                document_id=document_id,
                conversation_id=conversation_id,
                segment=segment,
                file_type=file_type,
                parser_name=parser_name,
                parser_metadata=parser_metadata,
            )
            if not text_block:
                continue
            dedupe_key = _build_dedupe_key(text_block)
            if dedupe_key in seen_dedupe_keys:
                duplicate_keys.append(dedupe_key)
                continue
            seen_dedupe_keys.add(dedupe_key)
            text_block.dedupe_key = dedupe_key
            blocks.append(text_block)
            if segment_id:
                segment_to_block_id[segment_id] = text_block.id

        diagram_blocks, block_assets = self.diagram_preparation_service.normalize(
            document_id=document_id,
            conversation_id=conversation_id,
            assets=parser_result.get("assets") or [],
            segments=parser_result.get("segments") or [],
            file_type=file_type,
            parser_name=parser_name,
            parser_metadata=parser_metadata,
        )
        for diagram_block in diagram_blocks:
            dedupe_key = _build_dedupe_key(diagram_block)
            if dedupe_key in seen_dedupe_keys:
                duplicate_keys.append(dedupe_key)
                continue
            seen_dedupe_keys.add(dedupe_key)
            diagram_block.dedupe_key = dedupe_key
            blocks.append(diagram_block)
            raw_asset = diagram_block.raw_content.get("asset") or {}
            asset_id = str(raw_asset.get("asset_id") or "").strip()
            if asset_id:
                asset_to_block_id[asset_id] = diagram_block.id
            raw_segment = diagram_block.raw_content.get("segment") or {}
            segment_id = str(raw_segment.get("segment_id") or "").strip()
            if segment_id:
                segment_to_block_id[segment_id] = diagram_block.id

        blocks.sort(key=_block_sort_key)
        for block in blocks:
            block.processing_status = PROCESSING_STATUS_NORMALIZED

        metadata = {
            "segment_to_block_id": segment_to_block_id,
            "asset_to_block_id": asset_to_block_id,
            "duplicate_dedupe_keys": duplicate_keys,
            "source_counts": {
                "segments": len(parser_result.get("segments") or []),
                "assets": len(parser_result.get("assets") or []),
                "references": len(parser_result.get("references") or []),
            },
        }
        return blocks, block_assets, metadata

    def _normalize_text_block(
        self,
        *,
        document_id: str,
        conversation_id: str | None,
        segment: dict[str, Any],
        file_type: str,
        parser_name: str,
        parser_metadata: dict[str, Any],
    ) -> TextBlock | None:
        text = _clean_text(segment.get("text"))
        if not text:
            return None

        metadata = segment.get("metadata") or {}
        if segment.get("heading") and "heading" not in metadata:
            metadata = {**metadata, "heading": segment.get("heading")}
        bbox = _bbox_from_value(metadata.get("bbox"))
        reading_order = _segment_reading_order(segment)
        source_unit_type = _infer_source_unit_type(file_type=file_type, raw_source_type=segment.get("source_type"))
        source_unit_index = _safe_int(segment.get("source_index"), 1)
        text_role, subtype = _classify_text_role(segment)
        heading_level = _detect_heading_level(text, metadata)
        list_type = _detect_list_type(text, metadata)
        is_heading = text_role == "heading"
        normalized_text = _normalize_inline_text(text)
        section_path = metadata.get("section_path") or []
        extraction_status = EXTRACTION_STATUS_PARTIAL if parser_metadata.get("warnings") else EXTRACTION_STATUS_SUCCESS

        return TextBlock(
            id=str(uuid4()),
            document_id=document_id,
            conversation_id=conversation_id,
            block_type="text",
            subtype=subtype,
            source_unit_type=source_unit_type,
            source_unit_index=source_unit_index,
            source_location=_build_source_location(
                source_unit_type=source_unit_type,
                source_unit_index=source_unit_index,
                bbox=bbox,
                reading_order=reading_order,
            ),
            reading_order=reading_order,
            bbox=bbox,
            raw_content={"segment": segment},
            normalized_content={
                "section_path": section_path,
                "section_numbers": metadata.get("section_numbers") or [],
                "text_role": text_role,
                "normalized_text": normalized_text,
                "is_heading": is_heading,
                "heading_level": heading_level,
                "list_type": list_type,
            },
            display_text=text,
            caption_text=text if text_role == "caption" else None,
            linked_context=LinkedContext(),
            source_metadata={
                **parser_metadata,
                "source_anchor_key": metadata.get("source_anchor_key"),
                "segment_metadata": metadata,
                "raw_segment_id": segment.get("segment_id"),
            },
            extraction_status=extraction_status,
            confidence=_extract_confidence(metadata),
            parser_name=parser_name,
            parser_version=self.parser_version,
            text_role=text_role,
            text_content=text,
            normalized_text=normalized_text,
            language=None,
            tokens_estimate=_estimate_tokens(normalized_text),
            is_heading=is_heading,
            heading_level=heading_level,
            list_type=list_type,
            parent_section_id=None,
        )


class TableNormalizationService:
    def __init__(self, *, parser_version: str | None = None) -> None:
        self.parser_version = parser_version

    def normalize(
        self,
        *,
        document_id: str,
        conversation_id: str | None,
        segments: list[dict[str, Any]],
        file_type: str,
        parser_name: str,
        parser_metadata: dict[str, Any],
    ) -> tuple[list[TableBlock], set[str]]:
        grouped_docx_rows: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
        table_blocks: list[TableBlock] = []
        consumed_segment_ids: set[str] = set()

        for segment in segments:
            metadata = segment.get("metadata") or {}
            if str(metadata.get("block_type") or "").strip().lower() == "table_row":
                source_index = _safe_int(segment.get("source_index"), 1)
                table_index = _safe_int(metadata.get("table_index"), _safe_int(segment.get("block_index"), 0))
                grouped_docx_rows[(source_index, table_index)].append(segment)
                continue

            if not _is_table_like_segment(segment):
                continue

            table_block = self._normalize_single_table_segment(
                document_id=document_id,
                conversation_id=conversation_id,
                segment=segment,
                file_type=file_type,
                parser_name=parser_name,
                parser_metadata=parser_metadata,
            )
            if not table_block:
                continue
            table_blocks.append(table_block)
            segment_id = str(segment.get("segment_id") or "").strip()
            if segment_id:
                consumed_segment_ids.add(segment_id)

        for group_segments in grouped_docx_rows.values():
            table_block = self._normalize_grouped_table_rows(
                document_id=document_id,
                conversation_id=conversation_id,
                segments=group_segments,
                file_type=file_type,
                parser_name=parser_name,
                parser_metadata=parser_metadata,
            )
            if not table_block:
                continue
            table_blocks.append(table_block)
            for segment in group_segments:
                segment_id = str(segment.get("segment_id") or "").strip()
                if segment_id:
                    consumed_segment_ids.add(segment_id)

        return table_blocks, consumed_segment_ids

    def _normalize_single_table_segment(
        self,
        *,
        document_id: str,
        conversation_id: str | None,
        segment: dict[str, Any],
        file_type: str,
        parser_name: str,
        parser_metadata: dict[str, Any],
    ) -> TableBlock | None:
        metadata = segment.get("metadata") or {}
        raw_text = _clean_text(segment.get("text"))
        table_html = str(metadata.get("table_html") or "").strip()
        explicit_caption = _clean_text(metadata.get("table_caption"))
        explicit_footnotes = _normalize_string_list(metadata.get("table_footnote"))

        caption, matrix, parsed_cells, merged_cells, footnotes = _extract_table_parts(
            raw_text,
            table_html=table_html,
            explicit_caption=explicit_caption or None,
            explicit_footnotes=explicit_footnotes,
        )

        if not raw_text and not matrix:
            return None

        bbox = _bbox_from_value(metadata.get("bbox"))
        reading_order = _segment_reading_order(segment)
        source_unit_type = _infer_source_unit_type(file_type=file_type, raw_source_type=segment.get("source_type"))
        source_unit_index = _safe_int(segment.get("source_index"), 1)

        header_rows, body_rows = _split_header_and_body(matrix)
        row_objects = _build_row_objects(header_rows, body_rows)
        cells = _build_cells(matrix, parsed_cells)

        title = _extract_reference_title(caption or raw_text, prefix="table")
        linearized_text = _linearize_table(
            title=title,
            caption=caption,
            header_rows=header_rows,
            body_rows=body_rows,
            footnotes=footnotes,
            context_lines=[],
        )

        quality = "native" if table_html or matrix else "malformed"

        return TableBlock(
            id=str(uuid4()),
            document_id=document_id,
            conversation_id=conversation_id,
            block_type="table",
            subtype="table",
            source_unit_type=source_unit_type,
            source_unit_index=source_unit_index,
            source_location=_build_source_location(
                source_unit_type=source_unit_type,
                source_unit_index=source_unit_index,
                bbox=bbox,
                reading_order=reading_order,
            ),
            reading_order=reading_order,
            bbox=bbox,
            raw_content={"primary_segment": segment, "segments": [segment]},
            normalized_content={},
            display_text=caption or raw_text,
            caption_text=caption,
            linked_context=LinkedContext(),
            source_metadata={
                **parser_metadata,
                "source_anchor_key": metadata.get("source_anchor_key"),
                "segment_metadata": metadata,
                "raw_segment_id": segment.get("segment_id"),
            },
            extraction_status=EXTRACTION_STATUS_SUCCESS,
            confidence=_extract_confidence(metadata),
            parser_name=parser_name,
            parser_version=self.parser_version,
            table_title=title,
            table_caption=caption,
            header_rows=header_rows,
            body_rows=body_rows,
            footer_rows=[],
            matrix=matrix,
            cells=cells,
            merged_cells=merged_cells,
            footnotes=footnotes,
            nearby_context_block_ids=[],
            table_continuation={},
            linearized_text=linearized_text,
            table_quality=quality,
            schema_version="1.0",
            row_objects=row_objects,
        )

    def _normalize_grouped_table_rows(
        self,
        *,
        document_id: str,
        conversation_id: str | None,
        segments: list[dict[str, Any]],
        file_type: str,
        parser_name: str,
        parser_metadata: dict[str, Any],
    ) -> TableBlock | None:
        ordered_segments = sorted(segments, key=lambda item: _safe_int(item.get("paragraph_index"), 0))
        matrix: list[list[str]] = []
        for segment in ordered_segments:
            row = [cell.strip() for cell in str(segment.get("text") or "").split("|")]
            row = [cell for cell in row if cell]
            if row:
                matrix.append(row)
        if not matrix:
            return None

        first_segment = ordered_segments[0]
        metadata = first_segment.get("metadata") or {}
        bbox = _bbox_from_segments(ordered_segments)
        reading_order = _segment_reading_order(first_segment)
        source_unit_type = _infer_source_unit_type(file_type=file_type, raw_source_type=first_segment.get("source_type"))
        source_unit_index = _safe_int(first_segment.get("source_index"), 1)
        header_rows, body_rows = _split_header_and_body(matrix)
        row_objects = _build_row_objects(header_rows, body_rows)
        cells = _build_cells(matrix, None)
        linearized_text = _linearize_table(
            title=None,
            caption=None,
            header_rows=header_rows,
            body_rows=body_rows,
            footnotes=[],
            context_lines=[],
        )

        return TableBlock(
            id=str(uuid4()),
            document_id=document_id,
            conversation_id=conversation_id,
            block_type="table",
            subtype="table",
            source_unit_type=source_unit_type,
            source_unit_index=source_unit_index,
            source_location=_build_source_location(
                source_unit_type=source_unit_type,
                source_unit_index=source_unit_index,
                bbox=bbox,
                reading_order=reading_order,
            ),
            reading_order=reading_order,
            bbox=bbox,
            raw_content={"primary_segment": first_segment, "segments": ordered_segments},
            normalized_content={},
            display_text=linearized_text,
            caption_text=None,
            linked_context=LinkedContext(),
            source_metadata={
                **parser_metadata,
                "source_anchor_key": metadata.get("source_anchor_key"),
                "segment_metadata": metadata,
                "raw_segment_ids": [segment.get("segment_id") for segment in ordered_segments],
            },
            extraction_status=EXTRACTION_STATUS_SUCCESS,
            confidence=0.7,
            parser_name=parser_name,
            parser_version=self.parser_version,
            table_title=None,
            table_caption=None,
            header_rows=header_rows,
            body_rows=body_rows,
            footer_rows=[],
            matrix=matrix,
            cells=cells,
            merged_cells=[],
            footnotes=[],
            nearby_context_block_ids=[],
            table_continuation={},
            linearized_text=linearized_text,
            table_quality="parsed",
            schema_version="1.0",
            row_objects=row_objects,
        )


class DiagramPreparationService:
    def __init__(self, *, parser_version: str | None = None) -> None:
        self.parser_version = parser_version

    def normalize(
        self,
        *,
        document_id: str,
        conversation_id: str | None,
        assets: list[dict[str, Any]],
        segments: list[dict[str, Any]],
        file_type: str,
        parser_name: str,
        parser_metadata: dict[str, Any],
    ) -> tuple[list[DiagramBlock], list[DocumentBlockAsset]]:
        diagram_blocks: list[DiagramBlock] = []
        block_assets: list[DocumentBlockAsset] = []

        for asset in assets:
            diagram_block, block_asset = self._normalize_asset_diagram(
                document_id=document_id,
                conversation_id=conversation_id,
                asset=asset,
                file_type=file_type,
                parser_name=parser_name,
                parser_metadata=parser_metadata,
            )
            if not diagram_block:
                continue
            diagram_blocks.append(diagram_block)
            if block_asset:
                block_assets.append(block_asset)

        for segment in segments:
            if str(segment.get("source_type") or "").strip().lower() != "image":
                continue
            diagram_block = self._normalize_image_segment(
                document_id=document_id,
                conversation_id=conversation_id,
                segment=segment,
                file_type=file_type,
                parser_name=parser_name,
                parser_metadata=parser_metadata,
            )
            if diagram_block:
                diagram_blocks.append(diagram_block)

        return diagram_blocks, block_assets

    def _normalize_asset_diagram(
        self,
        *,
        document_id: str,
        conversation_id: str | None,
        asset: dict[str, Any],
        file_type: str,
        parser_name: str,
        parser_metadata: dict[str, Any],
    ) -> tuple[DiagramBlock | None, DocumentBlockAsset | None]:
        asset_type = str(asset.get("asset_type") or "").strip().lower()
        if asset_type not in {"image", "figure", "chart"}:
            return None, None

        metadata = asset.get("metadata") or {}
        bbox = _bbox_from_value(asset.get("bbox"))
        reading_order = _safe_int(metadata.get("reading_order"), None)
        source_unit_type = _infer_source_unit_type(file_type=file_type, raw_source_type="image")
        source_unit_index = _safe_int(asset.get("source_index"), 1)
        block_id = str(uuid4())
        block_asset_id = str(uuid4())
        caption_text = _clean_text(metadata.get("caption_text"))
        diagram_kind = _classify_diagram_kind(caption_text or asset_type)
        storage_path = str(asset.get("upload_path") or asset.get("storage_path") or "").strip()

        block = DiagramBlock(
            id=block_id,
            document_id=document_id,
            conversation_id=conversation_id,
            block_type="diagram",
            subtype=diagram_kind,
            source_unit_type=source_unit_type,
            source_unit_index=source_unit_index,
            source_location=_build_source_location(
                source_unit_type=source_unit_type,
                source_unit_index=source_unit_index,
                bbox=bbox,
                reading_order=reading_order,
            ),
            reading_order=reading_order,
            bbox=bbox,
            raw_content={"asset": asset},
            normalized_content={},
            display_text=caption_text or str(asset.get("asset_id") or "diagram"),
            caption_text=caption_text or None,
            linked_context=LinkedContext(),
            source_metadata={
                **parser_metadata,
                "source_anchor_key": metadata.get("source_anchor_key"),
                "asset_metadata": metadata,
                "raw_asset_id": asset.get("asset_id"),
                "asset_storage_path": storage_path,
            },
            extraction_status=EXTRACTION_STATUS_SUCCESS,
            confidence=0.9,
            parser_name=parser_name,
            parser_version=self.parser_version,
            diagram_kind=diagram_kind,
            image_asset_id=block_asset_id,
            image_region={"bbox": bbox.to_dict() if bbox else None},
            ocr_text=None,
            nearby_context_block_ids=[],
            visual_description=None,
            semantic_links=[],
            vision_status="pending_vision_analysis",
            diagram_quality="native" if storage_path else "unknown",
        )
        block_asset = DocumentBlockAsset(
            block_asset_id=block_asset_id,
            block_id=block_id,
            asset_role="diagram_crop",
            storage_path=storage_path,
            mime_type=asset.get("mime_type"),
            byte_size=_safe_int(asset.get("byte_size"), None),
            content_hash=str(asset.get("content_hash") or "").strip() or None,
        )
        return block, block_asset

    def _normalize_image_segment(
        self,
        *,
        document_id: str,
        conversation_id: str | None,
        segment: dict[str, Any],
        file_type: str,
        parser_name: str,
        parser_metadata: dict[str, Any],
    ) -> DiagramBlock | None:
        metadata = segment.get("metadata") or {}
        bbox = _bbox_from_value(metadata.get("bbox"))
        data_uri = str(metadata.get("data_uri") or "").strip()
        if not data_uri:
            return None
        reading_order = _segment_reading_order(segment)
        source_unit_type = _infer_source_unit_type(file_type=file_type, raw_source_type="image")
        source_unit_index = _safe_int(segment.get("source_index"), 1)
        return DiagramBlock(
            id=str(uuid4()),
            document_id=document_id,
            conversation_id=conversation_id,
            block_type="diagram",
            subtype="image",
            source_unit_type=source_unit_type,
            source_unit_index=source_unit_index,
            source_location=_build_source_location(
                source_unit_type=source_unit_type,
                source_unit_index=source_unit_index,
                bbox=bbox,
                reading_order=reading_order,
            ),
            reading_order=reading_order,
            bbox=bbox,
            raw_content={"segment": segment},
            normalized_content={},
            display_text=str(segment.get("segment_id") or "image"),
            caption_text=None,
            linked_context=LinkedContext(),
            source_metadata={
                **parser_metadata,
                "source_anchor_key": metadata.get("source_anchor_key"),
                "segment_metadata": metadata,
                "data_uri_present": True,
            },
            extraction_status=EXTRACTION_STATUS_SUCCESS,
            confidence=0.8,
            parser_name=parser_name,
            parser_version=self.parser_version,
            diagram_kind="image",
            image_asset_id=None,
            image_region={"bbox": bbox.to_dict() if bbox else None},
            ocr_text=None,
            nearby_context_block_ids=[],
            visual_description=None,
            semantic_links=[],
            vision_status="pending_vision_analysis",
            diagram_quality="ocr_only",
        )


class ContextLinkingService:
    def link(
        self,
        *,
        blocks: list[Any],
        raw_references: list[dict[str, Any]],
        segment_to_block_id: dict[str, str],
        asset_to_block_id: dict[str, str],
    ) -> None:
        blocks_by_id = {block.id: block for block in blocks}
        text_blocks = [block for block in blocks if block.block_type == "text"]
        text_blocks.sort(key=_block_sort_key)

        for index, block in enumerate(text_blocks):
            previous_block = text_blocks[index - 1] if index > 0 else None
            next_block = text_blocks[index + 1] if index + 1 < len(text_blocks) else None
            block.linked_context.previous_block_id = previous_block.id if previous_block else None
            block.linked_context.next_block_id = next_block.id if next_block else None

        blocks_by_unit: dict[tuple[str, int], list[Any]] = defaultdict(list)
        for block in blocks:
            key = (block.source_unit_type, block.source_unit_index)
            blocks_by_unit[key].append(block)

        for same_unit_blocks in blocks_by_unit.values():
            same_unit_blocks.sort(key=_block_sort_key)
            same_page_ids = [block.id for block in same_unit_blocks]
            heading_parent_id: str | None = None
            for block in same_unit_blocks:
                block.linked_context.same_page_block_ids = [block_id for block_id in same_page_ids if block_id != block.id]
                block.linked_context.source_anchor = {
                    "source_unit_type": block.source_unit_type,
                    "source_unit_index": block.source_unit_index,
                    "bbox": block.bbox.to_dict() if block.bbox else None,
                    "reading_order": block.reading_order,
                }
                if block.block_type == "text" and block.normalized_content.get("is_heading"):
                    heading_parent_id = block.id
                elif heading_parent_id:
                    block.linked_context.parent_block_id = heading_parent_id
                    if block.block_type == "text":
                        block.parent_section_id = heading_parent_id
                        parent = blocks_by_id.get(heading_parent_id)
                        if parent and block.id not in parent.linked_context.child_block_ids:
                            parent.linked_context.child_block_ids.append(block.id)

            narrative_blocks = [
                block for block in same_unit_blocks
                if block.block_type == "text" and block.normalized_content.get("text_role") in {"heading", "paragraph", "list", "note", "caption"}
            ]
            for block in same_unit_blocks:
                if block.block_type == "text":
                    continue
                nearby_ids = _find_nearby_text_block_ids(block, narrative_blocks)
                block.linked_context.nearby_block_ids = nearby_ids
                block.linked_context.explainer_block_ids = nearby_ids
                if block.block_type == "table":
                    block.nearby_context_block_ids = nearby_ids
                if block.block_type == "diagram":
                    block.nearby_context_block_ids = nearby_ids

                caption_block_id = self._resolve_caption_block_id(
                    block=block,
                    same_unit_blocks=same_unit_blocks,
                    segment_to_block_id=segment_to_block_id,
                )
                if caption_block_id:
                    block.caption_block_id = caption_block_id
                    block.linked_context.caption_block_id = caption_block_id
                    if caption_block_id not in block.linked_context.described_by_block_ids:
                        block.linked_context.described_by_block_ids.append(caption_block_id)
                    caption_block = blocks_by_id.get(caption_block_id)
                    if caption_block and caption_block.display_text:
                        block.caption_text = caption_block.display_text

        for reference in raw_references:
            source_block_id = segment_to_block_id.get(str(reference.get("source_segment_id") or "").strip())
            target_block_id = None
            target_segment_id = str(reference.get("target_segment_id") or "").strip()
            target_asset_id = str(reference.get("target_asset_id") or "").strip()
            if target_segment_id:
                target_block_id = segment_to_block_id.get(target_segment_id)
            if not target_block_id and target_asset_id:
                target_block_id = asset_to_block_id.get(target_asset_id)
            if not source_block_id or not target_block_id or source_block_id == target_block_id:
                continue
            source_block = blocks_by_id.get(source_block_id)
            if not source_block:
                continue
            if target_block_id not in source_block.linked_context.refers_to_block_ids:
                source_block.linked_context.refers_to_block_ids.append(target_block_id)

        for block in blocks:
            block.processing_status = PROCESSING_STATUS_CONTEXT_LINKED

    def _resolve_caption_block_id(
        self,
        *,
        block: Any,
        same_unit_blocks: list[Any],
        segment_to_block_id: dict[str, str],
    ) -> str | None:
        raw_asset = block.raw_content.get("asset") or {}
        caption_segment_id = str(raw_asset.get("caption_segment_id") or "").strip()
        if caption_segment_id:
            return segment_to_block_id.get(caption_segment_id)

        raw_segment = block.raw_content.get("primary_segment") or block.raw_content.get("segment") or {}
        reference_label = str((raw_segment.get("metadata") or {}).get("reference_label") or "").strip()
        if not reference_label:
            return None
        for candidate in same_unit_blocks:
            if candidate.block_type != "text":
                continue
            candidate_text = candidate.display_text or ""
            if candidate_text and candidate_text.lower().startswith(reference_label.lower()):
                return candidate.id
        return None


class EmbeddingPreparationService:
    def prepare(self, blocks: list[Any]) -> None:
        blocks_by_id = {block.id: block for block in blocks}
        for block in blocks:
            retrieval_text = self._build_retrieval_text(block, blocks_by_id)
            if retrieval_text:
                block.normalized_content["retrieval_text"] = retrieval_text
                block.embedding_status = EMBEDDING_STATUS_READY
            block.processing_status = PROCESSING_STATUS_RETRIEVAL_PREPARED

    def _build_retrieval_text(self, block: Any, blocks_by_id: dict[str, Any]) -> str:
        if block.block_type == "text":
            section_path = block.normalized_content.get("section_path") or []
            parts = []
            if section_path:
                parts.append(f"Heading Path: {' > '.join(section_path)}.")
            if block.normalized_content.get("text_role"):
                parts.append(f"Text Role: {block.normalized_content['text_role']}.")
            if block.normalized_content.get("normalized_text"):
                parts.append(block.normalized_content["normalized_text"])
            return " ".join(part for part in parts if part).strip()

        if block.block_type == "table":
            context_lines = [
                (blocks_by_id.get(block_id).display_text or "").strip()
                for block_id in block.linked_context.explainer_block_ids
                if blocks_by_id.get(block_id)
            ]
            retrieval_text = _linearize_table(
                title=block.table_title,
                caption=block.caption_text or block.table_caption,
                header_rows=block.header_rows,
                body_rows=block.body_rows,
                footnotes=block.footnotes,
                context_lines=context_lines,
            )
            block.linearized_text = retrieval_text
            return retrieval_text

        if block.block_type == "diagram":
            lines = []
            if block.caption_text:
                lines.append(f"Caption: {block.caption_text}.")
            if block.ocr_text:
                lines.append(f"OCR: {block.ocr_text}.")
            nearby_lines = [
                (blocks_by_id.get(block_id).display_text or "").strip()
                for block_id in block.linked_context.explainer_block_ids
                if blocks_by_id.get(block_id)
            ]
            if nearby_lines:
                lines.append(f"Nearby Context: {' '.join(item for item in nearby_lines if item)}")
            lines.append("Diagram semantic interpretation pending vision analysis.")
            return " ".join(line for line in lines if line).strip()

        return ""


def _is_table_like_segment(segment: dict[str, Any]) -> bool:
    source_type = str(segment.get("source_type") or "").strip().lower()
    metadata = segment.get("metadata") or {}
    role = str(metadata.get("role") or "").strip().lower()
    block_type = str(metadata.get("block_type") or "").strip().lower()
    return source_type == "table" or role == "table" or block_type == "table_row"


def _classify_text_role(segment: dict[str, Any]) -> tuple[str, str]:
    metadata = segment.get("metadata") or {}
    role = str(metadata.get("role") or "").strip().lower()
    text = _clean_text(segment.get("text"))
    if role == "heading":
        return "heading", "heading"
    heading_style = str(metadata.get("heading") or segment.get("heading") or "").strip().lower()
    if heading_style.startswith("heading"):
        return "heading", "heading"
    if role == "figure_caption":
        return "caption", "caption"
    if role == "list_item" or _detect_list_type(text, metadata) != "none":
        return "list", "list"
    if role == "reference_entry":
        return "note", "note"
    if _CAPTION_PREFIX_PATTERN.match(text):
        return "caption", "caption"
    return "paragraph", "paragraph"


def _detect_heading_level(text: str, metadata: dict[str, Any]) -> int | None:
    explicit_level = _safe_int(metadata.get("heading_level"), None)
    if explicit_level:
        return explicit_level
    docx_style = str(metadata.get("heading") or "").strip().lower()
    if docx_style.startswith("heading"):
        trailing = docx_style.replace("heading", "").strip()
        return _safe_int(trailing, 1) or 1
    match = _NUMBERED_HEADING_PATTERN.match(text or "")
    if not match:
        return None
    return match.group(1).count(".") + 1


def _detect_list_type(text: str, metadata: dict[str, Any]) -> str:
    list_type = str(metadata.get("list_type") or "").strip().lower()
    if list_type in {"ordered", "unordered"}:
        return list_type
    if _ORDERED_LIST_PATTERN.match(text or ""):
        return "ordered"
    if _UNORDERED_LIST_PATTERN.match(text or ""):
        return "unordered"
    return "none"


def _classify_diagram_kind(text: str) -> str:
    cleaned = _clean_text(text).lower()
    if _FIGURE_HINT_PATTERN.search(cleaned):
        return "chart"
    if "image" in cleaned:
        return "image"
    if "figure" in cleaned or "fig." in cleaned:
        return "figure"
    return "figure" if cleaned else "unknown"


def _find_nearby_text_block_ids(block: Any, text_blocks: list[Any]) -> list[str]:
    same_unit_text_blocks = [
        candidate for candidate in text_blocks
        if candidate.source_unit_type == block.source_unit_type and candidate.source_unit_index == block.source_unit_index
    ]
    same_unit_text_blocks.sort(key=_block_sort_key)
    previous_block = None
    next_block = None
    for candidate in same_unit_text_blocks:
        if _block_sort_key(candidate) < _block_sort_key(block):
            previous_block = candidate
            continue
        if _block_sort_key(candidate) > _block_sort_key(block):
            next_block = candidate
            break

    nearby_ids = []
    if previous_block:
        nearby_ids.append(previous_block.id)
    if next_block:
        nearby_ids.append(next_block.id)
    return nearby_ids


def _extract_table_parts(
    raw_text: str,
    *,
    table_html: str,
    explicit_caption: str | None = None,
    explicit_footnotes: list[str] | None = None,
) -> tuple[str | None, list[list[str]], list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    caption = explicit_caption or None
    footnotes: list[str] = list(explicit_footnotes or [])
    matrix: list[list[str]] = []
    cells: list[dict[str, Any]] = []
    merged_cells: list[dict[str, Any]] = []

    if table_html:
        parsed = _parse_html_table(table_html)
        matrix = parsed.matrix
        cells = parsed.cells
        merged_cells = parsed.merged_cells

    lines = [line.strip() for line in (raw_text or "").splitlines() if line.strip()]

    if not matrix:
        table_lines = [line for line in lines if "|" in line]
        if table_lines:
            matrix = _matrix_from_pipe_lines(table_lines)
            non_table_lines = [line for line in lines if line not in table_lines]
            if non_table_lines and not caption:
                caption = non_table_lines[0]
                if not footnotes:
                    footnotes = non_table_lines[1:]
        elif raw_text:
            matrix = [[part.strip() for part in raw_text.split("|") if part.strip()]]
    else:
        caption_candidates = [line for line in lines if "|" not in line]
        if caption_candidates and not caption:
            caption = caption_candidates[0]
            if not footnotes:
                footnotes = caption_candidates[1:]

    if not caption:
        first_line = next((line.strip() for line in lines if line.strip()), "")
        if first_line and "|" not in first_line:
            caption = first_line

    return caption, matrix, cells, merged_cells, footnotes

def _matrix_from_html(html: str) -> list[list[str]]:
    return _parse_html_table(html).matrix

def _parse_html_table(html: str) -> TableParseResult:
    html = (html or "").strip()
    if not html:
        return TableParseResult(matrix=[], cells=[], merged_cells=[])

    try:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")
        trs = soup.find_all("tr")
        if not trs:
            return TableParseResult(matrix=[], cells=[], merged_cells=[])

        # Grid stores expanded visible text for retrieval / row_objects
        grid: list[list[str | None]] = []
        cells: list[dict[str, Any]] = []
        merged_cells: list[dict[str, Any]] = []

        def ensure_size(row_idx: int, col_idx: int) -> None:
            while len(grid) <= row_idx:
                grid.append([])
            while len(grid[row_idx]) <= col_idx:
                grid[row_idx].append(None)

        for row_index, tr in enumerate(trs):
            # ensure current row exists
            ensure_size(row_index, 0)

            # find first available col in this row as we place cells
            col_pointer = 0

            for cell in tr.find_all(["th", "td"]):
                while True:
                    ensure_size(row_index, col_pointer)
                    if grid[row_index][col_pointer] is None:
                        break
                    col_pointer += 1

                text = _normalize_inline_text(cell.get_text(" ", strip=True))
                row_span = int(cell.get("rowspan", 1) or 1)
                col_span = int(cell.get("colspan", 1) or 1)
                is_header = cell.name.lower() == "th" or row_index == 0

                cell_obj = {
                    "row_index": row_index,
                    "col_index": col_pointer,
                    "row_span": row_span,
                    "col_span": col_span,
                    "text": text,
                    "is_header": is_header,
                    "bbox": None,
                }
                cells.append(cell_obj)

                if row_span > 1 or col_span > 1:
                    merged_cells.append({
                        "row_index": row_index,
                        "col_index": col_pointer,
                        "row_span": row_span,
                        "col_span": col_span,
                        "text": text,
                    })

                # Fill expanded matrix with repeated text
                for r in range(row_index, row_index + row_span):
                    for c in range(col_pointer, col_pointer + col_span):
                        ensure_size(r, c)
                        # Repeat text into merged area for normalized matrix
                        grid[r][c] = text if text else ""

                col_pointer += col_span

        # Normalize None -> ""
        max_cols = max((len(row) for row in grid), default=0)
        matrix: list[list[str]] = []
        for row in grid:
            normalized_row = [(cell if cell is not None else "") for cell in row]
            if len(normalized_row) < max_cols:
                normalized_row.extend([""] * (max_cols - len(normalized_row)))
            matrix.append(normalized_row)

        return TableParseResult(
            matrix=matrix,
            cells=cells,
            merged_cells=merged_cells,
        )

    except Exception:
        return TableParseResult(matrix=[], cells=[], merged_cells=[])
    
def _matrix_from_pipe_lines(lines: list[str]) -> list[list[str]]:
    rows: list[list[str]] = []
    for line in lines:
        if re.match(r"^\|\s*[-:]+\s*(\|\s*[-:]+\s*)+\|?$", line):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        cells = [cell for cell in cells if cell]
        if cells:
            rows.append(cells)
    return rows


def _split_header_and_body(matrix: list[list[str]]) -> tuple[list[list[str]], list[list[str]]]:
    if not matrix:
        return [], []
    if len(matrix) == 1:
        return [matrix[0]], []
    return [matrix[0]], matrix[1:]


def _build_cells(
    matrix: list[list[str]],
    parsed_cells: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    if parsed_cells:
        return parsed_cells

    cells: list[dict[str, Any]] = []
    for row_index, row in enumerate(matrix):
        for col_index, text in enumerate(row):
            cells.append({
                "row_index": row_index,
                "col_index": col_index,
                "row_span": 1,
                "col_span": 1,
                "text": text,
                "is_header": row_index == 0,
                "bbox": None,
            })
    return cells


def _build_row_objects(header_rows: list[list[str]], body_rows: list[list[str]]) -> list[dict[str, Any]]:
    headers = header_rows[0][:] if header_rows else []

    # Fill blank headers with previous non-empty header or fallback name
    for i in range(len(headers)):
        if not headers[i]:
            headers[i] = headers[i - 1] if i > 0 and headers[i - 1] else f"column_{i + 1}"

    if not headers:
        return [{"row_index": index, "values": row} for index, row in enumerate(body_rows, start=1)]

    row_objects = []
    for index, row in enumerate(body_rows, start=1):
        values = {}
        for col_index, value in enumerate(row):
            key = headers[col_index] if col_index < len(headers) else f"column_{col_index + 1}"
            values[key] = value
        row_objects.append({
            "row_index": index,
            "values": values,
        })
    return row_objects


def _linearize_table(
    *,
    title: str | None,
    caption: str | None,
    header_rows: list[list[str]],
    body_rows: list[list[str]],
    footnotes: list[str],
    context_lines: list[str],
) -> str:
    parts = []
    if title:
        parts.append(f"Table: {title}.")
    if caption and _normalize_inline_text(caption).lower() != _normalize_inline_text(title).lower():
        parts.append(f"Table: {caption}.")
    if header_rows:
        parts.append(f"Headers: {' | '.join(header_rows[0])}.")
    if body_rows:
        headers = header_rows[0] if header_rows else []
        for row_index, row in enumerate(body_rows, start=1):
            if headers:
                pairs = []
                for col_index, value in enumerate(row):
                    header = headers[col_index] if col_index < len(headers) else f"Column {col_index + 1}"
                    pairs.append(f"{header}={value}")
                parts.append(f"Row {row_index}: {'; '.join(pairs)}.")
            else:
                parts.append(f"Row {row_index}: {' | '.join(row)}.")
    for footnote in footnotes:
        if footnote:
            parts.append(f"Footnote: {footnote}.")
    if context_lines:
        summary = " ".join(line for line in context_lines if line)
        if summary:
            parts.append(f"Context: {summary}")
    return " ".join(parts).strip()


def _extract_reference_title(text: str, *, prefix: str) -> str | None:
    text = _clean_text(text)
    if not text:
        return None
    match = re.search(rf"\b{prefix}\s+\d+\b", text, flags=re.IGNORECASE)
    return match.group(0) if match else None


def _build_dedupe_key(block: Any) -> str:
    bbox = block.bbox.to_dict() if block.bbox else {}
    base_text = block.display_text or block.caption_text or block.normalized_content.get("normalized_text") or ""
    raw_value = "|".join(
        [
            str(block.document_id),
            str(block.block_type),
            str(block.source_unit_type),
            str(block.source_unit_index),
            str(block.reading_order),
            str(bbox),
            _normalize_inline_text(base_text),
        ]
    )
    return hashlib.sha256(raw_value.encode("utf-8")).hexdigest()


def _build_source_location(
    *,
    source_unit_type: str,
    source_unit_index: int,
    bbox: BoundingBox | None,
    reading_order: int | None,
) -> SourceLocation:
    return SourceLocation(
        source_unit_type=source_unit_type,
        source_unit_index=source_unit_index,
        bbox=bbox.to_dict() if bbox else None,
        reading_order=reading_order,
        z_order=None,
    )


def _block_sort_key(block: Any) -> tuple[int, int, int, str]:
    return (
        _safe_int(block.source_unit_index, 0),
        _safe_int(block.reading_order, 999999),
        0 if block.block_type == "text" else 1,
        block.id,
    )


def _safe_int(value: Any, default: int | None = 0) -> int | None:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _bbox_from_value(value: Any) -> BoundingBox | None:
    if isinstance(value, dict):
        return BoundingBox(
            x0=_safe_float(value.get("x0")),
            y0=_safe_float(value.get("y0")),
            x1=_safe_float(value.get("x1")),
            y1=_safe_float(value.get("y1")),
            coordinate_space=str(value.get("coordinate_space") or "normalized_0_1"),
            page_width=_safe_float(value.get("page_width")),
            page_height=_safe_float(value.get("page_height")),
            origin=str(value.get("origin") or "top_left"),
        )
    if isinstance(value, (list, tuple)) and len(value) == 4:
        return BoundingBox(
            x0=_safe_float(value[0]),
            y0=_safe_float(value[1]),
            x1=_safe_float(value[2]),
            y1=_safe_float(value[3]),
        )
    return None


def _bbox_from_segments(segments: list[dict[str, Any]]) -> BoundingBox | None:
    bboxes = []
    for segment in segments:
        bbox = _bbox_from_value((segment.get("metadata") or {}).get("bbox"))
        if bbox and None not in {bbox.x0, bbox.y0, bbox.x1, bbox.y1}:
            bboxes.append(bbox)
    if not bboxes:
        return None
    return BoundingBox(
        x0=min(bbox.x0 for bbox in bboxes if bbox.x0 is not None),
        y0=min(bbox.y0 for bbox in bboxes if bbox.y0 is not None),
        x1=max(bbox.x1 for bbox in bboxes if bbox.x1 is not None),
        y1=max(bbox.y1 for bbox in bboxes if bbox.y1 is not None),
    )


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _segment_reading_order(segment: dict[str, Any]) -> int | None:
    metadata = segment.get("metadata") or {}
    return _safe_int(metadata.get("reading_order"), _safe_int(segment.get("block_index"), _safe_int(segment.get("paragraph_index"), 0)))


def _infer_parser_name(metadata: dict[str, Any]) -> str:
    parser_name = str(metadata.get("parser") or "").strip().lower()
    if "mineru" in parser_name:
        return "mineru"
    if "docx" in parser_name:
        return "docx_python"
    if "ocr" in parser_name:
        return "image_ocr"
    if "text" in parser_name:
        return "text_plain"
    return parser_name or "unknown"


def _infer_source_unit_type(*, file_type: str, raw_source_type: Any) -> str:
    file_type = str(file_type or "").strip().lower()
    if file_type == "pdf":
        return "page"
    if file_type in {"ppt", "pptx"}:
        return "slide"
    if file_type in {"doc", "docx", "txt", "text"}:
        return "document"
    if file_type in {"png", "jpg", "jpeg", "gif", "bmp", "webp"}:
        return "image"
    source_type = str(raw_source_type or "").strip().lower()
    if source_type in {"page", "slide", "image", "document"}:
        return source_type
    return "document"


def _extract_confidence(metadata: dict[str, Any]) -> float | None:
    for key in ("confidence", "classification_confidence"):
        value = metadata.get(key)
        try:
            if value is not None:
                return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _estimate_tokens(text: str) -> int:
    word_count = len((text or "").split())
    return max(1, round(word_count * 1.3)) if word_count else 0


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [_clean_text(item) for item in value if _clean_text(item)]
    text = _clean_text(value)
    return [text] if text else []


def _normalize_inline_text(value: Any) -> str:
    return " ".join(str(value or "").split())
