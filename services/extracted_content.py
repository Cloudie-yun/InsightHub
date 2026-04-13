from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal


BlockType = Literal["text", "table", "diagram"]
SourceUnitType = Literal["page", "slide", "image", "document"]

EXTRACTION_STATUS_PENDING = "pending"
EXTRACTION_STATUS_SUCCESS = "success"
EXTRACTION_STATUS_PARTIAL = "partial"
EXTRACTION_STATUS_FAILED = "failed"

EMBEDDING_STATUS_NOT_READY = "not_ready"
EMBEDDING_STATUS_READY = "ready"
EMBEDDING_STATUS_EMBEDDED = "embedded"
EMBEDDING_STATUS_FAILED = "failed"

PROCESSING_STATUS_RAW = "raw"
PROCESSING_STATUS_NORMALIZED = "normalized"
PROCESSING_STATUS_CONTEXT_LINKED = "context_linked"
PROCESSING_STATUS_RETRIEVAL_PREPARED = "retrieval_prepared"
PROCESSING_STATUS_FINALIZED = "finalized"

VISION_STATUS_PENDING = "pending_vision_analysis"
VISION_STATUS_PROCESSED = "processed"
VISION_STATUS_FAILED = "failed"


@dataclass
class BoundingBox:
    x0: float | None = None
    y0: float | None = None
    x1: float | None = None
    y1: float | None = None
    coordinate_space: str = "normalized_0_1"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class SourceLocation:
    source_unit_type: SourceUnitType
    source_unit_index: int
    bbox: dict[str, Any] | None = None
    reading_order: int | None = None
    z_order: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_unit_type": self.source_unit_type,
            "source_unit_index": self.source_unit_index,
            "bbox": self.bbox,
            "reading_order": self.reading_order,
            "z_order": self.z_order,
        }


@dataclass
class LinkedContext:
    previous_block_id: str | None = None
    next_block_id: str | None = None
    parent_block_id: str | None = None
    child_block_ids: list[str] = field(default_factory=list)
    caption_block_id: str | None = None
    described_by_block_ids: list[str] = field(default_factory=list)
    refers_to_block_ids: list[str] = field(default_factory=list)
    nearby_block_ids: list[str] = field(default_factory=list)
    same_page_block_ids: list[str] = field(default_factory=list)
    explainer_block_ids: list[str] = field(default_factory=list)
    source_anchor: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class BaseExtractedBlock:
    id: str
    document_id: str
    conversation_id: str | None
    block_type: BlockType
    subtype: str | None
    source_unit_type: SourceUnitType
    source_unit_index: int
    source_location: SourceLocation
    reading_order: int | None = None
    bbox: BoundingBox | None = None
    raw_content: dict[str, Any] = field(default_factory=dict)
    normalized_content: dict[str, Any] = field(default_factory=dict)
    display_text: str | None = None
    caption_text: str | None = None
    caption_block_id: str | None = None
    linked_context: LinkedContext = field(default_factory=LinkedContext)
    source_metadata: dict[str, Any] = field(default_factory=dict)
    extraction_status: str = EXTRACTION_STATUS_SUCCESS
    embedding_status: str = EMBEDDING_STATUS_NOT_READY
    processing_status: str = PROCESSING_STATUS_RAW
    confidence: float | None = None
    dedupe_key: str | None = None
    parser_name: str | None = None
    parser_version: str | None = None
    created_at: str | None = None
    updated_at: str | None = None

    def to_record(self) -> dict[str, Any]:
        return {
            "block_id": self.id,
            "document_id": self.document_id,
            "conversation_id": self.conversation_id,
            "block_type": self.block_type,
            "subtype": self.subtype,
            "source_unit_type": self.source_unit_type,
            "source_unit_index": self.source_unit_index,
            "reading_order": self.reading_order,
            "source_location": self.source_location.to_dict(),
            "bbox": self.bbox.to_dict() if self.bbox else None,
            "raw_content": self.raw_content,
            "normalized_content": self.normalized_content,
            "display_text": self.display_text,
            "caption_text": self.caption_text,
            "caption_block_id": self.caption_block_id,
            "source_metadata": self.source_metadata,
            "linked_context": self.linked_context.to_dict(),
            "confidence": self.confidence,
            "extraction_status": self.extraction_status,
            "embedding_status": self.embedding_status,
            "processing_status": self.processing_status,
            "parser_name": self.parser_name,
            "parser_version": self.parser_version,
            "dedupe_key": self.dedupe_key,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


@dataclass
class TextBlock(BaseExtractedBlock):
    text_role: str = "paragraph"
    text_content: str = ""
    normalized_text: str = ""
    language: str | None = None
    tokens_estimate: int | None = None
    is_heading: bool = False
    heading_level: int | None = None
    list_type: str | None = None
    parent_section_id: str | None = None

    def to_record(self) -> dict[str, Any]:
        self.normalized_content = {
            **self.normalized_content,
            "text_role": self.text_role,
            "text_content": self.text_content,
            "normalized_text": self.normalized_text,
            "language": self.language,
            "tokens_estimate": self.tokens_estimate,
            "is_heading": self.is_heading,
            "heading_level": self.heading_level,
            "list_type": self.list_type,
            "parent_section_id": self.parent_section_id,
            "previous_block_id": self.linked_context.previous_block_id,
            "next_block_id": self.linked_context.next_block_id,
        }
        return super().to_record()

@dataclass
class TableBlock(BaseExtractedBlock):
    table_title: str | None = None
    table_caption: str | None = None
    header_rows: list[list[str]] = field(default_factory=list)
    body_rows: list[list[str]] = field(default_factory=list)
    footer_rows: list[list[str]] = field(default_factory=list)
    matrix: list[list[str]] = field(default_factory=list)
    cells: list[dict[str, Any]] = field(default_factory=list)
    merged_cells: list[dict[str, Any]] = field(default_factory=list)
    footnotes: list[str] = field(default_factory=list)
    nearby_context_block_ids: list[str] = field(default_factory=list)
    table_continuation: dict[str, Any] = field(default_factory=dict)
    linearized_text: str = ""
    table_quality: str = "native"
    schema_version: str = "1.0"
    row_objects: list[dict[str, Any]] = field(default_factory=list)

    def to_record(self) -> dict[str, Any]:
        self.normalized_content = {
            **self.normalized_content,
            "title": self.table_title,
            "caption": self.table_caption,
            "header_rows": self.header_rows,
            "body_rows": self.body_rows,
            "footer_rows": self.footer_rows,
            "matrix": self.matrix,
            "cells": self.cells,
            "merged_cells": self.merged_cells,
            "footnotes": self.footnotes,
            "nearby_context_block_ids": self.nearby_context_block_ids,
            "table_continuation": self.table_continuation,
            "linearized_text": self.linearized_text,
            "table_quality": self.table_quality,
            "schema_version": self.schema_version,
            "row_objects": self.row_objects,
        }
        return super().to_record()


@dataclass
class DiagramBlock(BaseExtractedBlock):
    diagram_kind: str = "figure"
    image_asset_id: str | None = None
    image_region: dict[str, Any] = field(default_factory=dict)
    ocr_text: str | None = None
    nearby_context_block_ids: list[str] = field(default_factory=list)
    visual_description: str | None = None
    semantic_links: list[dict[str, Any]] = field(default_factory=list)
    vision_status: str = VISION_STATUS_PENDING
    diagram_quality: str = "unknown"

    def to_record(self) -> dict[str, Any]:
        self.normalized_content = {
            **self.normalized_content,
            "diagram_kind": self.diagram_kind,
            "image_asset_id": self.image_asset_id,
            "image_region": self.image_region,
            "ocr_text": self.ocr_text,
            "nearby_context_block_ids": self.nearby_context_block_ids,
            "visual_description": self.visual_description,
            "semantic_links": self.semantic_links,
            "vision_status": self.vision_status,
            "diagram_quality": self.diagram_quality,
        }
        return super().to_record()


@dataclass
class DocumentBlockAsset:
    block_asset_id: str
    block_id: str
    asset_role: str
    storage_path: str
    mime_type: str | None = None
    byte_size: int | None = None
    content_hash: str | None = None
    created_at: str | None = None

    def to_record(self) -> dict[str, Any]:
        return asdict(self)
