from datetime import datetime, timezone

from psycopg2.extras import Json

from services.document_block_store import (
    get_document_block_assets,
    get_document_blocks,
    get_diagram_block_details,
    save_document_blocks,
)
from services.extraction_normalizer import normalize_extraction_result
from services.summary_jobs import enqueue_document_summary_job


PARSER_VERSION = "1.0.0"
PARSER_STATUS_PENDING = "pending"
PARSER_STATUS_SUCCESS = "success"
PARSER_STATUS_FAILED = "failed"
PARSER_STATUS_PARTIAL = "partial"


def _relation_exists(cur, relation_name: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (relation_name,))
    return bool(cur.fetchone()[0])


def _normalize_segment(segment):
    return {
        "segment_id": segment.get("segment_id"),
        "text": segment.get("text", ""),
        "source_type": segment.get("source_type"),
        "source_index": segment.get("source_index"),
        "block_index": segment.get("block_index"),
        "paragraph_index": segment.get("paragraph_index"),
        "metadata": segment.get("metadata") or {},
    }


def _normalize_asset(asset):
    return {
        "asset_id": asset.get("asset_id"),
        "asset_type": asset.get("asset_type"),
        "storage_path": asset.get("storage_path", ""),
        "upload_path": asset.get("upload_path", ""),
        "original_zip_path": asset.get("original_zip_path", ""),
        "mime_type": asset.get("mime_type"),
        "byte_size": asset.get("byte_size"),
        "content_hash": asset.get("content_hash"),
        "source_index": asset.get("source_index"),
        "bbox": asset.get("bbox") or [],
        "caption_segment_id": asset.get("caption_segment_id"),
        "metadata": asset.get("metadata") or {},
    }


def _normalize_reference(reference):
    return {
        "reference_id": reference.get("reference_id"),
        "source_segment_id": reference.get("source_segment_id"),
        "reference_kind": reference.get("reference_kind"),
        "reference_label": reference.get("reference_label", ""),
        "target_segment_id": reference.get("target_segment_id"),
        "target_asset_id": reference.get("target_asset_id"),
        "normalized_target_key": reference.get("normalized_target_key", ""),
        "confidence": reference.get("confidence"),
        "resolution_status": reference.get("resolution_status"),
        "metadata": reference.get("metadata") or {},
    }


def build_pending_extraction_payload(document_id, parser_version=PARSER_VERSION):
    return {
        "document_id": str(document_id),
        "parser_status": PARSER_STATUS_PENDING,
        "parser_version": parser_version,
        "extraction_timestamp": None,
        "file_type": None,
        "metadata": {},
        "errors": [],
        "segments": [],
        "assets": [],
        "references": [],
        "document_blocks": [],
        "block_assets": [],
    }


def build_extraction_payload(document_id, parser_result, parser_version=PARSER_VERSION, conversation_id=None):
    parser_errors = parser_result.get("errors", [])
    has_segments = bool(parser_result.get("segments"))
    has_assets = bool(parser_result.get("assets"))
    parser_status = (
        PARSER_STATUS_SUCCESS if (has_segments or has_assets) and not parser_errors
        else PARSER_STATUS_FAILED if not (has_segments or has_assets)
        else PARSER_STATUS_PARTIAL
    )
    extraction_timestamp = datetime.now(timezone.utc)
    document_blocks, block_assets, canonical_metadata = normalize_extraction_result(
        document_id=str(document_id),
        parser_result=parser_result,
        conversation_id=conversation_id,
        parser_version=parser_version,
    )
    metadata = parser_result.get("metadata") or {}
    metadata = {
        **metadata,
        "canonical": {
            **(metadata.get("canonical") or {}),
            **canonical_metadata,
        },
    }

    return {
        "document_id": str(document_id),
        "parser_status": parser_status,
        "parser_version": parser_version,
        "extraction_timestamp": extraction_timestamp.isoformat(),
        "file_type": parser_result.get("file_type"),
        "metadata": metadata,
        "errors": parser_errors,
        "segments": [_normalize_segment(segment) for segment in parser_result.get("segments", [])],
        "assets": [_normalize_asset(asset) for asset in parser_result.get("assets", [])],
        "references": [_normalize_reference(reference) for reference in parser_result.get("references", [])],
        "document_blocks": document_blocks,
        "block_assets": block_assets,
    }


def save_document_extraction(cur, document_id, extraction_payload):
    extraction_timestamp = extraction_payload.get("extraction_timestamp")
    if extraction_timestamp:
        extraction_timestamp = datetime.fromisoformat(extraction_timestamp)
    parser_status = extraction_payload.get("parser_status")
    if parser_status == PARSER_STATUS_PARTIAL:
        # The original schema only allows pending/success/failed.
        # Persist partial parses as success while keeping errors in JSON payload.
        parser_status = PARSER_STATUS_SUCCESS

    cur.execute(
        """
        INSERT INTO document_extractions (
            document_id,
            parser_status,
            parser_version,
            extraction_timestamp,
            file_type,
            metadata,
            errors
        )
        VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)
        ON CONFLICT (document_id)
        DO UPDATE SET
            parser_status = EXCLUDED.parser_status,
            parser_version = EXCLUDED.parser_version,
            extraction_timestamp = EXCLUDED.extraction_timestamp,
            file_type = EXCLUDED.file_type,
            metadata = EXCLUDED.metadata,
            errors = EXCLUDED.errors,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            document_id,
            parser_status,
            extraction_payload.get("parser_version"),
            extraction_timestamp,
            extraction_payload.get("file_type"),
            Json(extraction_payload.get("metadata") or {}),
            Json(extraction_payload.get("errors") or []),
        ),
    )

    cur.execute(
        """
        DELETE FROM document_extraction_segments
        WHERE document_id = %s
        """,
        (document_id,),
    )
    has_assets_table = _relation_exists(cur, "document_extraction_assets")
    has_references_table = _relation_exists(cur, "document_extraction_references")
    if has_assets_table:
        cur.execute(
            """
            DELETE FROM document_extraction_assets
            WHERE document_id = %s
            """,
            (document_id,),
        )
    if has_references_table:
        cur.execute(
            """
            DELETE FROM document_extraction_references
            WHERE document_id = %s
            """,
            (document_id,),
        )

    for segment_index, segment in enumerate(extraction_payload.get("segments") or []):
        cur.execute(
            """
            INSERT INTO document_extraction_segments (
                document_id,
                segment_index,
                segment_id,
                text,
                source_type,
                source_index,
                block_index,
                paragraph_index,
                metadata
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                document_id,
                segment_index,
                segment.get("segment_id"),
                segment.get("text", ""),
                segment.get("source_type"),
                segment.get("source_index"),
                segment.get("block_index"),
                segment.get("paragraph_index"),
                Json(segment.get("metadata") or {}),
            ),
        )

    for asset_index, asset in enumerate(extraction_payload.get("assets") or []):
        if not has_assets_table:
            break
        cur.execute(
            """
            INSERT INTO document_extraction_assets (
                document_id,
                asset_index,
                asset_id,
                asset_type,
                storage_path,
                upload_path,
                original_zip_path,
                mime_type,
                byte_size,
                content_hash,
                source_index,
                bbox,
                caption_segment_id,
                metadata
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s::jsonb)
            """,
            (
                document_id,
                asset_index,
                asset.get("asset_id"),
                asset.get("asset_type"),
                asset.get("storage_path", ""),
                asset.get("upload_path", ""),
                asset.get("original_zip_path", ""),
                asset.get("mime_type"),
                asset.get("byte_size"),
                asset.get("content_hash"),
                asset.get("source_index"),
                Json(asset.get("bbox") or []),
                asset.get("caption_segment_id"),
                Json(asset.get("metadata") or {}),
            ),
        )

    for reference_index, reference in enumerate(extraction_payload.get("references") or []):
        if not has_references_table:
            break
        cur.execute(
            """
            INSERT INTO document_extraction_references (
                document_id,
                reference_index,
                reference_id,
                source_segment_id,
                reference_kind,
                reference_label,
                target_segment_id,
                target_asset_id,
                normalized_target_key,
                confidence,
                resolution_status,
                metadata
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                document_id,
                reference_index,
                reference.get("reference_id"),
                reference.get("source_segment_id"),
                reference.get("reference_kind"),
                reference.get("reference_label", ""),
                reference.get("target_segment_id"),
                reference.get("target_asset_id"),
                reference.get("normalized_target_key", ""),
                reference.get("confidence"),
                reference.get("resolution_status"),
                Json(reference.get("metadata") or {}),
            ),
        )

    document_blocks = extraction_payload.get("document_blocks") or []
    save_document_blocks(
        cur,
        document_id=document_id,
        document_blocks=document_blocks,
        block_assets=extraction_payload.get("block_assets") or [],
    )

    enqueue_document_summary_job(
        cur,
        document_id=str(document_id),
        conversation_id=(
            str(document_blocks[0].get("conversation_id"))
            if document_blocks and document_blocks[0].get("conversation_id")
            else None
        ),
        parser_version=extraction_payload.get("parser_version"),
        document_blocks=document_blocks,
    )


def _serialize_extraction_row(row):
    return {
        "document_id": str(row[0]),
        "parser_status": row[1],
        "parser_version": row[2],
        "extraction_timestamp": row[3].isoformat() if row[3] else None,
        "file_type": row[4],
        "metadata": row[5] or {},
        "errors": row[6] or [],
    }


def _serialize_extraction_segment_row(row):
    return {
        "segment_index": row[0],
        "segment_id": row[1],
        "text": row[2] or "",
        "source_type": row[3],
        "source_index": row[4],
        "block_index": row[5],
        "paragraph_index": row[6],
        "metadata": row[7] or {},
    }


def _serialize_extraction_asset_row(row):
    return {
        "asset_index": row[0],
        "asset_id": row[1],
        "asset_type": row[2],
        "storage_path": row[3] or "",
        "upload_path": row[4] or "",
        "original_zip_path": row[5] or "",
        "mime_type": row[6],
        "byte_size": row[7],
        "content_hash": row[8],
        "source_index": row[9],
        "bbox": row[10] or [],
        "caption_segment_id": row[11],
        "metadata": row[12] or {},
    }


def _serialize_extraction_reference_row(row):
    return {
        "reference_index": row[0],
        "reference_id": row[1],
        "source_segment_id": row[2],
        "reference_kind": row[3],
        "reference_label": row[4] or "",
        "target_segment_id": row[5],
        "target_asset_id": row[6],
        "normalized_target_key": row[7] or "",
        "confidence": float(row[8]) if row[8] is not None else None,
        "resolution_status": row[9],
        "metadata": row[10] or {},
    }


def get_document_extraction(cur, document_id, conversation_id=None):
    conversation_filter = ""
    query_params = [document_id]
    if conversation_id:
        conversation_filter = """
            AND EXISTS (
                SELECT 1
                FROM conversation_documents cd
                WHERE cd.document_id = de.document_id
                  AND cd.conversation_id = %s
            )
        """
        query_params.append(conversation_id)

    cur.execute(
        f"""
        SELECT
            de.document_id,
            de.parser_status,
            de.parser_version,
            de.extraction_timestamp,
            de.file_type,
            de.metadata,
            de.errors
        FROM document_extractions de
        WHERE de.document_id = %s
        {conversation_filter}
        """,
        tuple(query_params),
    )
    extraction_row = cur.fetchone()
    if not extraction_row:
        return None

    cur.execute(
        """
        SELECT
            segment_index,
            segment_id,
            text,
            source_type,
            source_index,
            block_index,
            paragraph_index,
            metadata
        FROM document_extraction_segments
        WHERE document_id = %s
        ORDER BY segment_index ASC
        """,
        (document_id,),
    )
    segment_rows = cur.fetchall()

    asset_rows = []
    reference_rows = []
    if _relation_exists(cur, "document_extraction_assets"):
        cur.execute(
            """
            SELECT
                asset_index,
                asset_id,
                asset_type,
                storage_path,
                upload_path,
                original_zip_path,
                mime_type,
                byte_size,
                content_hash,
                source_index,
                bbox,
                caption_segment_id,
                metadata
            FROM document_extraction_assets
            WHERE document_id = %s
            ORDER BY asset_index ASC
            """,
            (document_id,),
        )
        asset_rows = cur.fetchall()

    if _relation_exists(cur, "document_extraction_references"):
        cur.execute(
            """
            SELECT
                reference_index,
                reference_id,
                source_segment_id,
                reference_kind,
                reference_label,
                target_segment_id,
                target_asset_id,
                normalized_target_key,
                confidence,
                resolution_status,
                metadata
            FROM document_extraction_references
            WHERE document_id = %s
            ORDER BY reference_index ASC
            """,
            (document_id,),
        )
        reference_rows = cur.fetchall()

    extraction_payload = _serialize_extraction_row(extraction_row)
    extraction_payload["segments"] = [_serialize_extraction_segment_row(row) for row in segment_rows]
    extraction_payload["assets"] = [_serialize_extraction_asset_row(row) for row in asset_rows]
    extraction_payload["references"] = [_serialize_extraction_reference_row(row) for row in reference_rows]
    extraction_payload["document_blocks"] = get_document_blocks(cur, document_id=document_id)
    extraction_payload["block_assets"] = get_document_block_assets(cur, document_id=document_id)
    extraction_payload["diagram_block_details"] = get_diagram_block_details(cur, document_id=document_id)
    return extraction_payload


def get_conversation_extractions(cur, conversation_id):
    cur.execute(
        """
        SELECT d.document_id
        FROM conversation_documents cd
        JOIN documents d
            ON d.document_id = cd.document_id
        WHERE cd.conversation_id = %s
          AND d.is_deleted = FALSE
        ORDER BY cd.added_at DESC
        """,
        (conversation_id,),
    )
    document_rows = cur.fetchall()

    extraction_results = []
    for document_row in document_rows:
        extraction_payload = get_document_extraction(
            cur,
            document_id=document_row[0],
            conversation_id=conversation_id,
        )
        if extraction_payload:
            extraction_results.append(extraction_payload)

    return extraction_results
