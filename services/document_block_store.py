from __future__ import annotations

from datetime import datetime

from psycopg2.extras import Json


def _relation_exists(cur, relation_name: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (relation_name,))
    return bool(cur.fetchone()[0])


def save_document_blocks(cur, document_id, document_blocks, block_assets) -> None:
    if not _relation_exists(cur, "document_blocks"):
        return

    has_block_assets_table = _relation_exists(cur, "document_block_assets")
    if has_block_assets_table:
        cur.execute(
            """
            DELETE FROM document_block_assets
            WHERE block_id IN (
                SELECT block_id
                FROM document_blocks
                WHERE document_id = %s
            )
            """,
            (document_id,),
        )
    cur.execute(
        """
        DELETE FROM document_blocks
        WHERE document_id = %s
        """,
        (document_id,),
    )

    for block in document_blocks or []:
        cur.execute(
            """
            INSERT INTO document_blocks (
                block_id,
                document_id,
                conversation_id,
                block_type,
                subtype,
                source_unit_type,
                source_unit_index,
                reading_order,
                source_location,
                bbox,
                raw_content,
                normalized_content,
                display_text,
                caption_text,
                caption_block_id,
                source_metadata,
                linked_context,
                confidence,
                extraction_status,
                embedding_status,
                processing_status,
                parser_name,
                parser_version,
                dedupe_key,
                created_at,
                updated_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb,
                %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s, %s, %s,
                COALESCE(%s, CURRENT_TIMESTAMP),
                COALESCE(%s, CURRENT_TIMESTAMP)
            )
            """,
            (
                block.get("block_id"),
                document_id,
                block.get("conversation_id"),
                block.get("block_type"),
                block.get("subtype"),
                block.get("source_unit_type"),
                block.get("source_unit_index"),
                block.get("reading_order"),
                Json(block.get("source_location") or {}),
                Json(block.get("bbox") or {}),
                Json(block.get("raw_content") or {}),
                Json(block.get("normalized_content") or {}),
                block.get("display_text"),
                block.get("caption_text"),
                block.get("caption_block_id"),
                Json(block.get("source_metadata") or {}),
                Json(block.get("linked_context") or {}),
                block.get("confidence"),
                block.get("extraction_status"),
                block.get("embedding_status"),
                block.get("processing_status"),
                block.get("parser_name"),
                block.get("parser_version"),
                block.get("dedupe_key"),
                _parse_timestamp(block.get("created_at")),
                _parse_timestamp(block.get("updated_at")),
            ),
        )

    if not has_block_assets_table:
        return

    for block_asset in block_assets or []:
        cur.execute(
            """
            INSERT INTO document_block_assets (
                block_asset_id,
                block_id,
                asset_role,
                storage_path,
                mime_type,
                byte_size,
                content_hash,
                created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, COALESCE(%s, CURRENT_TIMESTAMP))
            """,
            (
                block_asset.get("block_asset_id"),
                block_asset.get("block_id"),
                block_asset.get("asset_role"),
                block_asset.get("storage_path") or "",
                block_asset.get("mime_type"),
                block_asset.get("byte_size"),
                block_asset.get("content_hash"),
                _parse_timestamp(block_asset.get("created_at")),
            ),
        )


def get_document_blocks(cur, document_id) -> list[dict]:
    if not _relation_exists(cur, "document_blocks"):
        return []

    cur.execute(
        """
        SELECT
            block_id,
            document_id,
            conversation_id,
            block_type,
            subtype,
            source_unit_type,
            source_unit_index,
            reading_order,
            source_location,
            bbox,
            raw_content,
            normalized_content,
            display_text,
            caption_text,
            caption_block_id,
            source_metadata,
            linked_context,
            confidence,
            extraction_status,
            embedding_status,
            processing_status,
            parser_name,
            parser_version,
            dedupe_key,
            created_at,
            updated_at
        FROM document_blocks
        WHERE document_id = %s
        ORDER BY source_unit_index ASC, reading_order ASC NULLS LAST, created_at ASC
        """,
        (document_id,),
    )
    rows = cur.fetchall()
    return [_serialize_document_block_row(row) for row in rows]


def get_document_block_assets(cur, document_id) -> list[dict]:
    if not _relation_exists(cur, "document_block_assets") or not _relation_exists(cur, "document_blocks"):
        return []

    cur.execute(
        """
        SELECT
            dba.block_asset_id,
            dba.block_id,
            dba.asset_role,
            dba.storage_path,
            dba.mime_type,
            dba.byte_size,
            dba.content_hash,
            dba.created_at
        FROM document_block_assets dba
        JOIN document_blocks db
          ON db.block_id = dba.block_id
        WHERE db.document_id = %s
        ORDER BY dba.created_at ASC, dba.block_asset_id ASC
        """,
        (document_id,),
    )
    rows = cur.fetchall()
    return [_serialize_document_block_asset_row(row) for row in rows]


def _serialize_document_block_row(row) -> dict:
    return {
        "block_id": str(row[0]),
        "document_id": str(row[1]),
        "conversation_id": str(row[2]) if row[2] else None,
        "block_type": row[3],
        "subtype": row[4],
        "source_unit_type": row[5],
        "source_unit_index": row[6],
        "reading_order": row[7],
        "source_location": row[8] or {},
        "bbox": row[9] or {},
        "raw_content": row[10] or {},
        "normalized_content": row[11] or {},
        "display_text": row[12],
        "caption_text": row[13],
        "caption_block_id": str(row[14]) if row[14] else None,
        "source_metadata": row[15] or {},
        "linked_context": row[16] or {},
        "confidence": float(row[17]) if row[17] is not None else None,
        "extraction_status": row[18],
        "embedding_status": row[19],
        "processing_status": row[20],
        "parser_name": row[21],
        "parser_version": row[22],
        "dedupe_key": row[23],
        "created_at": row[24].isoformat() if row[24] else None,
        "updated_at": row[25].isoformat() if row[25] else None,
    }


def _serialize_document_block_asset_row(row) -> dict:
    return {
        "block_asset_id": str(row[0]),
        "block_id": str(row[1]),
        "asset_role": row[2],
        "storage_path": row[3] or "",
        "mime_type": row[4],
        "byte_size": row[5],
        "content_hash": row[6],
        "created_at": row[7].isoformat() if row[7] else None,
    }



def get_diagram_block_details(cur, document_id) -> list[dict]:
    if not _relation_exists(cur, "diagram_block_details") or not _relation_exists(cur, "document_blocks"):
        return []

    cur.execute(
        """
        SELECT
            db.block_id,
            db.caption_text,
            dbd.visual_description,
            dbd.ocr_text,
            dbd.semantic_links,
            dbd.question_answerable_facts,
            dba.storage_path,
            dbd.vision_status,
            dbd.vision_confidence,
            dbd.diagram_kind,
            dbd.last_analyzed_at,
            dbd.vision_gate_score,
            dbd.vision_gate_reasons
        FROM document_blocks db
        JOIN diagram_block_details dbd
          ON dbd.block_id = db.block_id
        LEFT JOIN document_block_assets dba
          ON dba.block_asset_id = dbd.image_asset_id
        WHERE db.document_id = %s
          AND db.block_type = 'diagram'
        ORDER BY db.source_unit_index ASC, db.reading_order ASC NULLS LAST
        """,
        (document_id,),
    )

    rows = cur.fetchall()
    result = []
    for row in rows:
        result.append(
            {
                "block_id": str(row[0]),
                "caption_text": row[1],
                "visual_description": row[2],
                "ocr_text": row[3] or [],
                "semantic_links": row[4] or [],
                "question_answerable_facts": row[5] or [],
                "storage_path": row[6] or "",
                "vision_status": row[7],
                "vision_confidence": float(row[8]) if row[8] is not None else None,
                "diagram_kind": row[9],
                "last_analyzed_at": row[10].isoformat() if row[10] else None,
                "vision_gate_score": float(row[11]) if row[11] is not None else None,
                "vision_gate_reasons": row[12] or [],
            }
        )
    return result

def _parse_timestamp(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))
