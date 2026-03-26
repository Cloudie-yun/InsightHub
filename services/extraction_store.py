from datetime import datetime, timezone

from psycopg2.extras import Json


PARSER_VERSION = "1.0.0"
PARSER_STATUS_PENDING = "pending"
PARSER_STATUS_SUCCESS = "success"
PARSER_STATUS_FAILED = "failed"


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
    }


def build_extraction_payload(document_id, parser_result, parser_version=PARSER_VERSION):
    parser_errors = parser_result.get("errors", [])
    has_segments = bool(parser_result.get("segments"))
    parser_status = (
        PARSER_STATUS_SUCCESS if has_segments and not parser_errors
        else PARSER_STATUS_FAILED if not has_segments
        else "partial"  # or add a PARSER_STATUS_PARTIAL constant
    )
    extraction_timestamp = datetime.now(timezone.utc)

    return {
        "document_id": str(document_id),
        "parser_status": parser_status,
        "parser_version": parser_version,
        "extraction_timestamp": extraction_timestamp.isoformat(),
        "file_type": parser_result.get("file_type"),
        "metadata": parser_result.get("metadata") or {},
        "errors": parser_errors,
        "segments": [_normalize_segment(segment) for segment in parser_result.get("segments", [])],
    }


def save_document_extraction(cur, document_id, extraction_payload):
    extraction_timestamp = extraction_payload.get("extraction_timestamp")
    if extraction_timestamp:
        extraction_timestamp = datetime.fromisoformat(extraction_timestamp)

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
            extraction_payload.get("parser_status"),
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

    extraction_payload = _serialize_extraction_row(extraction_row)
    extraction_payload["segments"] = [_serialize_extraction_segment_row(row) for row in segment_rows]
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
