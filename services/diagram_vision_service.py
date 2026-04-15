from __future__ import annotations

import base64
import json
import mimetypes
import os
import re
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib import error, request

from psycopg2.extras import Json


GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta"
GEMINI_MODEL = os.environ.get("GEMINI_VISION_MODEL", "gemini-2.5-flash")
PROMPT_VERSION = "diagram_v1"
VISION_GATE_PROMPT_VERSION = "diagram_gate_v1"
DIAGRAM_VISION_MIN_SCORE = float(os.environ.get("DIAGRAM_VISION_MIN_SCORE", "0.45"))

_POSITIVE_DIAGRAM_KEYWORDS = {
    "architecture",
    "workflow",
    "pipeline",
    "framework",
    "system",
    "process",
    "flowchart",
    "chart",
    "graph",
    "plot",
    "scatter",
    "histogram",
    "distribution",
    "trend",
    "axis",
    "component",
    "module",
    "network",
    "circuit",
    "diagram",
}
_REFERENCE_CONTEXT_KEYWORDS = {
    "figure",
    "fig.",
    "shown",
    "illustrates",
    "depicts",
    "overview",
    "architecture",
    "workflow",
    "pipeline",
    "results",
    "comparison",
    "performance",
    "trend",
}
_NEGATIVE_IMAGE_KEYWORDS = {
    "logo",
    "icon",
    "avatar",
    "portrait",
    "author",
    "headshot",
    "photo",
    "photograph",
    "cover",
    "decoration",
    "ornament",
}

DIAGRAM_RESPONSE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "diagram_type",
        "title_or_caption",
        "ocr_text",
        "summary",
        "key_entities",
        "relationships",
        "question_answerable_facts",
        "confidence",
    ],
    "properties": {
        "diagram_type": {
            "type": "string",
            "enum": [
                "flowchart",
                "chart",
                "table_like_figure",
                "architecture_diagram",
                "scientific_figure",
                "screenshot",
                "image",
                "unknown",
            ],
        },
        "title_or_caption": {"type": ["string", "null"]},
        "ocr_text": {"type": "array", "items": {"type": "string"}},
        "summary": {"type": "string"},
        "key_entities": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["name", "type"],
                "properties": {
                    "name": {"type": "string"},
                    "type": {
                        "type": "string",
                        "enum": ["node", "axis", "label", "box", "legend", "component", "arrow", "unknown"],
                    },
                },
            },
        },
        "relationships": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["source", "relation", "target"],
                "properties": {
                    "source": {"type": "string"},
                    "relation": {
                        "type": "string",
                        "enum": [
                            "points_to",
                            "connected_to",
                            "contains",
                            "compares_with",
                            "shows",
                            "influences",
                            "depends_on",
                            "unknown",
                        ],
                    },
                    "target": {"type": "string"},
                },
            },
        },
        "question_answerable_facts": {"type": "array", "items": {"type": "string"}},
        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
    },
}


def _is_truthy_env(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


@dataclass
class DiagramVisionInput:
    block_id: str
    image_path: str
    caption_text: str | None
    nearby_text: str | None
    image_asset_id: str | None = None
    diagram_kind: str = "figure"


@dataclass
class DiagramVisionDecision:
    score: float
    should_analyze: bool
    reasons: list[str]


def build_diagram_prompt(*, caption: str | None, nearby_text: str | None) -> str:
    caption = (caption or "").strip()
    nearby_text = (nearby_text or "").strip()
    return f"""
You are analyzing a diagram extracted from an academic or technical document.

Goal:
Produce structured information that helps a document-grounded chatbot answer questions about this figure accurately.

Instructions:
1. Identify the figure type.
2. Read any visible labels or text inside the figure.
3. Explain what the figure shows in a short factual summary.
4. Extract key entities such as nodes, labels, components, legends, arrows, or axes.
5. Extract explicit relationships shown by arrows, lines, containment, comparison, or dependency.
6. Extract concise facts that a chatbot can directly reuse in question answering.
7. Do not invent information that is not visible or reasonably supported by the caption/context.
8. Return JSON only.

Caption:
{caption if caption else "null"}

Nearby document context:
{nearby_text if nearby_text else "null"}
""".strip()


def score_diagram_for_vision(item: DiagramVisionInput) -> DiagramVisionDecision:
    caption = (item.caption_text or "").strip().lower()
    nearby_text = (item.nearby_text or "").strip().lower()
    combined_text = f"{caption}\n{nearby_text}".strip()
    score = 0.0
    reasons: list[str] = []

    kind = str(item.diagram_kind or "").strip().lower()
    if kind in {"chart", "figure"}:
        score += 0.22
        reasons.append(f"diagram kind '{kind}' is usually useful for vision QA")
    elif kind == "image":
        score -= 0.18
        reasons.append("diagram kind is generic image")
    elif kind and kind != "unknown":
        score += 0.12
        reasons.append(f"diagram kind '{kind}' indicates structured content")

    if caption:
        score += 0.10
        reasons.append("caption is available")
        if len(caption.split()) >= 5:
            score += 0.08
            reasons.append("caption has descriptive detail")

    positive_hits = sorted(keyword for keyword in _POSITIVE_DIAGRAM_KEYWORDS if keyword in combined_text)
    if positive_hits:
        score += min(0.28, 0.07 * len(positive_hits))
        reasons.append(f"diagram keywords found: {', '.join(positive_hits[:4])}")

    reference_hits = sorted(keyword for keyword in _REFERENCE_CONTEXT_KEYWORDS if keyword in nearby_text)
    if reference_hits:
        score += min(0.18, 0.06 * len(reference_hits))
        reasons.append(f"nearby text references figure semantics: {', '.join(reference_hits[:3])}")

    negative_hits = sorted(keyword for keyword in _NEGATIVE_IMAGE_KEYWORDS if keyword in combined_text)
    if negative_hits:
        score -= min(0.40, 0.12 * len(negative_hits))
        reasons.append(f"decorative/photo-like keywords found: {', '.join(negative_hits[:4])}")

    if caption and re.fullmatch(r"(figure|fig\.?)\s*\d+[a-z]?:?", caption, flags=re.IGNORECASE):
        score -= 0.10
        reasons.append("caption is only a bare figure label")

    if not caption and not nearby_text:
        score -= 0.12
        reasons.append("no caption or nearby context available")

    bounded_score = round(max(0.0, min(1.0, score)), 3)
    return DiagramVisionDecision(
        score=bounded_score,
        should_analyze=bounded_score >= DIAGRAM_VISION_MIN_SCORE,
        reasons=reasons or ["no strong evidence that vision analysis would add value"],
    )


def _guess_mime_type(path: str) -> str:
    mime, _ = mimetypes.guess_type(path)
    return mime or "image/png"


def _encode_file_base64(path: str) -> str:
    with open(path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode("utf-8")


class GeminiDiagramVisionService:
    def __init__(self, *, api_key: str, model: str = GEMINI_MODEL, timeout_seconds: int = 60) -> None:
        self.api_key = api_key
        self.model = model
        self.timeout_seconds = timeout_seconds

    def analyze(self, item: DiagramVisionInput) -> dict[str, Any]:
        image_path = Path(item.image_path)
        if not image_path.exists():
            raise FileNotFoundError(f"Diagram image not found: {image_path}")

        payload = {
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {"text": build_diagram_prompt(caption=item.caption_text, nearby_text=item.nearby_text)},
                        {
                            "inlineData": {
                                "mimeType": _guess_mime_type(str(image_path)),
                                "data": _encode_file_base64(str(image_path)),
                            }
                        },
                    ],
                }
            ],
            "generationConfig": {
                "responseMimeType": "application/json",
                "responseJsonSchema": DIAGRAM_RESPONSE_SCHEMA,
                "temperature": 0.1,
                "topP": 0.95,
                "maxOutputTokens": 2048,
            },
        }

        url = f"{GEMINI_API_BASE}/models/{self.model}:generateContent?key={self.api_key}"
        req = request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as response:
                raw = json.loads(response.read().decode("utf-8"))
        except error.HTTPError as http_error:
            details = http_error.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"Gemini request failed: {http_error.code} {details}") from http_error

        text = (
            raw.get("candidates", [{}])[0]
            .get("content", {})
            .get("parts", [{}])[0]
            .get("text", "")
            .strip()
        )
        if not text:
            raise ValueError("Gemini returned no JSON text")

        return {
            "provider_name": "gemini",
            "model_name": self.model,
            "prompt_version": PROMPT_VERSION,
            "request_payload": payload,
            "raw_response": raw,
            "parsed_output": json.loads(text),
        }


def save_diagram_analysis(cur, *, block_id: str, image_asset_id: str | None, diagram_kind: str, analysis_result: dict[str, Any]) -> None:
    parsed = analysis_result["parsed_output"]
    analysis_run_id = str(uuid.uuid4())

    cur.execute(
        """
        INSERT INTO diagram_block_analysis_runs (
            analysis_run_id,
            block_id,
            provider_name,
            model_name,
            prompt_version,
            request_payload,
            raw_response,
            parsed_output,
            status,
            completed_at
        )
        VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, 'completed', NOW())
        """,
        (
            analysis_run_id,
            block_id,
            analysis_result["provider_name"],
            analysis_result["model_name"],
            analysis_result["prompt_version"],
            Json(analysis_result["request_payload"]),
            Json(analysis_result["raw_response"]),
            Json(parsed),
        ),
    )

    cur.execute(
        """
        INSERT INTO diagram_block_details (
            block_id,
            image_asset_id,
            diagram_kind,
            image_region,
            ocr_text,
            visual_description,
            semantic_links,
            question_answerable_facts,
            vision_status,
            vision_confidence,
            provider_name,
            model_name,
            prompt_version,
            last_analyzed_at,
            updated_at
        )
        VALUES (
            %s, %s, %s, '{}'::jsonb, %s::jsonb, %s, %s::jsonb, %s::jsonb,
            'completed', %s, %s, %s, %s, NOW(), NOW()
        )
        ON CONFLICT (block_id)
        DO UPDATE SET
            image_asset_id = EXCLUDED.image_asset_id,
            diagram_kind = EXCLUDED.diagram_kind,
            ocr_text = EXCLUDED.ocr_text,
            visual_description = EXCLUDED.visual_description,
            semantic_links = EXCLUDED.semantic_links,
            question_answerable_facts = EXCLUDED.question_answerable_facts,
            vision_status = EXCLUDED.vision_status,
            vision_confidence = EXCLUDED.vision_confidence,
            provider_name = EXCLUDED.provider_name,
            model_name = EXCLUDED.model_name,
            prompt_version = EXCLUDED.prompt_version,
            last_analyzed_at = EXCLUDED.last_analyzed_at,
            updated_at = NOW()
        """,
        (
            block_id,
            image_asset_id,
            parsed.get("diagram_type") or diagram_kind or "unknown",
            Json(parsed.get("ocr_text", [])),
            parsed.get("summary"),
            Json(parsed.get("relationships", [])),
            Json(parsed.get("question_answerable_facts", [])),
            parsed.get("confidence"),
            analysis_result["provider_name"],
            analysis_result["model_name"],
            analysis_result["prompt_version"],
        ),
    )

    cur.execute(
        """
        UPDATE document_blocks
        SET
            normalized_content = COALESCE(normalized_content, '{}'::jsonb) || %s::jsonb,
            display_text = COALESCE(caption_text, display_text),
            updated_at = NOW()
        WHERE block_id = %s
        """,
        (
            Json(
                {
                    "vision": parsed,
                    "visual_description": parsed.get("summary"),
                    "ocr_text": parsed.get("ocr_text", []),
                    "semantic_links": parsed.get("relationships", []),
                    "vision_status": "completed",
                }
            ),
            block_id,
        ),
    )


def save_diagram_analysis_failure(
    cur,
    *,
    block_id: str,
    provider_name: str,
    model_name: str,
    prompt_version: str,
    request_payload: dict[str, Any],
    error_message: str,
) -> None:
    analysis_run_id = str(uuid.uuid4())

    cur.execute(
        """
        INSERT INTO diagram_block_analysis_runs (
            analysis_run_id,
            block_id,
            provider_name,
            model_name,
            prompt_version,
            request_payload,
            status,
            error_message,
            completed_at
        )
        VALUES (%s, %s, %s, %s, %s, %s::jsonb, 'failed', %s, NOW())
        """,
        (
            analysis_run_id,
            block_id,
            provider_name,
            model_name,
            prompt_version,
            Json(request_payload),
            error_message,
        ),
    )

    cur.execute(
        """
        INSERT INTO diagram_block_details (
            block_id,
            vision_status,
            provider_name,
            model_name,
            prompt_version,
            updated_at
        )
        VALUES (%s, 'failed', %s, %s, %s, NOW())
        ON CONFLICT (block_id)
        DO UPDATE SET
            vision_status = 'failed',
            provider_name = EXCLUDED.provider_name,
            model_name = EXCLUDED.model_name,
            prompt_version = EXCLUDED.prompt_version,
            updated_at = NOW()
        """,
        (block_id, provider_name, model_name, prompt_version),
    )

    cur.execute(
        """
        UPDATE document_blocks
        SET
            normalized_content = COALESCE(normalized_content, '{}'::jsonb) || %s::jsonb,
            updated_at = NOW()
        WHERE block_id = %s
        """,
        (Json({"vision_status": "failed", "vision_error": error_message}), block_id),
    )


def save_diagram_analysis_skipped(
    cur,
    *,
    block_id: str,
    image_asset_id: str | None,
    diagram_kind: str,
    decision: DiagramVisionDecision,
) -> None:
    cur.execute(
        """
        INSERT INTO diagram_block_details (
            block_id,
            image_asset_id,
            diagram_kind,
            vision_status,
            vision_confidence,
            vision_gate_score,
            vision_gate_reasons,
            prompt_version,
            updated_at
        )
        VALUES (%s, %s, %s, 'skipped', %s, %s, %s::jsonb, %s, NOW())
        ON CONFLICT (block_id)
        DO UPDATE SET
            image_asset_id = EXCLUDED.image_asset_id,
            diagram_kind = EXCLUDED.diagram_kind,
            vision_status = 'skipped',
            vision_confidence = EXCLUDED.vision_confidence,
            vision_gate_score = EXCLUDED.vision_gate_score,
            vision_gate_reasons = EXCLUDED.vision_gate_reasons,
            prompt_version = EXCLUDED.prompt_version,
            updated_at = NOW()
        """,
        (
            block_id,
            image_asset_id,
            diagram_kind or "unknown",
            decision.score,
            decision.score,
            Json(decision.reasons),
            VISION_GATE_PROMPT_VERSION,
        ),
    )

    cur.execute(
        """
        UPDATE document_blocks
        SET
            normalized_content = COALESCE(normalized_content, '{}'::jsonb) || %s::jsonb,
            updated_at = NOW()
        WHERE block_id = %s
        """,
        (
            Json(
                {
                    "vision_status": "skipped",
                    "vision_gate_score": decision.score,
                    "vision_gate_reasons": decision.reasons,
                }
            ),
            block_id,
        ),
    )


def fetch_pending_diagram_inputs(cur, *, document_id: str, context_char_limit: int = 2000) -> list[DiagramVisionInput]:
    statuses = ["pending_vision_analysis", "failed"]
    if _is_truthy_env(os.environ.get("DIAGRAM_VISION_INCLUDE_SKIPPED", "0")):
        statuses.append("skipped")

    cur.execute(
        """
        WITH ordered_blocks AS (
            SELECT
                db.block_id,
                db.block_type,
                db.display_text,
                db.caption_text,
                db.subtype,
                db.reading_order,
                db.source_unit_index,
                dba.block_asset_id,
                dba.storage_path,
                COALESCE(dbd.vision_status, 'pending_vision_analysis') AS vision_status,
                LAG(db.display_text) OVER (ORDER BY db.source_unit_index ASC, db.reading_order ASC NULLS LAST) AS prev_text,
                LEAD(db.display_text) OVER (ORDER BY db.source_unit_index ASC, db.reading_order ASC NULLS LAST) AS next_text
            FROM document_blocks db
            LEFT JOIN document_block_assets dba
                ON dba.block_id = db.block_id
               AND dba.asset_role = 'diagram_crop'
            LEFT JOIN diagram_block_details dbd
                ON dbd.block_id = db.block_id
            WHERE db.document_id = %s
              AND db.block_type = 'diagram'
        )
        SELECT
            block_id,
            block_asset_id,
            storage_path,
            caption_text,
            subtype,
            LEFT(CONCAT_WS('\n', prev_text, next_text), %s)
        FROM ordered_blocks
        WHERE COALESCE(storage_path, '') <> ''
          AND vision_status = ANY(%s)
        ORDER BY source_unit_index ASC, reading_order ASC NULLS LAST
        """,
        (document_id, context_char_limit, statuses),
    )

    results = []
    for block_id, block_asset_id, storage_path, caption_text, subtype, nearby_text in cur.fetchall():
        results.append(
            DiagramVisionInput(
                block_id=str(block_id),
                image_asset_id=str(block_asset_id) if block_asset_id else None,
                image_path=storage_path,
                caption_text=caption_text,
                nearby_text=nearby_text,
                diagram_kind=subtype or "figure",
            )
        )
    return results


def run_diagram_analysis_for_document(cur, *, document_id: str, api_key: str | None = None) -> list[str]:
    api_key = api_key or os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is required for diagram analysis")

    service = GeminiDiagramVisionService(api_key=api_key)
    analyzed_block_ids: list[str] = []

    for item in fetch_pending_diagram_inputs(cur, document_id=document_id):
        decision = score_diagram_for_vision(item)
        if not decision.should_analyze:
            save_diagram_analysis_skipped(
                cur,
                block_id=item.block_id,
                image_asset_id=item.image_asset_id,
                diagram_kind=item.diagram_kind,
                decision=decision,
            )
            continue
        try:
            result = service.analyze(item)
            save_diagram_analysis(
                cur,
                block_id=item.block_id,
                image_asset_id=item.image_asset_id,
                diagram_kind=item.diagram_kind,
                analysis_result=result,
            )
            analyzed_block_ids.append(item.block_id)
        except Exception as exc:
            save_diagram_analysis_failure(
                cur,
                block_id=item.block_id,
                provider_name="gemini",
                model_name=GEMINI_MODEL,
                prompt_version=PROMPT_VERSION,
                request_payload={},
                error_message=str(exc),
            )

    return analyzed_block_ids
