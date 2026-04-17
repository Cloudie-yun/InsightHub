from __future__ import annotations

from typing import Any

from db import get_db_connection
from services.diagram_vision_service import get_default_diagram_vision_prompt_template
from services.text_answer_service import get_default_grounded_answer_prompt_template


PROMPT_TYPE_QNA = "qna"
PROMPT_TYPE_VISION = "vision"
PROMPT_PROFILE_TYPES = (PROMPT_TYPE_QNA, PROMPT_TYPE_VISION)
PROMPT_PROFILE_MAX_LENGTH = 3000
def get_default_prompt_profiles() -> dict[str, str]:
    return {
        PROMPT_TYPE_QNA: get_default_grounded_answer_prompt_template(),
        PROMPT_TYPE_VISION: get_default_diagram_vision_prompt_template(),
    }


def _relation_exists(cur, relation_name: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (relation_name,))
    row = cur.fetchone()
    return bool(row and row[0])


def _get_table_columns(cur, table_name: str) -> set[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s
        """,
        (table_name,),
    )
    return {str(row[0]) for row in cur.fetchall()}


def _load_legacy_custom_system_prompt(cur, user_id: str) -> str:
    if not _relation_exists(cur, "users"):
        return ""
    columns = _get_table_columns(cur, "users")
    if "custom_system_prompt" not in columns:
        return ""
    cur.execute(
        "SELECT COALESCE(custom_system_prompt, '') FROM users WHERE user_id = %s",
        (user_id,),
    )
    row = cur.fetchone()
    return str(row[0] or "").strip() if row else ""


def get_prompt_profiles_for_user(cur, user_id: str) -> dict[str, str]:
    profiles = {prompt_type: "" for prompt_type in PROMPT_PROFILE_TYPES}
    if not user_id:
        return profiles

    if _relation_exists(cur, "user_prompt_profiles"):
        cur.execute(
            """
            SELECT prompt_type, prompt_text
            FROM user_prompt_profiles
            WHERE user_id = %s
            """,
            (user_id,),
        )
        for prompt_type, prompt_text in cur.fetchall():
            normalized_type = str(prompt_type or "").strip().lower()
            if normalized_type not in profiles:
                continue
            profiles[normalized_type] = str(prompt_text or "").strip()

    if not profiles[PROMPT_TYPE_QNA]:
        profiles[PROMPT_TYPE_QNA] = _load_legacy_custom_system_prompt(cur, user_id)

    return profiles


def save_prompt_profiles_for_user(cur, user_id: str, prompt_profiles: dict[str, Any]) -> dict[str, str]:
    normalized_profiles = {prompt_type: "" for prompt_type in PROMPT_PROFILE_TYPES}
    for prompt_type in PROMPT_PROFILE_TYPES:
        value = str((prompt_profiles or {}).get(prompt_type) or "").strip()
        normalized_profiles[prompt_type] = value[:PROMPT_PROFILE_MAX_LENGTH]

    if _relation_exists(cur, "user_prompt_profiles"):
        for prompt_type, prompt_text in normalized_profiles.items():
            cur.execute(
                """
                INSERT INTO user_prompt_profiles (
                    user_id,
                    prompt_type,
                    prompt_text
                )
                VALUES (%s, %s, %s)
                ON CONFLICT (user_id, prompt_type)
                DO UPDATE SET
                    prompt_text = EXCLUDED.prompt_text,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (user_id, prompt_type, prompt_text),
            )

    if _relation_exists(cur, "users"):
        columns = _get_table_columns(cur, "users")
        if "custom_system_prompt" in columns:
            cur.execute(
                """
                UPDATE users
                SET custom_system_prompt = %s
                WHERE user_id = %s
                """,
                (normalized_profiles[PROMPT_TYPE_QNA], user_id),
            )

    return normalized_profiles


def load_prompt_profiles_for_user(user_id: str) -> dict[str, str]:
    conn = get_db_connection()
    try:
        with conn, conn.cursor() as cur:
            return get_prompt_profiles_for_user(cur, user_id)
    finally:
        conn.close()
