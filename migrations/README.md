# SQL Migrations

This repository currently uses plain SQL migrations rather than a migration framework.

## Recommended paths

For a fresh database, use the unified bootstrap migration:

```bash
psql "$DATABASE_URL" -f migrations/000_document_extraction_unified.sql
psql postgresql://postgres:admin@localhost:5432/InsightHubDB -f migrations/000_document_extraction_unified.sql
```

For an existing database that already applied older files, keep using the historical incremental files instead of the unified bootstrap. Apply them in lexical order:

1. `001_document_extraction_persistence.sql`
2. `002_document_extraction_assets_and_references.sql`
3. `003_document_blocks.sql`
4. `003_diagram_vision_tables.sql`
5. `004_document_block_embeddings.sql`
6. `004_diagram_vision_scoring.sql`
7. `005_embedding_runs.sql`
8. `006_document_block_soft_delete.sql`
9. `007_fix_diagram_crop_storage_paths.sql`
10. `008_quota_state.sql`
11. `009_quota_limits.sql`
12. `010_conversation_messages.sql`
13. `012_user_profile_settings.sql`
14. `013_user_prompt_profiles.sql`
15. `014_conversation_message_versioning.sql`

## Notes

- `000_document_extraction_unified.sql` represents the current final schema for fresh installs.
- The numbered files remain as the historical upgrade path for databases created before the unified bootstrap existed.
- Do not apply `000_document_extraction_unified.sql` and then re-apply the numbered files on the same fresh database.
- `012_user_profile_settings.sql` is legacy compatibility for `users.custom_system_prompt`. Prompt customization now lives in `user_prompt_profiles`.
