# SQL Migrations

This repository currently uses plain SQL migrations (no migration framework).

## Apply order

Apply files in lexical order:

1. `001_document_extraction_persistence.sql`

Example command:

```bash
psql "$DATABASE_URL" -f migrations/001_document_extraction_persistence.sql
psql postgresql://postgres:admin@localhost:5432/InsightHubDB -f migrations/002_document_extraction_assets_and_references.sql
```

If your environment does not expose `DATABASE_URL`, use explicit Postgres connection flags and execute the same file.
