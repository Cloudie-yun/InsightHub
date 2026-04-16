# RAG Pipeline Status Review

## Current Stage Coverage

### Implemented
1. **User Upload**
   - Upload endpoints exist for both direct uploads and conversation-scoped uploads.
2. **Raw File Storage**
   - Files are written to disk under `uploads/` and tracked with metadata.
3. **Text Extraction / OCR**
   - Parsing pipeline runs asynchronously and stores parser outputs (segments, assets, references).
4. **Parser / Cleaner**
   - Canonical normalization converts parser output into structured `document_blocks`.
5. **Retrieval Preparation (pre-embedding)**
   - `EmbeddingPreparationService` builds `retrieval_text` and marks blocks as `embedding_status=ready`.
6. **Metadata DB**
   - PostgreSQL persistence exists for `document_blocks`, extraction metadata, and diagram analysis tables.

### Not Yet Implemented End-to-End
1. **Embedding Model execution**
   - Blocks are marked ready for embedding, but no embedding generation job is wired.
2. **Vector DB indexing/search**
   - No vector index table/service and no similarity query path.
3. **Query Embedding + Semantic Retrieval + Top-k selection**
   - No API route that embeds user queries and retrieves nearest chunks.
4. **Prompt Builder + LLM API answer generation**
   - Chat UI exists, but backend API routes are currently upload/parser-focused.
5. **Answer + Citations grounding flow**
   - No retrieval-grounded answer/citation assembly endpoint yet.

## Practical Stage Summary

The system is currently at **late ingestion / normalization / retrieval-prep stage**:
- You are **past parsing and cleaning**.
- You are **at retrieval-ready text construction**.
- You are **not yet running embeddings or retrieval-time RAG answering**.

## Recommended Next Steps

1. **Add embedding persistence schema**
   - Introduce a table for block embeddings (or add vector column using pgvector) keyed by `block_id`.
2. **Build embedding worker**
   - Background task to read `embedding_status=ready`, call embedding model, persist vectors, then mark `embedded`.
3. **Add retrieval service**
   - Given `conversation_id` + selected docs + user query, produce query embedding and top-k blocks.
4. **Add chat answer API route**
   - New route (e.g. `POST /api/conversations/<id>/chat`) for retrieval + prompt assembly + LLM call.
5. **Return citation payloads**
   - Include block IDs, source unit index/page, snippet text, and confidence/similarity for UI rendering.
6. **Wire frontend send action**
   - Hook chat send button/input to the new chat API and render assistant answers with source chips.

## Suggested Milestone Order

- **Milestone 1**: Embedding DB + worker.
- **Milestone 2**: Retrieval API returning top-k evidence only.
- **Milestone 3**: LLM answer route using retrieved evidence.
- **Milestone 4**: Citation UI + quality checks (empty retrieval fallback, hallucination guardrails).
# RAG Pipeline Status (Milestone 1)

This document is a practical runbook for confirming that **Milestone 1 embedding ingestion** is working in a production-like environment.

> If you are not comfortable inspecting raw database tables, that is okay.
> Use the copy/paste checks below and compare the outputs to the expected results.

## Scope

Milestone 1 includes:

- parsing uploads into retrievable `document_blocks`
- embedding those retrievable blocks
- tracking failures and retrying failed blocks

Milestone 1 does **not** include:

- retrieval/chat endpoint integration (that starts Milestone 2)

---

## Prerequisites

1. App is running and can accept uploads.
2. Worker can run in the same environment with embedding provider credentials.
3. Migrations are applied, including:
   - `migrations/004_document_block_embeddings.sql`
   - `migrations/005_embedding_runs.sql` (optional but strongly recommended for diagnostics)

---

## Operator checks (copy/paste)

### 1) Confirm queue status quickly

Run:

```bash
python -m services.embedding_diagnostics --pretty
```

Expected:

- `pending_count` eventually decreases after worker runs.
- `embedded_count` increases for successful runs.
- `failed_count` stays `0` for healthy runs, or non-zero with visible `recent_failures` if errors occur.

### 2) Run worker once (or loop)

Single pass:

```bash
python -m services.embedding_worker --limit 256 --batch-size 64 --max-attempts 3 --retry-backoff-seconds 1.0
```

Continuous mode:

```bash
python -m services.embedding_worker --loop --sleep-seconds 2 --limit 256 --batch-size 64 --max-attempts 3 --retry-backoff-seconds 1.0
```

Expected logs include:

- `model=<name>`
- `batch_size=<n>`
- `latency_ms=<value>`
- final worker summary line with selected/embedded/failed/skipped

### 3) Validate DB state (minimal SQL)

If you can run `psql`, use:

```sql
SELECT
  COUNT(*) FILTER (WHERE embedding_status='ready')    AS pending_count,
  COUNT(*) FILTER (WHERE embedding_status='embedded') AS embedded_count,
  COUNT(*) FILTER (WHERE embedding_status='failed')   AS failed_count
FROM document_blocks;
```

Expected:

- retrievable blocks move from `ready` to `embedded` after successful worker runs.

Check embeddings exist:

```sql
SELECT COUNT(*) AS embeddings_count FROM document_block_embeddings;
```

Expected:

- count is non-zero after successful embedding.

Sample queryability check:

```sql
SELECT block_id, model_name, embedding_dim, updated_at
FROM document_block_embeddings
ORDER BY updated_at DESC
LIMIT 10;
```

Expected:

- recent rows are present and include model name/dimension.

### 4) Inspect failures + reasons

When `failed_count > 0`:

```sql
SELECT
  block_id,
  status,
  error_message,
  started_at,
  completed_at,
  model_name
FROM embedding_runs
WHERE status = 'failed'
ORDER BY completed_at DESC
LIMIT 20;
```

Expected:

- each failed attempt includes a human-readable `error_message` and timestamps.

---

## Retry procedure for failed blocks

For Milestone 1, retries are operational/manual.

1. Fix root cause (credentials, provider outage, invalid config, etc.).
2. Requeue failed blocks:

```sql
UPDATE document_blocks
SET embedding_status='ready', updated_at=CURRENT_TIMESTAMP
WHERE embedding_status='failed';
```

3. Re-run worker.
4. Re-run diagnostics and confirm failures clear.

---

## Milestone 1 completion criteria (acceptance checklist)

Mark Milestone 1 complete only when all are true:

1. **New uploads eventually produce `document_blocks.embedding_status='embedded'` for retrievable blocks.**
2. **Embeddings are persisted in `document_block_embeddings` and queryable via SQL.**
3. **Failed blocks are visible with reasons (`embedding_runs.error_message`) and can be retried by requeueing to `ready`.**
4. **No chat retrieval endpoint is required yet** (deferred to Milestone 2).

---

## Manual verification log template

Use this section during release validation so completion is tied to concrete evidence.

### A) New upload reaches `embedded`

1. Upload a small test document through the app UI.
2. Run the worker in loop mode until queue settles:

```bash
python -m services.embedding_worker --loop --sleep-seconds 2 --limit 256 --batch-size 64 --max-attempts 3 --retry-backoff-seconds 1.0
```

3. In a separate terminal, confirm status progression:

```sql
SELECT id, document_id, retrievable, embedding_status, updated_at
FROM document_blocks
WHERE document_id = '<YOUR_DOCUMENT_ID>'
ORDER BY id;
```

Pass condition:

- retrievable rows for the uploaded document eventually report `embedding_status='embedded'`.

### B) Embeddings persisted and queryable

Run:

```sql
SELECT COUNT(*) AS embeddings_count FROM document_block_embeddings;
SELECT block_id, model_name, embedding_dim, updated_at
FROM document_block_embeddings
ORDER BY updated_at DESC
LIMIT 10;
```

Pass condition:

- count is non-zero (and increases after test upload),
- recent rows include expected model metadata.

### C) Failure visibility + retry

Force or wait for a known failure scenario, then run:

```sql
SELECT
  block_id,
  status,
  error_message,
  started_at,
  completed_at,
  model_name
FROM embedding_runs
WHERE status='failed'
ORDER BY completed_at DESC
LIMIT 20;
```

Retry and confirm recovery:

```sql
UPDATE document_blocks
SET embedding_status='ready', updated_at=CURRENT_TIMESTAMP
WHERE embedding_status='failed';
```

Then re-run worker + diagnostics.

Pass condition:

- failures are listed with readable reasons,
- previously failed blocks can transition from `failed` → `ready` → `embedded`.

### D) Explicit Milestone boundary check

Confirm no Milestone 2 dependency was introduced:

- no requirement for a chat retrieval endpoint in this validation,
- acceptance is strictly ingestion, persistence, visibility, and retry.

---

## Notes

- In this repository, prefer module execution (`python -m services.embedding_worker`, `python -m services.embedding_diagnostics`) so imports resolve correctly from the repo root.
- `embedding_runs` is optional by schema rollout sequence; worker continues without it, but observability is reduced.
- If your team prefers, this can be converted into a formal runbook with environment-specific commands.
