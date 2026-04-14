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
python services/embedding_diagnostics.py --pretty
```

Expected:

- `pending_count` eventually decreases after worker runs.
- `embedded_count` increases for successful runs.
- `failed_count` stays `0` for healthy runs, or non-zero with visible `recent_failures` if errors occur.

### 2) Run worker once (or loop)

Single pass:

```bash
python services/embedding_worker.py --limit 256 --batch-size 64 --max-attempts 3 --retry-backoff-seconds 1.0
```

Continuous mode:

```bash
python services/embedding_worker.py --loop --sleep-seconds 2 --limit 256 --batch-size 64 --max-attempts 3 --retry-backoff-seconds 1.0
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

## Notes

- `embedding_runs` is optional by schema rollout sequence; worker continues without it, but observability is reduced.
- If your team prefers, this can be converted into a formal runbook with environment-specific commands.
