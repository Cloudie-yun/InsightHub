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
6. **Embedding Model Execution**
   - `EmbeddingWorker` reads ready blocks, generates embeddings, persists vectors, and marks blocks `embedded` or `failed`.
7. **Vector DB Indexing / Search**
   - `document_block_embeddings` stores pgvector embeddings and retrieval queries rank blocks by vector similarity.
8. **Query Embedding + Semantic Retrieval + Top-k Selection**
   - `RetrievalService` embeds the user query, scopes by conversation and selected documents, and returns ranked chunks.
9. **Metadata DB**
   - PostgreSQL persistence exists for `document_blocks`, extraction metadata, diagram analysis tables, embedding rows, and embedding run logs.
10. **Retrieval Diagnostics**
   - CLI diagnostics exist for both embedding status and retrieval troubleshooting.
11. **Inspection-Only Chat Persistence**
   - Conversation messages can persist retrieval inspection output, but this is not yet grounded LLM answer generation.

### Not Yet Implemented End-to-End
1. **Prompt Builder + LLM API answer generation**
   - There is no grounded LLM answer generation step yet; the current message flow stores a retrieval summary rather than model-generated answers.
2. **Answer + Citations grounding flow**
   - Retrieval results include chunk metadata, but there is no finalized answer-plus-citations UX or grounding policy yet.
3. **Frontend chat UX wired to true RAG answers**
   - The backend can retrieve evidence, but the full user-facing ask-answer flow is not yet complete.

## Practical Stage Summary

The system is currently at **Milestone 2 retrieval stage**:

- You are **past parsing and cleaning**.
- You are **running embeddings and storing vectors**.
- You are **retrieving top related chunks for a user query across selected conversation documents**.
- You are **not yet producing grounded LLM answers with final citations/UI polish**.

## Recommended Next Steps

1. **Add grounded LLM answer generation**
   - Build the prompt assembly + model call step on top of retrieved evidence.
2. **Stabilize the chat answer API contract**
   - Decide whether `/api/conversations/<id>/messages` remains the answer route or if a dedicated answer endpoint is preferred.
3. **Return finalized citation payloads**
   - Standardize citation fields for block IDs, snippet text, page/unit metadata, and UI rendering.
4. **Wire frontend send action to true answers**
   - Replace retrieval-summary responses with grounded assistant answers.
5. **Close the doc/code contract gap**
   - Keep this document aligned with the actual route name, default `k`, and response schema used by the implementation.

## Suggested Milestone Order

- **Milestone 1**: Embedding DB + worker.
- **Milestone 2**: Retrieval API returning top-k evidence only.
- **Milestone 3**: LLM answer route using retrieved evidence.
- **Milestone 4**: Citation UI + quality checks (empty retrieval fallback, hallucination guardrails).

## Current Milestone

- **Current repo state**: Milestone 2 implemented.
- **Next target**: Milestone 3.

# RAG Pipeline Status (Milestone 1)

This section is a practical runbook for confirming that **Milestone 1 embedding ingestion** is working in a production-like environment.

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

---

# Milestone 2 Retrieval API Contract

This section defines the **Milestone 2 evidence retrieval API**. Milestone 2 means:

- take a user query,
- restrict search to the conversation's documents,
- optionally narrow further to the user-selected `document_ids`,
- return the top related chunks only,
- do **not** generate a final LLM answer yet.

That is the correct practical definition of Milestone 2 in this repository.

## Endpoint

- **Method**: `POST`
- **Path**: `/api/conversations/<conversation_id>/retrieve`
- **Auth**: same authenticated user/session requirements as existing conversation APIs.

## Request schema

```json
{
  "query": "What does the contract say about renewal?",
  "k": 5,
  "document_ids": ["doc_123", "doc_456"]
}
```

### Fields

- `query` (string, required)
  - Must be non-empty after trimming whitespace.
- `k` (integer, optional)
  - Defaults to **5** when omitted.
  - Maximum allowed: **20**.
  - Minimum allowed: **1**.
- `document_ids` (array[string], optional)
  - If present, each id must belong to the given `conversation_id` and requesting user.
  - If omitted, retrieval searches across all conversation-scoped documents available to that conversation.

## Response schema (success)

```json
{
  "query": "What does the contract say about renewal?",
  "k": 5,
  "strategy": "vector",
  "returned_count": 3,
  "include_filtered": false,
  "filter_summary": {
    "total_candidate_count": 42,
    "included_candidate_count": 36,
    "excluded_candidate_count": 6,
    "visible_candidate_count": 36,
    "include_filtered": false
  },
  "results": [
    {
      "block_id": "b_101",
      "document_id": "doc_123",
      "document_name": "contract.pdf",
      "score": 0.8877,
      "snippet": "The agreement renews automatically every 12 months...",
      "block_type": "text",
      "subtype": "paragraph",
      "text_role": "",
      "section_path": ["Renewal"],
      "source_metadata": {
        "page": 7,
        "unit_index": 42
      },
      "is_filtered": false,
      "filter_reason": "",
      "relevance_reason": "matched body paragraph within section"
    }
  ]
}
```

### Notes

- `strategy` is currently `vector` for normal semantic retrieval.
- `strategy` may become `keyword_fallback` if query embedding is unavailable but fallback retrieval succeeds.
- `returned_count` may be lower than requested `k` when eligible blocks are fewer than `k`.
- `score` is a normalized similarity-style value derived from vector distance for display and ranking feedback.

## Ranking behavior (required)

1. **Distance metric**: `cosine_distance` over query embedding vs. `document_block_embeddings.embedding`.
2. **Primary sort**: ascending `distance` (smaller is more similar).
3. **Current API response**: exposes normalized `score` rather than raw `distance`.
4. **Default `k`**: `5`.
5. **Max `k`**: `20` (reject larger values with `400`).

## Filtering rules (required)

Retrieval candidates MUST satisfy all rules:

1. `document_blocks.retrievable = TRUE`.
2. `document_blocks.embedding_status = 'embedded'`.
3. Block has an embedded vector row (`document_block_embeddings.block_id = document_blocks.block_id`).
4. Block belongs to the requested `conversation_id` through its parent document.
5. If `document_ids` is supplied, the block's `document_id` must be in that supplied set.

Practical implication:

- Blocks that are parsed but not yet embedded are excluded.
- Blocks outside the conversation scope are excluded even if IDs are guessed by a client.

## Error responses

### 400 Bad Request (input validation)

Use for malformed payloads:

- missing/blank `query`
- non-integer `k`
- `k < 1` or `k > 20`
- `document_ids` not an array of strings

```json
{
  "error": {
    "code": "invalid_request",
    "message": "k must be an integer between 1 and 20."
  }
}
```

### Scope violation

Current implementation returns a validation-style error when requested `document_ids` are outside the conversation scope.

```json
{
  "error": "One or more document_ids are not part of this conversation.",
  "details": {
    "code": "invalid_document_scope"
  }
}
```

### 404 Not Found

Use when the target conversation does not exist for the current user.

```json
{
  "error": "Conversation not found."
}
```

### 200 OK with empty retrieval outcome

Milestone 2 treats "no eligible matches" as a successful retrieval call with zero results.

```json
{
  "query": "Explain the cancellation terms",
  "k": 5,
  "strategy": "vector",
  "returned_count": 0,
  "results": []
}
```

## Milestone 2 acceptance checklist

Use these checks to mark Milestone 2 complete.

1. **API contract available and implemented**
   - `POST /api/conversations/<conversation_id>/retrieve` accepts `query`, optional `k`, optional `document_ids`.
2. **Semantic retrieval is live**
   - The service embeds the query and returns top related chunks from the selected scope.
3. **`k` behavior enforced**
   - Omitted `k` resolves to `5`.
   - `k > 20` returns `400`.
4. **Filtering rules enforced**
   - Non-embedded blocks never appear.
   - Out-of-scope conversation/doc IDs are not retrievable.
5. **Empty retrieval is non-error**
   - No matching eligible blocks returns `200` with `results: []`.

### Concrete SQL/API verification

Run SQL (example diagnostic query):

```sql
SELECT
  db.block_id,
  db.document_id,
  db.embedding_status,
  db.retrievable,
  dbe.block_id IS NOT NULL AS has_vector
FROM document_blocks db
LEFT JOIN document_block_embeddings dbe ON dbe.block_id = db.block_id
WHERE db.document_id = '<DOC_ID>'
ORDER BY db.block_id
LIMIT 50;
```

Pass criteria:

- only rows with `retrievable=true`, `embedding_status='embedded'`, and `has_vector=true` are eligible.

API checks (example):

```bash
# 1) Default k check
curl -s -X POST "$APP_BASE_URL/api/conversations/<CONV_ID>/retrieve" \
  -H "Content-Type: application/json" \
  -b "<SESSION_COOKIE>" \
  -d '{"query":"renewal clause"}'

# 2) k upper bound check (expect 400)
curl -s -o /tmp/m2_err.json -w "%{http_code}\n" \
  -X POST "$APP_BASE_URL/api/conversations/<CONV_ID>/retrieve" \
  -H "Content-Type: application/json" \
  -b "<SESSION_COOKIE>" \
  -d '{"query":"renewal clause","k":21}'

# 3) Empty retrieval check (expect 200 and results: [])
curl -s -X POST "$APP_BASE_URL/api/conversations/<CONV_ID>/retrieve" \
  -H "Content-Type: application/json" \
  -b "<SESSION_COOKIE>" \
  -d '{"query":"nonexistent token sequence zyxwvu"}'

# 4) Retrieval diagnostics CLI (JSON output)
python -m services.retrieval_diagnostics \
  --conversation-id "<CONV_ID>" \
  --document-ids "<DOC_ID_1>,<DOC_ID_2>" \
  -k 5 \
  "renewal clause"

# 5) Retrieval diagnostics CLI (HTML troubleshooting report)
python -m services.retrieval_diagnostics \
  --conversation-id "<CONV_ID>" \
  --pretty \
  "renewal clause"
```

The retrieval diagnostics command surfaces latency and candidate counts (`conversation_document_count`, `scoped_document_count`, `scoped_block_count`, and `eligible_candidate_count`) to quickly isolate filtering or performance issues during Milestone 2 verification.

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
- previously failed blocks can transition from `failed` -> `ready` -> `embedded`.

### D) Explicit Milestone boundary check

Confirm the Milestone 2 boundary remains intact:

- retrieval returns evidence only,
- final grounded answer generation is deferred to Milestone 3.

---

## Notes

- In this repository, prefer module execution (`python -m services.embedding_worker`, `python -m services.embedding_diagnostics`) so imports resolve correctly from the repo root.
- `embedding_runs` is optional by schema rollout sequence; worker continues without it, but observability is reduced.
- If your team prefers, this can be converted into a formal runbook with environment-specific commands.
