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
