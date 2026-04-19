# RAG Pipeline — Full Fix Specification for Codex

This file instructs Codex to apply all improvements to the RAG retrieval and generation pipeline.
Apply every section in order. Do not skip sections. Do not refactor code outside the scope described.

## Implementation Status

- [x] Section 1
- [x] Section 2
- [x] Section 3
- [x] Section 4
- [x] Section 5
- [x] Section 6
- [x] Section 7
- [x] Section 8
- [x] Section 9

---

## SECTION 1 — `services/retrieval_service.py`: Fix `_classify_query_scope`

**Problem:** The classifier uses single-word triggers that over-fire on narrow queries (e.g. "list the author's name" → broad) and misses many genuinely broad queries (e.g. "What are the benefits of X?" → narrow). The 12-word threshold is too low.

**Replace** the entire `_classify_query_scope` static method with the following:

```python
@staticmethod
def _classify_query_scope(query: str) -> str:
    query_text = str(query or "").strip().lower()
    if not query_text:
        return "narrow"

    # Multiple questions always means broad
    if query_text.count("?") > 1:
        return "broad"

    # Vague follow-ups: weak embeddings need more chunks to compensate
    followup_markers = (
        "tell me more",
        "elaborate",
        "more detail",
        "give an example",
        "can you explain",
        "what do you mean",
        "go on",
        "continue",
    )
    if any(query_text == m or query_text.startswith(m) for m in followup_markers):
        return "broad"

    # Intent phrases that require enumeration or synthesis across multiple chunks
    broad_intent_phrases = (
        "list all", "list the", "list every",
        "main points", "key points", "key concepts",
        "overview of", "summarize", "summary of",
        "compare", "contrast",
        "advantages and disadvantages", "pros and cons",
        "challenges and", "future directions",
        "explain the concept", "explain how", "explain why",
        "applications of", "uses of",
        "what are the", "what were the",
        "describe all", "describe the main",
    )
    if any(phrase in query_text for phrase in broad_intent_phrases):
        return "broad"

    # Long queries are likely multi-part or complex
    word_count = len([w for w in query_text.split() if w])
    if word_count >= 15:
        return "broad"

    return "narrow"
```

---

## SECTION 2 — `services/retrieval_service.py`: Improve `_normalize_query`

**Problem:** Current normalization only collapses whitespace. Trailing `?` and `.` can cause minor tokenizer inconsistencies during embedding.

**Replace** the entire `_normalize_query` method with:

```python
def _normalize_query(self, query: str | None) -> str:
    normalized = str(query or "").strip()
    if not self.query_normalization_enabled:
        return normalized
    normalized = " ".join(normalized.split())
    # Strip trailing punctuation that adds no semantic value to the embedding
    if normalized.endswith("?") or normalized.endswith("."):
        normalized = normalized[:-1].strip()
    return normalized
```

---

## SECTION 3 — `services/retrieval_service.py`: Add reranker to `_retrieve_postgres_hybrid`

**Problem:** The Postgres hybrid path ranks results using only a weighted fusion score (`0.7 * vector + 0.3 * keyword`) with no semantic cross-encoder reranking. The Qdrant path already uses the reranker; the Postgres path should too.

**Find** the `_retrieve_postgres_hybrid` method. Locate the line where `fused_rows` is assigned from `_apply_result_limits`, which looks like:

```python
fused_rows = self._apply_result_limits(
    rows=fused_rows,
    ...
)
```

**Immediately after** that block (before the `return self._build_payload(...)` call), add:

```python
# Semantic reranking pass — mirrors the Qdrant backend behaviour
reranker_summary: dict[str, Any] = {}
try:
    fused_rows, reranker_summary = self._rerank_candidates(
        query=query, rows=fused_rows, parsed_k=parsed_k
    )
except RerankerServiceError:
    reranker_summary = {"reranker_applied": False, "reranker_fallback": True}
```

**Then update** the `candidate_summary` dict inside the `return self._build_payload(...)` call to include reranker info by merging `reranker_summary`:

```python
candidate_summary={
    "vector_candidate_count": len(vector_rows),
    "keyword_candidate_count": len(keyword_rows),
    "fused_candidate_count": len(fused_rows),
    "query_scope": self._classify_query_scope(query),
    "source_unit_diversity_limit": self._resolve_result_limits(
        query=query,
        requested_k=parsed_k,
        scoped_document_ids=scoped_document_ids,
    )["source_unit_limit"],
    **reranker_summary,
},
```

---

## SECTION 4 — `services/text_answer_service.py`: Use full `retrieval_text` in evidence chunks

**Problem:** `_build_payload` builds evidence lines using `result.get("snippet")`, which is truncated to `RETRIEVAL_SNIPPET_MAX_CHARS` (default 900 chars). The reranker scored on full `retrieval_text`. The LLM should see the same content the reranker ranked on.

**Find** `_build_payload` inside `TextAnswerService`. Find this line:

```python
chunk_text = str(result.get("snippet") or "").strip()
```

**Replace it with:**

```python
# Prefer full retrieval_text so the LLM sees what the reranker scored on.
# Fall back to snippet if retrieval_text is absent.
chunk_text = str(result.get("retrieval_text") or result.get("snippet") or "").strip()
# Guard against extremely long chunks that would overflow the context window.
if len(chunk_text) > 2000:
    chunk_text = chunk_text[:2000]
```

---

## SECTION 5 — `services/chat_answer_service.py`: Fix over-aggressive confidence downgrade

**Problem:** `_apply_grounding_guardrails` unconditionally downgrades `high` confidence to `partial` whenever there is only one citation. This is wrong for focused single-source factual questions.

**Find** `_apply_grounding_guardrails`. Find and **remove** these lines:

```python
if confidence == "high" and len(supported_citation_block_ids) == 1:
    confidence = "partial"
```

Do not replace them with anything. Leave the surrounding logic intact.

---

## SECTION 6 — `services/text_answer_service.py`: Fix prompt sentence-count instruction

**Problem:** The prompt template says "Typical target length: around N sentences" which competes with Rule 10 ("Prefer completeness"). The model may truncate a complete answer to hit the sentence target.

**Find** `DEFAULT_GROUNDED_ANSWER_PROMPT_TEMPLATE`. Find this line inside it:

```
Typical target length: around {prompt_profile_max_sentences} sentences when that fits the query, but answer more fully when the query is multi-part and the evidence supports it.
```

**Replace it with:**

```
Minimum response length: aim for at least {prompt_profile_max_sentences} sentences, but always answer fully when the query is multi-part or the evidence supports additional detail. Never truncate a complete answer to hit a length target.
```

---

## SECTION 7 — Citation support: backend changes

This section adds inline citations to answers so the frontend can render clickable links to the exact location in a source document.

### 7a — `services/retrieval_service.py`: Persist page/section metadata on result rows

**Find** `_candidate_row_from_tuple` (the method that builds a result dict from a database row). Confirm that it already includes `source_metadata` as a dict on the returned row. If it does, no change is needed here — the data is already flowing through. If `source_metadata` is missing or `None`, ensure it defaults to `{}`.

### 7b — `services/chat_answer_service.py`: Enrich `_build_citations` with anchor data

**Find** `_build_citations`. The current implementation already builds a list of citation dicts. **Replace the entire method** with the following expanded version that adds `anchor` (a deep-link descriptor the frontend can use to scroll to the exact location):

```python
@staticmethod
def _build_citations(
    *,
    retrieval_payload: dict[str, Any],
    answer_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    results = retrieval_payload.get("results") if isinstance(retrieval_payload, dict) else []
    results = results if isinstance(results, list) else []
    result_map = {
        str(result.get("block_id") or "").strip(): result
        for result in results
        if str(result.get("block_id") or "").strip()
    }

    citation_block_ids = ChatAnswerService._normalize_supported_citation_block_ids(
        retrieval_payload=retrieval_payload,
        answer_payload=answer_payload,
    )

    citations: list[dict[str, Any]] = []
    for index, block_id in enumerate(citation_block_ids, start=1):
        result = result_map.get(block_id)
        if not result:
            continue
        source_metadata = result.get("source_metadata") if isinstance(result.get("source_metadata"), dict) else {}

        # Page location
        page_value = (
            source_metadata.get("page")
            or source_metadata.get("page_number")
            or source_metadata.get("page_index")
        )
        page_label = f"p. {page_value}" if page_value not in (None, "") else ""

        # Section path (e.g. ["Chapter 3", "Section 3.2"])
        section_path: list[str] = [
            str(s).strip()
            for s in (source_metadata.get("section_path") or result.get("section_path") or [])
            if str(s).strip()
        ]

        # Anchor: structured descriptor for frontend deep-linking
        anchor: dict[str, Any] = {}
        if page_value not in (None, ""):
            anchor["page"] = page_value
        if section_path:
            anchor["section_path"] = section_path
        char_offset = source_metadata.get("char_offset") or source_metadata.get("start_char")
        if char_offset is not None:
            anchor["char_offset"] = char_offset

        citations.append(
            {
                "index": index,           # 1-based citation number shown to user, e.g. [1]
                "block_id": block_id,
                "document_id": str(result.get("document_id") or ""),
                "document_name": str(result.get("document_name") or result.get("document_id") or "Source"),
                "snippet": str(result.get("snippet") or ""),
                "page_label": page_label,
                "score": float(result.get("score") or 0.0),
                "anchor": anchor,         # use this in the frontend to build the deep-link URL
            }
        )
    return citations
```

### 7c — `services/text_answer_service.py`: Instruct the LLM to embed citation markers in `answer_text`

**Find** `DEFAULT_GROUNDED_ANSWER_PROMPT_TEMPLATE`. The evidence chunks are currently formatted as:

```
[block_id: {block_id} | source: {source_name}] {chunk_text}
```

**Update** the evidence line format in `_build_payload` inside `TextAnswerService`. Find:

```python
evidence_lines.append(
    f"[block_id: {result.get('block_id') or ''} | source: {source_name}] {chunk_text}"
)
```

**Replace with:**

```python
citation_index = len(evidence_lines) + 1
evidence_lines.append(
    f"[{citation_index}][block_id: {result.get('block_id') or ''} | source: {source_name}] {chunk_text}"
)
```

Then **add a new rule** at the end of the rules list inside `DEFAULT_GROUNDED_ANSWER_PROMPT_TEMPLATE`. Find the line:

```
10. Prefer completeness over brevity when evidence is sufficient.
```

After the existing rules, add:

```
12. When you use a fact from a chunk, embed its citation number inline in answer_text using the format [1], [2], etc., matching the number at the start of the evidence chunk. Place the marker immediately after the sentence that uses that chunk. Example: "The process involves three stages [1]. The final stage requires manual review [2]."
```

### 7d — `services/chat_answer_service.py`: Wire `answer_text` citation markers to citation objects

After the LLM returns `answer_text` containing inline markers like `[1]`, `[2]`, we need to confirm those numbers map correctly to the ordered citation list. **Find** `_build_persisted_retrieval_payload` and **replace** it with:

```python
def _build_persisted_retrieval_payload(
    self,
    *,
    retrieval_payload: dict[str, Any],
    answer_payload: dict[str, Any],
) -> dict[str, Any]:
    citations = self._build_citations(
        retrieval_payload=retrieval_payload,
        answer_payload=answer_payload,
    )
    enriched_payload = dict(retrieval_payload or {})
    enriched_payload["citations"] = citations

    # Build a lookup so the frontend can resolve [N] markers in answer_text
    enriched_payload["citation_index_map"] = {
        str(c["index"]): {
            "block_id": c["block_id"],
            "document_id": c["document_id"],
            "document_name": c["document_name"],
            "page_label": c["page_label"],
            "anchor": c["anchor"],
        }
        for c in citations
    }

    enriched_payload["grounded_answer"] = {
        "prompt_version": answer_payload.get("prompt_version") or PROMPT_VERSION,
        "prompt_profile": answer_payload.get("prompt_profile") or "default",
        "model_provider": answer_payload.get("model_provider") or "",
        "model_name": answer_payload.get("model_name") or "",
        "confidence": answer_payload.get("confidence") or "insufficient",
        "grounding_status": "grounded" if citations else "insufficient_evidence",
    }
    return enriched_payload
```

---

## SECTION 8 — Frontend: Render inline citations as clickable links

This section describes the UI changes needed to display `[1]`, `[2]` markers as clickable links that open the source document at the correct location. Apply this to whatever component currently renders the assistant's `answer_text`.

### 8a — Parse `answer_text` for citation markers

In the component that renders the answer, after receiving the API response, post-process `answer_text` to replace `[N]` with a styled clickable element. Example (adapt to your framework — Flutter or JS):

**If Flutter (Dart):**

```dart
// In your answer rendering widget, replace plain text rendering with this logic.
// Assumes you have access to `citationIndexMap` from the API response.

List<InlineSpan> buildAnswerSpans(String answerText, Map<String, dynamic> citationIndexMap) {
  final pattern = RegExp(r'\[(\d+)\]');
  final spans = <InlineSpan>[];
  int lastEnd = 0;

  for (final match in pattern.allMatches(answerText)) {
    // Text before this marker
    if (match.start > lastEnd) {
      spans.add(TextSpan(text: answerText.substring(lastEnd, match.start)));
    }

    final index = match.group(1)!;
    final citation = citationIndexMap[index];

    spans.add(
      WidgetSpan(
        child: GestureDetector(
          onTap: () => _openCitationAnchor(citation),
          child: Container(
            padding: const EdgeInsets.symmetric(horizontal: 4, vertical: 1),
            margin: const EdgeInsets.symmetric(horizontal: 1),
            decoration: BoxDecoration(
              color: Theme.of(context).colorScheme.primaryContainer,
              borderRadius: BorderRadius.circular(4),
            ),
            child: Text(
              '[${citation?["page_label"] ?? index}]',
              style: TextStyle(
                fontSize: 11,
                color: Theme.of(context).colorScheme.primary,
                fontWeight: FontWeight.w600,
              ),
            ),
          ),
        ),
      ),
    );
    lastEnd = match.end;
  }

  if (lastEnd < answerText.length) {
    spans.add(TextSpan(text: answerText.substring(lastEnd)));
  }

  return spans;
}

void _openCitationAnchor(Map<String, dynamic>? citation) {
  if (citation == null) return;
  final documentId = citation['document_id'] as String?;
  final anchor = citation['anchor'] as Map<String, dynamic>? ?? {};
  final page = anchor['page'];

  // Navigate to document viewer, passing page or char_offset
  // e.g. context.push('/document/$documentId?page=$page')
}
```

**If JavaScript/React:**

```jsx
function AnswerText({ answerText, citationIndexMap }) {
  const parts = answerText.split(/(\[\d+\])/g);
  return (
    <span>
      {parts.map((part, i) => {
        const match = part.match(/^\[(\d+)\]$/);
        if (!match) return <span key={i}>{part}</span>;
        const index = match[1];
        const citation = citationIndexMap?.[index];
        return (
          <a
            key={i}
            href="#"
            onClick={(e) => { e.preventDefault(); openCitationAnchor(citation); }}
            style={{
              display: 'inline-block',
              padding: '0 4px',
              margin: '0 1px',
              borderRadius: 4,
              background: '#e8f0fe',
              color: '#1a73e8',
              fontSize: 11,
              fontWeight: 600,
              textDecoration: 'none',
              verticalAlign: 'middle',
            }}
          >
            {citation?.page_label || part}
          </a>
        );
      })}
    </span>
  );
}

function openCitationAnchor(citation) {
  if (!citation) return;
  const { document_id, anchor } = citation;
  const page = anchor?.page;
  // Navigate to your document viewer route, e.g.:
  // router.push(`/documents/${document_id}?page=${page}`)
}
```

### 8b — Citation list below the answer

Below the `answer_text`, render a collapsible citation list using the `citations` array from the API response. Each item has `index`, `document_name`, `page_label`, `snippet`, and `anchor`. Render them as:

```
[1] filename.pdf, p. 4  — "...snippet text..."        [Open ↗]
[2] report.pdf, p. 12   — "...snippet text..."        [Open ↗]
```

The `[Open ↗]` button should call the same `openCitationAnchor` function above.

---

## SECTION 9 — Environment variable recommendations

Add or update the following in your `.env`:

```env
# Increase default retrieved chunks — the improved classifier needs headroom
RETRIEVAL_DEFAULT_K=8
RETRIEVAL_MAX_K=25

# Give the LLM more content per chunk
RETRIEVAL_SNIPPET_MAX_CHARS=1800

# Enable reranker if not already on
RERANKER_ENABLED=1
RERANKER_MODEL=BAAI/bge-reranker-base

# Keep temperature low for grounded factual answers
GEMINI_TEXT_TEMPERATURE=0.2
```

---

## Summary of all changes

| # | File | What changes |
|---|------|-------------|
| 1 | `retrieval_service.py` | `_classify_query_scope` — phrase-based, adds follow-up detection, raises word threshold to 15 |
| 2 | `retrieval_service.py` | `_normalize_query` — strips trailing `?`/`.` before embedding |
| 3 | `retrieval_service.py` | `_retrieve_postgres_hybrid` — adds reranker pass after fusion |
| 4 | `text_answer_service.py` | `_build_payload` — uses `retrieval_text` instead of `snippet` for evidence |
| 5 | `chat_answer_service.py` | `_apply_grounding_guardrails` — removes incorrect single-citation downgrade |
| 6 | `text_answer_service.py` | Prompt template — changes sentence target to minimum |
| 7a | `retrieval_service.py` | Confirm `source_metadata` flows through on result rows |
| 7b | `chat_answer_service.py` | `_build_citations` — adds `index`, `anchor`, `section_path` fields |
| 7c | `text_answer_service.py` | Evidence lines get `[N]` prefix; prompt gains Rule 12 for inline markers |
| 7d | `chat_answer_service.py` | `_build_persisted_retrieval_payload` — adds `citation_index_map` to response |
| 8a | Frontend | Parse `[N]` markers in `answer_text`, render as tappable citation chips |
| 8b | Frontend | Citation list below answer with `[Open ↗]` deep-link buttons |
| 9 | `.env` | Raise `RETRIEVAL_DEFAULT_K`, `RETRIEVAL_SNIPPET_MAX_CHARS`; confirm reranker on |
