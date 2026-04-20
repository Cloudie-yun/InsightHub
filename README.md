# 📌 InsightHub

*An AI-powered academic support platform that transforms uploaded documents into searchable knowledge, grounded answers, and interactive study aids.*

## 🚀 Overview

- **InsightHub** helps students and researchers work with complex documents more efficiently.
- The system accepts uploaded files, processes them into structured content, and makes that content reusable across multiple learning workflows.
- Instead of treating a document as raw text only, the platform preserves useful structure such as paragraphs, headings, tables, and diagrams.
- Extracted content is then used for **grounded chat**, **document review**, **summaries**, **flashcards**, and **mind maps**.
- The goal is to turn static study material into an interactive learning environment.

## 🎥 Video Demonstration

[Watch the demo video (Google Drive)](https://drive.google.com/file/d/1HKdovXlirdDNdEmWh8blVxuCIVjtgh9P/view?usp=sharing)

[Watch the demo video (Youtube Link)](https://youtu.be/WQK3i32cNao)

## ✨ Core Functions

| Function | Description |
|--------|------------|
| Document Upload | Users upload study materials such as PDFs, slides, text files, and images. |
| Document Parsing | The system extracts structured content from uploaded documents in the background. |
| Content Normalization | Parsed output is cleaned and reorganized into reusable blocks for downstream features. |
| Grounded Question Answering | Users can ask questions and receive answers based on uploaded document evidence. |
| Parser Result Review | Users can inspect extracted content and review how the system interpreted the document. |
| Diagram Analysis | Diagram regions can be analyzed to produce text descriptions and study-friendly context. |
| Summary Generation | The system produces concise summaries from processed document content. |
| Flashcard Generation | Users can generate editable flashcards directly from selected documents. |
| Mind Map Generation | Users can generate structured mind maps to visualize document topics and relationships. |
| Conversation-Based Study Flow | Documents can be grouped into conversations for context-aware study sessions. |

## 🧠 What the System Does

### 1. Document Understanding

- Accepts uploaded academic or study documents
- Processes them asynchronously to avoid blocking the user interface
- Extracts meaningful document structure instead of keeping only plain text
- Preserves content needed for later retrieval and analysis

### 2. Knowledge Grounding

- Converts extracted content into searchable document blocks
- Uses those blocks to support evidence-based answers
- Reduces unsupported responses by grounding outputs in uploaded material
- Connects source content to chat, summaries, and study aids

### 3. Learning Support

- Helps users review parsed material visually
- Generates revision tools from document content
- Supports different study styles through chat, flashcards, and mind maps
- Turns a static file into an interactive study workflow

## 🏗️ System Structure

```text
InsightHub/
├── app.py                    # Main application and route handling
├── db.py                     # Database connection layer
├── email_service.py          # Email delivery for account flows
├── services/                 # Parsing, retrieval, AI, and processing services
├── templates/                # Jinja UI templates
├── static/                   # Shared frontend assets
├── migrations/               # SQL schema evolution
├── tests/                    # Validation of parsing and answer behavior
├── docs/                     # Supporting implementation notes
└── uploads/                  # User-uploaded files and generated assets
```

## ⚙️ Main Modules

| Module | Responsibility |
|-------|----------------|
| `app.py` | Coordinates routes, auth flows, upload flows, API responses, and page rendering. |
| `services/document_parser.py` | Orchestrates document parsing and extracted content preparation. |
| `services/extraction_store.py` | Stores and loads parser output for later reuse. |
| `services/retrieval_service.py` | Retrieves relevant content blocks for grounded responses. |
| `services/chat_answer_service.py` | Produces answer responses using retrieved source context. |
| `services/summary_service.py` | Generates summaries from document and conversation content. |
| `services/diagram_vision_service.py` | Handles diagram analysis and diagram-related enrichment. |
| `templates/` | Defines the main user-facing pages such as dashboard, chat, parser results, flashcards, and mind map views. |
| `static/script/chat/` | Contains modular client-side behavior for the chat workspace. |

## 🖥️ User-Facing Pages

| Page | Purpose |
|------|---------|
| Dashboard | Entry point for conversations, uploads, and document access. |
| Chat | Main grounded Q&A workspace for studying uploaded materials. |
| Parser Results | Displays extracted content and parser review information. |
| Flashcards | Generates and edits study flashcards from document content. |
| Mind Map | Generates and edits visual topic maps from document content. |

## 🔄 High-Level Workflow

1. A user uploads one or more study documents.
2. The system stores the files and starts background parsing.
3. Parsed output is cleaned, normalized, and stored as structured content.
4. That content becomes available to retrieval and study workflows.
5. The user interacts with the system through chat, summaries, flashcards, mind maps, and parser review pages.

## 📡 Key Capabilities

| Capability | Outcome |
|-----------|---------|
| Background Processing | Large or complex documents can be handled without blocking the UI. |
| Structured Extraction | The system keeps useful layout-aware content for better downstream quality. |
| Evidence-Aware Retrieval | Answers can be tied back to uploaded document content. |
| Multi-Feature Reuse | A single parsed document supports multiple study tools. |
| Study Aid Persistence | Generated study aids can be revisited and updated later. |
| Diagram-Aware Enrichment | Visual content can be included in the overall study pipeline. |

## 🎯 Project Purpose

- Improve how students interact with long or complex academic materials
- Reduce friction between reading, understanding, and revising
- Combine document parsing and AI assistance into one workflow
- Support more active, reusable, and visual forms of studying

## 🤝 Contributing

- Keep changes focused and aligned with the current architecture
- Prefer small updates over broad refactors
- Preserve existing frontend hooks and backend flow assumptions
- Add tests when extending parser or answer behavior

## 📄 License

No license file is currently included in this repository.
