# 📌 InsightHub

*AI-powered document study workspace for parsing files, grounded chat, summaries, flashcards, and mind maps.*

## 🚀 Overview

- Upload academic or study documents and process them in the background.
- Extract structured document content with MinerU-based parsing and post-processing.
- Ask grounded questions over uploaded material with retrieval-backed chat.
- Generate document summaries, flashcards, and mind maps from extracted content.
- Review parser output, diagram analysis, and source-backed answers in one Flask app.

## ✨ Features

| Feature | Description |
|--------|------------|
| Document Uploads | Supports `.pdf`, `.doc`, `.docx`, `.ppt`, `.pptx`, `.txt`, `.png`, `.jpg`, `.jpeg`, `.webp`. |
| Background Parsing | Stores uploads immediately, then runs document parsing asynchronously. |
| Grounded Chat | Uses retrieval over extracted document blocks to answer with source context. |
| Parser Review | Exposes parser results and review flows for extracted blocks and diagrams. |
| Diagram Analysis | Runs Gemini-based vision analysis on detected diagram blocks. |
| Summaries | Generates document and conversation summaries from extracted content. |
| Study Aids | Builds editable **flashcards** and **mind maps** from selected source documents. |
| Authentication | Email/password auth, email verification, password reset, and Google OAuth. |
| Prompt Profiles | Stores per-user system prompt and prompt profile preferences. |
| Retrieval Backends | Supports PostgreSQL hybrid retrieval and optional Qdrant-backed dense retrieval. |

## 🏗️ Architecture / Structure

```text
project-root/
├── app.py                         # Main Flask entrypoint and route handlers
├── db.py                          # PostgreSQL connection helpers
├── email_service.py               # SMTP email sending
├── requirements.txt
├── database_schema.sql            # Baseline schema snapshot
├── migrations/                    # Incremental SQL migrations
├── services/
│   ├── document_parser.py         # Parsing pipeline orchestration
│   ├── extraction_store.py        # Persisted parser payloads
│   ├── retrieval_service.py       # Grounded retrieval orchestration
│   ├── chat_answer_service.py     # Answer generation
│   ├── summary_service.py         # Summary generation
│   ├── diagram_vision_service.py  # Diagram analysis
│   ├── embedding_service.py       # Embedding provider integration
│   └── parsers/
│       ├── pdf_parser.py
│       ├── pptx_parser.py
│       ├── text_parser.py
│       ├── image_parser.py
│       └── mineru/                # MinerU ZIP/API parsing helpers
├── templates/
│   ├── base.html
│   ├── dashboard.html
│   ├── chat.html
│   ├── document_parser_results.html
│   ├── flashcards.html
│   ├── mindmap.html
│   └── partials/
├── static/
│   ├── styles/base.css
│   └── script/
│       ├── base.js
│       ├── chat.js
│       └── chat/                  # Chat page modules
├── tests/                         # unittest-based regression coverage
├── docs/                          # Project notes and pipeline docs
└── uploads/                       # Local uploaded files and generated assets
```

## ⚙️ Installation

### 1. Clone and enter the project

```bash
git clone <your-repo-url>
cd FYP2
```

### 2. Create a virtual environment

```bash
python -m venv .venv
```

### 3. Activate the virtual environment

```bash
# Windows PowerShell
.venv\Scripts\Activate.ps1

# macOS / Linux
source .venv/bin/activate
```

### 4. Install dependencies

```bash
pip install -r requirements.txt
```

### 5. Configure environment variables

Create a `.env` file in the project root and set the required values.

### 6. Apply database migrations

```bash
psql "$DATABASE_URL" -f migrations/001_document_extraction_persistence.sql
psql "$DATABASE_URL" -f migrations/002_document_extraction_assets_and_references.sql
psql "$DATABASE_URL" -f migrations/003_diagram_vision_tables.sql
psql "$DATABASE_URL" -f migrations/003_document_blocks.sql
psql "$DATABASE_URL" -f migrations/004_diagram_vision_scoring.sql
psql "$DATABASE_URL" -f migrations/004_document_block_embeddings.sql
psql "$DATABASE_URL" -f migrations/005_embedding_runs.sql
psql "$DATABASE_URL" -f migrations/006_document_block_soft_delete.sql
psql "$DATABASE_URL" -f migrations/007_fix_diagram_crop_storage_paths.sql
psql "$DATABASE_URL" -f migrations/008_quota_state.sql
psql "$DATABASE_URL" -f migrations/009_quota_limits.sql
psql "$DATABASE_URL" -f migrations/010_conversation_messages.sql
psql "$DATABASE_URL" -f migrations/011_quota_limits_billing_tier.sql
psql "$DATABASE_URL" -f migrations/012_user_profile_settings.sql
psql "$DATABASE_URL" -f migrations/013_user_prompt_profiles.sql
psql "$DATABASE_URL" -f migrations/014_conversation_message_versioning.sql
psql "$DATABASE_URL" -f migrations/015_summary_pipeline.sql
psql "$DATABASE_URL" -f migrations/016_study_aids.sql
```

### 7. Start the development server

```bash
python app.py
```

## ▶️ Usage

### Web App

1. Open `http://127.0.0.1:5000`
2. Create an account or sign in
3. Upload one or more study documents
4. Wait for parsing to complete
5. Use:
   - `/chat` for grounded Q&A
   - `/documents/<document_id>/parser-results` for extraction review
   - `/flashcards` for deck generation
   - `/mindmap` for map generation

### Example Upload Request

```bash
curl -X POST http://127.0.0.1:5000/api/documents/upload \
  -F "files=@sample.pdf"
```

### Example Retrieval Request

```bash
curl -X POST http://127.0.0.1:5000/api/conversations/<conversation_id>/retrieve \
  -H "Content-Type: application/json" \
  -d "{\"query\":\"Summarize the main findings\"}"
```

## 🔧 Configuration

| Variable | Description | Default |
|---------|-------------|---------|
| `FLASK_SECRET_KEY` | Flask session secret | `dev-secret-change-me` |
| `DATABASE_URL` | Full PostgreSQL connection string | `""` |
| `DB_HOST` | PostgreSQL host when `DATABASE_URL` is not used | `localhost` |
| `DB_NAME` | PostgreSQL database name | `InsightHubDB` |
| `DB_USER` | PostgreSQL user | `postgres` |
| `DB_PASSWORD` | PostgreSQL password | `""` |
| `DB_PORT` | PostgreSQL port | `5432` |
| `DB_SSLMODE` | PostgreSQL SSL mode | `""` |
| `DB_TIMEZONE` | Session timezone for DB connections | `""` |
| `APP_BASE_URL` | Public app base URL for links/callbacks | `""` |
| `GOOGLE_CLIENT_ID` | Google OAuth client ID | `""` |
| `GOOGLE_CLIENT_SECRET` | Google OAuth client secret | `""` |
| `GOOGLE_REDIRECT_URI` | OAuth callback override | `""` |
| `MAIL_USERNAME` | SMTP username | `""` |
| `MAIL_APP_PASSWORD` | SMTP app password | `""` |
| `MAIL_FROM_NAME` | Sender display name | `InsightHub` |
| `MINERU_API_KEY` | MinerU parsing API key | `""` |
| `GEMINI_API_KEY` | Primary Gemini API key | `""` |
| `GEMINI_API_KEYS` | Comma-separated Gemini API key pool | `""` |
| `GEMINI_TEXT_MODEL` | Text generation model | `gemini-2.5-flash` |
| `GEMINI_VISION_MODEL` | Diagram vision model | `gemini-3-flash` |
| `EMBEDDING_PROVIDER` | Embedding provider (`gemini` or `openai`) | `gemini` |
| `EMBEDDING_MODEL` | Embedding model override | provider-dependent |
| `OPENAI_API_KEY` | OpenAI key for embeddings | `""` |
| `QDRANT_URL` | Qdrant base URL for dense retrieval | `""` |
| `QDRANT_API_KEY` | Qdrant API key | `""` |
| `QDRANT_COLLECTION` | Qdrant collection name | `document_blocks` |
| `RERANKER_ENABLED` | Enables reranking stage | `1` |
| `DOCUMENT_PARSE_MAX_WORKERS` | Background parse worker count | `2` |
| `EMBEDDING_AUTORUN_ENABLED` | Auto-run embedding jobs | `1` |
| `SUMMARY_AUTORUN_ENABLED` | Auto-run summary jobs | `1` |

## 📡 API

| Endpoint | Method | Description |
|---------|--------|-------------|
| `/api/auth/signup` | `POST` | Create a new user account |
| `/api/auth/login` | `POST` | Authenticate a user |
| `/api/auth/logout` | `POST` | End the current session |
| `/api/auth/verify-email` | `GET` | Verify signup email token |
| `/api/auth/forgot-password/request` | `POST` | Request password reset |
| `/api/auth/forgot-password/reset` | `POST` | Reset password |
| `/api/auth/google/start` | `GET` | Start Google OAuth login |
| `/api/documents/upload` | `POST` | Upload documents for parsing |
| `/api/conversations/<conversation_id>/documents/upload` | `POST` | Upload into an existing conversation |
| `/api/conversations/<conversation_id>/documents` | `GET` | List conversation documents |
| `/api/conversations/<conversation_id>/documents/<document_id>` | `PATCH` | Rename/update a document record |
| `/api/conversations/<conversation_id>/documents/<document_id>` | `DELETE` | Soft-delete a document from a conversation |
| `/api/documents/<document_id>/parser-results` | `GET` | Fetch parser output |
| `/api/documents/<document_id>/parser-review` | `POST` | Save parser review edits |
| `/api/documents/<document_id>/diagram-analysis` | `POST` | Run diagram analysis |
| `/api/documents/<document_id>/summary` | `GET` | Fetch document summary |
| `/api/conversations/<conversation_id>/summary` | `GET` | Fetch conversation summary |
| `/api/conversations/<conversation_id>/retrieve` | `POST` | Retrieve relevant source blocks |
| `/api/conversations/<conversation_id>/messages` | `POST` | Send a chat message |
| `/api/conversations/<conversation_id>/message-versions/select` | `POST` | Switch active message version |
| `/api/flashcards/generate` | `POST` | Generate flashcards from a document |
| `/api/mindmap/generate` | `POST` | Generate a mind map from a document |
| `/api/study-aids` | `POST` | Save a study aid |
| `/api/study-aids/<study_aid_id>` | `GET` | Load a saved study aid |
| `/api/study-aids/<study_aid_id>` | `PUT` | Update a saved study aid |

## 🧪 Testing

### Run syntax checks

```bash
python -m py_compile app.py db.py email_service.py
```

### Run unit tests

```bash
python -m unittest discover -s tests -p "test_*.py" -v
```

### Current coverage focus

- Parser post-processing
- MinerU text and table pipeline behavior
- Chat answer guardrails

## 📦 Deployment

### Production entrypoint

```bash
gunicorn app:app
```

### Recommended deployment steps

1. Provision **PostgreSQL**
2. Set all required `.env` variables
3. Apply SQL migrations in order
4. Serve the Flask app with **Gunicorn**
5. Put a reverse proxy in front of the app
6. Persist the `uploads/` directory if uploaded artifacts must survive restarts

## 🤝 Contributing

- Keep changes **small and localized**, especially in `app.py`
- Follow the existing style in Python, Jinja, JavaScript, and CSS
- Place schema changes in `migrations/`
- Run `py_compile` and relevant tests before opening a PR
- Include screenshots for visible UI changes

## 📄 License

No license file is currently included in this repository.
