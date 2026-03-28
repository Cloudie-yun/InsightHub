# Repository Guidelines

## Project Structure & Module Organization
`app.py` is the main Flask entry point and currently holds most routes, auth flows, upload handling, and API logic. Database access lives in `db.py`, email helpers in `email_service.py`, and document parsing code in `services/` with format-specific parsers under `services/parsers/`. Jinja templates live in `templates/`, shared assets in `static/`, uploaded files in `uploads/`, and schema changes in `migrations/`. Treat `database_schema.sql` as the baseline schema and `migrations/*.sql` as incremental changes.

## Build, Test, and Development Commands
Use a local virtual environment before running the app.

```bash
python app.py
```

Starts the Flask development server with `debug=True`.

```bash
psql "$DATABASE_URL" -f migrations/001_document_extraction_persistence.sql
psql "$DATABASE_URL" -f migrations/002_document_extraction_assets_and_references.sql
```

Applies SQL migrations in lexical order. If `DATABASE_URL` is unavailable, use explicit Postgres connection flags.

```bash
python -m py_compile app.py db.py email_service.py
```

Performs a lightweight syntax check for touched Python files.

## Coding Style & Naming Conventions
Follow the existing style in touched files: 4-space indentation, `snake_case` for Python functions and variables, `UPPER_SNAKE_CASE` for constants, and `camelCase` for JavaScript locals in `static/script/`. Keep templates and asset names descriptive and consistent with current usage, for example `document_parser_results.html` and `prompt-rail.js`. Prefer small, scoped edits over large refactors because much of the app is centralized in `app.py`.

## Testing Guidelines
There is no dedicated `tests/` directory yet. For now, verify changes with targeted manual testing of the affected route, template, and JavaScript flow, then run `python -m py_compile ...` on edited Python files. When adding tests, use `test_*.py` naming and place them in a new `tests/` package so the suite is easy to discover later.

## Commit & Pull Request Guidelines
Recent commits use short, direct summaries such as `Hide Migration Files` and `PDF Parsing Still Continuing`. Keep commit messages concise, imperative, and focused on one change. Pull requests should include a clear description, impacted routes or files, migration notes if schema changes are involved, and screenshots for UI updates in `templates/` or `static/`.

## Security & Configuration Tips
Store secrets in `.env`, not in source. Important variables include `FLASK_SECRET_KEY`, `DB_*`, `MAIL_*`, `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`, `GOOGLE_REDIRECT_URI`, `APP_BASE_URL`, and `MINERU_API_KEY`. Do not commit uploaded user files or real credentials.
