# Repository Guidelines

## Project Structure & Ownership
`app.py` is the primary Flask entrypoint and currently owns most route handlers, auth flows, upload flows, and API responses. Keep backend edits scoped and avoid broad refactors unless the task explicitly requires one. Database helpers live in `db.py`, email helpers in `email_service.py`, and parsing logic in `services/`, with format-specific parsers under `services/parsers/`.

Frontend rendering is Jinja-first. Shared layout and Tailwind config live in `templates/base.html`. Page templates include `templates/dashboard.html`, `templates/chat.html`, `templates/document_parser_results.html`, `templates/mindmap.html`, and `templates/flashcards.html`. Shared auth UI lives in `templates/partials/auth_modal.html`.

Static assets live under `static/`. Global CSS is in `static/styles/base.css`. Shared page behavior lives in `static/script/base.js` and `static/script/chat.js`. Chat-specific behavior is split across `static/script/chat/shared.js`, `panel.js`, `sources.js`, `upload.js`, and `prompt-rail.js`. User-uploaded files are stored in `uploads/`. SQL changes belong in `migrations/`, while `database_schema.sql` remains the baseline schema snapshot.

## Development Commands
Use a local virtual environment before running anything.

```bash
python app.py
```

Starts the Flask development server with `debug=True`.

```bash
psql "$DATABASE_URL" -f migrations/001_document_extraction_persistence.sql
psql "$DATABASE_URL" -f migrations/002_document_extraction_assets_and_references.sql
```

Applies migrations in lexical order. If `DATABASE_URL` is unavailable, use explicit Postgres connection flags.

```bash
python -m py_compile app.py db.py email_service.py
```

Runs a lightweight syntax check for touched Python files.

## Editing Conventions
Follow existing file style instead of imposing a new one.

- Python: 4-space indentation, `snake_case`, `UPPER_SNAKE_CASE` for constants.
- JavaScript: `camelCase` locals and small DOM-oriented helpers.
- Templates: prefer Tailwind utility classes inline; only add local `<style>` blocks when utility-only markup would become noisy or stateful behavior needs a small custom rule.
- CSS: keep shared theme behavior in `static/styles/base.css`; do not duplicate global dark-mode rules in page templates unless a page-specific surface truly needs it.

Prefer small, localized changes. This codebase is still centralized in a few files, especially `app.py`, so unrelated cleanup often creates risk without improving delivery.

## Frontend Guidance
Treat `templates/base.html` as the source of truth for app-wide layout, theme tokens, and Tailwind color definitions. The `brand` palette is already defined there and should be reused instead of introducing ad hoc blue/purple values.

When editing `dashboard.html` or `chat.html`:

- Preserve existing IDs and `data-*` hooks unless you are also updating the matching JavaScript.
- Keep drag/drop, upload, rename, delete, and modal behavior wired to the current script expectations.
- Prefer Tailwind utility classes over custom CSS for spacing, borders, typography, and colors.
- If a custom class exists only to style a single element state, consider replacing it with utility classes and JS class toggles.
- Check dark-mode compatibility against the existing `body.theme-dark` overrides in `static/styles/base.css` before adding page-specific dark styles.

## Testing Expectations
There is no dedicated `tests/` package yet. For most tasks:

- manually verify the affected route or UI flow,
- test the related JavaScript behavior in the browser when markup or client logic changes,
- run `python -m py_compile` on touched Python files.

If you add automated tests, place them in a new `tests/` package and use `test_*.py` naming.

## Commits & Pull Requests
Recent history uses short, direct commit messages such as `Hide Migration Files` and `PDF Parsing Still Continuing`. Keep commit messages imperative, concise, and focused on one change.

Pull requests should include:

- a short summary of the change,
- affected routes/templates/scripts,
- migration notes if schema changes are involved,
- screenshots or short recordings for visible UI changes.

## Security & Configuration
Secrets belong in `.env`, never in source control. Important variables include `FLASK_SECRET_KEY`, `DB_*`, `MAIL_*`, `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`, `GOOGLE_REDIRECT_URI`, `APP_BASE_URL`, and `MINERU_API_KEY`.

Do not commit real credentials, uploaded user content, or generated parser artifacts that should remain local.
