# Agent Guide

## Purpose
This project is a Flask web app for document-assisted study workflows with:
- Auth (local + Google OAuth)
- Conversation dashboard/chat flow
- File upload and preview support

Use this file as the operating guide for coding agents working in this repository.

## Stack
- Backend: Python + Flask (`app.py`)
- Database: PostgreSQL via `psycopg2` (`db.py`)
- Templates: Jinja2 in `templates/`
- Static assets: `static/`
- Upload storage: `uploads/`

## Key Files
- `app.py`: Main Flask app, routes, auth, dashboard, upload logic
- `db.py`: Database connection factory using env vars
- `database_schema.sql`: DB schema source of truth
- `templates/dashboard.html`: Dashboard UI and client-side conversation actions
- `email_service.py`: Email sending helper

## Environment
Expected `.env` keys (based on code):
- `FLASK_SECRET_KEY`
- `DB_HOST`
- `DB_NAME`
- `DB_USER`
- `DB_PASSWORD`
- `DB_PORT`
- `APP_BASE_URL` (optional, used for external links)
- Google OAuth keys if using Google login flow

## Local Run
1. Create and activate a virtual environment.
2. Install dependencies used by imports in `app.py` and helpers.
3. Ensure PostgreSQL is running and schema is applied from `database_schema.sql`.
4. Start app:

```bash
python app.py
```

App runs with `debug=True` from `if __name__ == '__main__': app.run(debug=True)`.

## Agent Working Rules
- Keep edits minimal and scoped to the requested change.
- Preserve current coding style in touched files.
- Prefer server-validated behavior for auth/data mutations.
- Avoid broad refactors unless explicitly requested.
- If you touch frontend actions, verify matching backend endpoints exist.

## Current Gaps Worth Noting
- `templates/dashboard.html` uses client calls to:
  - `PUT /api/conversations/<id>`
  - `DELETE /api/conversations/<id>`
- If these endpoints are missing in `app.py`, implement them before relying on rename/delete persistence.

## Suggested Next Engineering Tasks
- Add/verify conversation rename and delete API routes.
- Add tests for auth flows and conversation CRUD.
- Add a pinned dependency file (`requirements.txt` or `pyproject.toml`) if not present.
- Improve error reporting/logging around DB operations.

## Definition Of Done For Agent Changes
- Feature works end-to-end (frontend + backend + DB).
- No unrelated file churn.
- Basic manual verification completed for touched flow.
- Brief changelog note included in PR/commit message.
