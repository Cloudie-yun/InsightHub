from flask import Flask, render_template, jsonify, request, send_from_directory, abort
from db import get_db_connection
from pathlib import Path
import subprocess
import psycopg2
from werkzeug.security import generate_password_hash

app = Flask(__name__)
UPLOADS_DIR = Path(app.root_path) / "uploads"
PREVIEW_DIR = UPLOADS_DIR / ".preview"
PREVIEW_DIR.mkdir(parents=True, exist_ok=True)


def convert_docx_to_pdf(source_path: Path, output_path: Path) -> bool:
    # Try docx2pdf first (uses Word on Windows), then fallback to LibreOffice.
    try:
        from docx2pdf import convert as docx2pdf_convert
        docx2pdf_convert(str(source_path), str(output_path))
        return output_path.exists()
    except Exception:
        pass

    soffice_cmd = [
        "soffice",
        "--headless",
        "--convert-to",
        "pdf",
        "--outdir",
        str(output_path.parent),
        str(source_path),
    ]
    try:
        subprocess.run(soffice_cmd, check=True, capture_output=True)
    except Exception:
        return False

    libreoffice_output = output_path.parent / f"{source_path.stem}.pdf"
    if libreoffice_output.exists() and libreoffice_output != output_path:
        libreoffice_output.replace(output_path)
    return output_path.exists()


def get_preview_pdf_path(file_path: Path) -> Path:
    return PREVIEW_DIR / f"{file_path.stem}.pdf"

@app.route('/')
def root():
    return render_template('dashboard.html', active_page='dashboard')

@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html', active_page='dashboard')

@app.route('/chat')
def chat():
    return render_template('chat.html', active_page='chat')

@app.route('/flashcards')
def flashcards():
    return render_template('flashcards.html', active_page='study')

@app.route('/mindmap')
def mindmap():
    return render_template('mindmap.html', active_page='study')


@app.route('/api/auth/signup', methods=['POST'])
def signup():
    data = request.get_json(silent=True) or {}

    username = (data.get('username') or '').strip()
    email = (data.get('email') or '').strip().lower()
    password = data.get('password') or ''

    if not username:
        return jsonify({'error': 'Username is required.'}), 400
    if not email:
        return jsonify({'error': 'Email is required.'}), 400
    if not password:
        return jsonify({'error': 'Password is required.'}), 400

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            password_hash = generate_password_hash(password)
            cur.execute(
                """
                INSERT INTO users (username, email, auth_provider, password_hash)
                VALUES (%s, %s, 'local', %s)
                RETURNING user_id, username, email, created_at
                """,
                (username, email, password_hash),
            )
            created_user = cur.fetchone()
        conn.commit()
    except psycopg2.IntegrityError:
        if conn is not None:
            conn.rollback()
        return jsonify({'error': 'An account with this email already exists.'}), 409
    except Exception:
        if conn is not None:
            conn.rollback()
        return jsonify({'error': 'Unable to create account right now.'}), 500
    finally:
        if conn is not None:
            conn.close()

    return jsonify({
        'message': 'Signup successful.',
        'user': {
            'user_id': str(created_user[0]),
            'username': created_user[1],
            'email': created_user[2],
            'created_at': created_user[3].isoformat() if created_user[3] else None,
        },
    }), 201

@app.route('/uploads/<path:filename>')
def uploaded_file(filename):
    file_path = UPLOADS_DIR / filename
    if not file_path.exists() or not file_path.is_file():
        abort(404)
    return send_from_directory(UPLOADS_DIR, filename, as_attachment=False)


@app.route('/uploads/preview/<path:filename>')
def uploaded_file_preview(filename):
    file_path = UPLOADS_DIR / filename
    if not file_path.exists() or not file_path.is_file():
        abort(404)

    suffix = file_path.suffix.lower()
    if suffix == ".pdf":
        return send_from_directory(UPLOADS_DIR, filename, as_attachment=False)

    if suffix != ".docx":
        abort(415, description="Preview is only supported for PDF and DOCX files.")

    preview_pdf_path = get_preview_pdf_path(file_path)
    needs_convert = (
        not preview_pdf_path.exists()
        or preview_pdf_path.stat().st_mtime < file_path.stat().st_mtime
    )
    if needs_convert:
        converted = convert_docx_to_pdf(file_path, preview_pdf_path)
        if not converted:
            abort(500, description="Could not convert DOCX to PDF for preview.")

    return send_from_directory(PREVIEW_DIR, preview_pdf_path.name, as_attachment=False)

if __name__ == '__main__':
    app.run(debug=True, port=5000)
