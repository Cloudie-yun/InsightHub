from flask import Flask, render_template, jsonify, redirect, url_for, send_from_directory, abort
from db import get_db_connection
from pathlib import Path

app = Flask(__name__)
UPLOADS_DIR = Path(app.root_path) / "uploads"

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

@app.route('/uploads/<path:filename>')
def uploaded_file(filename):
    file_path = UPLOADS_DIR / filename
    if not file_path.exists() or not file_path.is_file():
        abort(404)
    return send_from_directory(UPLOADS_DIR, filename, as_attachment=False)

if __name__ == '__main__':
    app.run(debug=True, port=5000)
