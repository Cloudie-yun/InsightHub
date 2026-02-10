from flask import Flask, render_template, jsonify
from db import get_db_connection

app = Flask(__name__)

@app.route('/')
def root():
    # Redirect root to login for the prototype flow
    return render_template('dashboard.html')

@app.route('/login')
def login():
    return render_template('login.html')

@app.route('/signup')
def signup():
    return render_template('signup.html')

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

if __name__ == '__main__':
    app.run(debug=True, port=5000)