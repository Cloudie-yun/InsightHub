from flask import Flask, render_template, request
from PyPDF2 import PdfReader
import os

app = Flask(__name__)

UPLOAD_FOLDER = "uploads"
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/upload", methods=["POST"])
def upload_file():
    file = request.files["document"]
    file_path = os.path.join(app.config["UPLOAD_FOLDER"], file.filename)
    file.save(file_path)

    extracted_text = ""

    if file.filename.endswith(".txt"):
        with open(file_path, "r", encoding="utf-8") as f:
            extracted_text = f.read()
    elif file.filename.endswith(".pdf"):
        reader = PdfReader(file_path)
        for page in reader.pages:
            extracted_text += page.extract_text() + "\n"
    return f"<pre>{extracted_text}</pre>"


if __name__ == "__main__":
    app.run(debug=True)