import os
from pathlib import Path

document_id = "aea4b439-3867-40cf-b13b-be3f7a5a1485"
base = Path(r"C:\Users\Clouddy\Desktop\FYP2")

doc_folder = base / document_id
print("Document folder exists?", doc_folder.exists())

if doc_folder.exists():
    print("Contents:", list(doc_folder.iterdir()))
    
    extracted = doc_folder / ".extracted"
    print(".extracted exists?", extracted.exists())
    
    if extracted.exists():
        for f in extracted.rglob("*"):
            print(" ", f)
else:
    print("\nSearching entire FYP2 folder for .jpg files...")
    jpgs = list(base.rglob("*.jpg"))
    print(f"Found {len(jpgs)} jpg files:")
    for j in jpgs[:10]:
        print(" ", j)